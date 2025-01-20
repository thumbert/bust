use duckdb::Connection;
use flate2::read::GzDecoder;
use jiff::civil::*;
use jiff::Timestamp;
use jiff::Zoned;
use log::error;
use log::info;
use serde::Serialize;
use serde_json::Value;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io;
use std::io::Read;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;

/// See https://www.hydroquebec.com/documents-data/open-data/hydrometeorological-data/
pub struct HqHydroDataArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Debug, Serialize, Clone, Copy)]
pub enum Frequency {
    Hourly,
    Daily,
}

impl FromStr for Frequency {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Horaire" => Ok(Frequency::Hourly),
            _ => Err(format!("Failed to parse {s} as Frequency")),
        }
    }
}

#[derive(Debug, Serialize, Clone, Copy)]
pub enum MeasurementType {
    Instantaneous,
    Max,
    Mean,
    Min,
    Sum,
}

impl FromStr for MeasurementType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Instantanée" => Ok(MeasurementType::Instantaneous),
            "Maximum" => Ok(MeasurementType::Max),
            "Moyenne" => Ok(MeasurementType::Mean),
            "Minimum" => Ok(MeasurementType::Min),
            "Somme" => Ok(MeasurementType::Sum),
            _ => Err(format!("Failed to parse {s} as MeasurementType")),
        }
    }
}

#[derive(Debug, Serialize, PartialEq, Clone, Copy)]
pub enum Variable {
    FlowRate,
    Humidity,
    MinTemperature,
    MaxTemperature,
    Precipitation,
    SnowLevel,
    WaterEquivalentSnowLevel,
    WaterLevel,
    WaterTemperature,
    WindDirection,
    WindSpeed,
}

impl FromStr for Variable {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Débit" => Ok(Variable::FlowRate),
            "Direction du vent.10 mètres" => Ok(Variable::WindDirection),
            "Équivalent en eau de la neige" => Ok(Variable::WaterEquivalentSnowLevel),
            "Épaisseur de neige" => Ok(Variable::SnowLevel),
            "Humidité relative.2 mètres" => Ok(Variable::Humidity),
            "Niveau" => Ok(Variable::WaterLevel),
            "Précipitation" => Ok(Variable::Precipitation),
            "Température de l'eau" => Ok(Variable::WaterTemperature),
            "Température Maximum" => Ok(Variable::MaxTemperature),
            "Température Minimum" => Ok(Variable::MinTemperature),
            "Vitesse du vent.10 mètres" => Ok(Variable::WindSpeed),
            _ => Err(format!("Failed to parse {s} as Variable")),
        }
    }
}

#[derive(Debug, Serialize)]
pub struct StationInfo {
    pub code_region_qc: i32,
    pub region_name: String,
    pub start_date: Date,
    pub end_date: Option<Date>,
    pub station_id: String,
    pub station_name: String,
    pub coord_x: f32,
    pub coord_y: f32,
    pub coord_z: Option<f32>,
}

#[derive(Debug, Serialize)]
pub struct StationData {
    pub station_id: String,
    pub unit_of_measure: String,
    pub frequency: Frequency,
    pub measure_type: MeasurementType,
    pub variable: Variable,
    pub hour_beginning: Timestamp,
    pub value: f32,
}

impl HqHydroDataArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/"
            + &date.to_string()
            + "_hydro_data.json"
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.   
    pub fn update_duckdb(&self, days: Vec<Date>) -> Result<(), Box<dyn Error>> {
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
            CREATE TABLE IF NOT EXISTS WaterLevel (
                station_id VARCHAR NOT NULL,
                hour_beginning TIMESTAMP NOT NULL,
                value DOUBLE NOT NULL, 
            );",
        )?;

        for day in days {
            // extract the water level data
            let path = self.filename(&day) + ".gz";
            if !Path::new(&path).exists() {
                info!("No file for {}.  Skipping", day);
                continue;
            }
            let xs = self.process_hourly_observations(&path, Variable::WaterLevel)?;
            let path = self.base_dir.clone() + &format!("/tmp/water_level_data_{}.csv", day);
            let mut wtr = csv::Writer::from_path(path)?;
            for x in xs {
                wtr.serialize(x)?;
            }
            wtr.flush()?;

            // insert into duckdb
            let query = format!(
                r"
            INSERT INTO WaterLevel
                SELECT station_id, hour_beginning, value 
                FROM read_csv('{}/tmp/water_level_data_{}.csv', 
                    header = true) 
                EXCEPT SELECT * FROM WaterLevel;    
            ",
                self.base_dir, day
            );
            match conn.execute(&query, []) {
                Ok(updated) => info!("{} rows were updated", updated),
                Err(e) => error!("{}", e),
            }
        }
        let _ = conn.close();

        Ok(())
    }

    // Get metadata about all the stations from a file
    pub fn read_station_metadata(&self, path: &String) -> Result<Vec<StationInfo>, Box<dyn Error>> {
        let mut file = File::open(path).unwrap();
        // accept both a json or a json.gz file
        let ext = Path::new(path).extension().unwrap();
        let v: Value = match ext.to_str().unwrap() {
            "json" => {
                let mut buffer = String::new();
                file.read_to_string(&mut buffer).unwrap();
                serde_json::from_str(&buffer)?
            }
            "gz" => {
                let rdr = GzDecoder::new(file);
                serde_json::from_reader(rdr)?
            }
            _ => return Err(format!("Invalid file format {:?}", ext).into()),
        };

        let station = match &v["Station"] {
            Value::Array(v) => v,
            _ => panic!("Wrong file format"),
        };

        let mut station_info: Vec<StationInfo> = Vec::new();
        for e in station.iter() {
            let start = Date::strptime("%Y/%m/%d", e["date debut"].as_str().unwrap());
            let end = match Date::strptime("%Y/%m/%d", e["date fin"].to_string()) {
                Ok(d) => Some(d),
                Err(_) => None,
            };
            // println!("{:?}", e);
            let one = StationInfo {
                code_region_qc: e["CodeRegionQC"].as_str().unwrap().parse::<i32>().unwrap(),
                region_name: e["RegionQC"].as_str().unwrap().to_string(),
                start_date: start.unwrap(),
                end_date: end,
                station_id: e["identifiant"].as_str().unwrap().to_string(),
                station_name: e["nom"].as_str().unwrap().to_string(),
                coord_x: e["xcoord"].as_str().unwrap().parse::<f32>().unwrap(),
                coord_y: e["ycoord"].as_str().unwrap().parse::<f32>().unwrap(),
                coord_z: None,
            };
            // println!("{:?}", one);
            station_info.push(one);
        }
        station_info.sort_by(|a, b| a.station_name.cmp(&b.station_name));

        Ok(station_info)
    }

    /// Process a file for observations for a given variable.  
    pub fn process_hourly_observations(
        &self,
        path: &String,
        variable: Variable,
    ) -> Result<Vec<StationData>, Box<dyn Error>> {
        let mut file = File::open(path).unwrap();
        // accept both a json or a json.gz file
        let ext = Path::new(path).extension().unwrap();
        let v: Value = match ext.to_str().unwrap() {
            "json" => {
                let mut buffer = String::new();
                file.read_to_string(&mut buffer).unwrap();
                serde_json::from_str(&buffer)?
            }
            "gz" => {
                let rdr = GzDecoder::new(file);
                serde_json::from_reader(rdr)?
            }
            _ => return Err(format!("Invalid file format {:?}", ext).into()),
        };

        let station = match &v["Station"] {
            Value::Array(v) => v,
            _ => return Err("Wrong Station field format".to_string().into()),
        };

        let mut hourly_data: Vec<StationData> = Vec::new();
        for e in station.iter() {
            let data = match &e["Composition"] {
                Value::Array(v) => v,
                _ => return Err("Wrong Composition field format".to_string().into()),
            };
            let station_id = e["identifiant"].as_str().unwrap().to_string();

            for f in data.iter() {
                // println!("{:?}", f);
                let obs = match &f["Donnees"] {
                    Value::Object(v) => v,
                    _ => return Err(format!("Wrong Donnees field format: {}", e["Donnees"]).into()),
                };
                let var = f["type_point_donnee"]
                    .as_str()
                    .unwrap()
                    .parse::<Variable>()
                    .unwrap();
                if var != variable {
                    continue;
                }
                let uom = f["nom_unite_mesure"].as_str().unwrap().to_string();
                let frequency = f["pas_temps"]
                    .as_str()
                    .unwrap()
                    .parse::<Frequency>()
                    .unwrap();
                let measure_type = f["type_mesure"]
                    .as_str()
                    .unwrap()
                    .parse::<MeasurementType>()
                    .unwrap();

                for (k, v) in obs.into_iter() {
                    let hour_beginning: Timestamp = k.replace("/", "-").parse().unwrap();
                    let value = match v {
                        Value::String(e) => e.parse::<f32>(),
                        _ => return Err(format!("Expected a string, got {v}").into()),
                    };
                    let one = StationData {
                        station_id: station_id.clone(),
                        unit_of_measure: uom.clone(),
                        frequency,
                        measure_type,
                        variable,
                        hour_beginning,
                        value: value.unwrap(),
                    };
                    hourly_data.push(one);
                }
            }
        }

        Ok(hourly_data)
    }

    /// Data is updated on the website twice every day
    pub fn download_file(&self) -> Result<(), Box<dyn Error>> {
        let url = "https://www.hydroquebec.com/data/documents-donnees/donnees-ouvertes/json/Donnees_VUE_STATIONS_ET_TARAGES.json";
        let resp = reqwest::blocking::get(url).expect("request failed");
        let body = resp.text().expect("body invalid");
        let today: Date = Zoned::now().date();
        let path = &self.filename(&today);
        let dir = Path::new(path).parent().unwrap();
        let _ = fs::create_dir_all(dir);
        let mut out = File::create(path).expect("failed to create file");
        io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");

        // gzip it
        Command::new("gzip")
            .args(["-f", path])
            .current_dir(dir)
            .spawn()
            .unwrap()
            .wait()
            .expect("gzip failed");

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use jiff::{civil::date, ToSpan};
    use std::error::Error;

    use crate::db::prod_db::ProdDb;

    use super::*;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        let archive = ProdDb::hq_hydro_data();
        // let days = vec![date(2024, 12, 4), date(2024, 12, 5), date(2024, 12, 6)];
        let days = date(2024, 12, 8).series(1.day()).take(5).collect();
        
        archive.update_duckdb(days)
    }

    #[test]
    fn process_hourly_level_data() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::hq_hydro_data();
        let day = date(2024, 12, 5);
        let path = archive.filename(&day) + ".gz";
        let xs = archive.process_hourly_observations(&path, Variable::WaterLevel)?;
        assert_eq!(xs.len(), 72324);
        Ok(())
    }

    #[test]
    fn read_metadata() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::hq_hydro_data();
        let path = archive.filename(&date(2024, 12, 4)) + ".gz";
        let stations = archive.read_station_metadata(&path)?;
        assert_eq!(stations.len(), 417);

        // let mut wtr = csv::Writer::from_writer(io::stdout());
        // for station in stations {
        //     wtr.serialize(station)?;
        // }
        // wtr.flush()?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::hq_hydro_data();
        archive.download_file()?;
        Ok(())
    }
}
