use flate2::read::GzDecoder;
use jiff::tz::{self, TimeZone};
use jiff::{civil::*, Zoned};
use log::{error, info};
use quick_xml::de::from_str;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::process::Command;

use crate::db::isone::lib_isoexpress::download_file;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct IesoGenOutputByFuelArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IesoGenOutputByFuelArchive {
    /// Return the xml filename for the day
    pub fn filename(&self, year: i16) -> String {
        self.base_dir.to_owned()
            + "/Raw"
            + "/PUB_GenOutputbyFuelHourly_"
            + &year.to_string()
            + ".xml"
    }

    /// Data is published every day at 06:51, don't know timezone.
    pub fn download_file(&self, year: i16) -> Result<(), Box<dyn Error>> {
        download_file(
            format!(
                "https://reports-public.ieso.ca/public/GenOutputbyFuelHourly/{}",
                Path::new(&self.filename(year))
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("")
            ),
            false,
            None,
            Path::new(&self.filename(year)),
            true,
        )
    }

    pub fn read_file(&self, year: i16) -> Result<Vec<Row>, Box<dyn Error>> {
        let path_gz = self.filename(year) + ".gz";
        let mut file = GzDecoder::new(File::open(path_gz).unwrap());
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let doc: Document = from_str(&buffer)?;
        let mut rows: Vec<Row> = Vec::new();
        for dd in doc.doc_body.daily_data {
            let delivery_date: Date = dd.day.parse()?;
            for hd in dd.hourly_data {
                for ft in hd.fuel_total {
                    rows.push(Row {
                        begin_hour: delivery_date
                            .at(hd.hour - 1, 0, 0, 0)
                            .to_zoned(TimeZone::fixed(tz::offset(-5)))?,
                        output_quality: ft.energy_value.output_quality,
                        fuel_type: ft.fuel,
                        mw: ft.energy_value.output,
                    });
                }
            }
        }

        Ok(rows)
    }

    /// Don't use a time offset anymore because America/Cancun timezone doesn't work properly before 2016.
    pub fn make_gzfile_for_year(&self, year: i16) -> Result<(), Box<dyn Error>> {
        let file_out = format!(
            "{}/year/PUB_GenOutputbyFuelHourly_{}.csv",
            self.base_dir.to_owned(),
            year
        );
        let mut wtr = csv::Writer::from_path(&file_out)?;
        wtr.write_record(["hour_beginning", "fuel_type", "output_quality", "mw"])?;

        let rows = self.read_file(year)?;
        for row in rows {
            let _ = wtr.write_record(&[
                row.begin_hour
                    .strftime("%Y-%m-%dT%H:%M:%S.000")
                    .to_string(),
                row.fuel_type.to_string(),
                row.output_quality.to_string(),
                match row.mw {
                    Some(mw) => mw.to_string(),
                    None => "".into(),
                },
            ]);
        }
        wtr.flush()?;

        Command::new("gzip")
            .args(["-f", &file_out])
            .current_dir(format!("{}/year", self.base_dir))
            .spawn()
            .unwrap()
            .wait()
            .expect("gzip failed");
        Ok(())
    }

    /// Upload each one year to DuckDB.
    /// Assumes the corresponding json.gz file exists.  
    /// 
    pub fn update_duckdb(&self, year: i16) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting hourly generation totals by fuel for IESO's area for year {} ...",
            year
        );
        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS gen_by_fuel (
    hour_beginning TIMESTAMP NOT NULL,    
    fuel_type ENUM('NUCLEAR', 'GAS', 'HYDRO', 'WIND', 'SOLAR', 'BIOFUEL', 'OTHER') NOT NULL,
    output_quality INT1 NOT NULL,
    mw UINT16,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT 
        hour_beginning, fuel_type, output_quality, mw
    FROM read_csv('{}/year/PUB_GenOutputbyFuelHourly_{}.csv.gz', 
    columns = {{
        'hour_beginning': "TIMESTAMP NOT NULL",
        'fuel_type': "VARCHAR NOT NULL",
        'output_quality': "INT1 NOT NULL",
        'mw': "UINT16"
        }}
    )
;

INSERT INTO gen_by_fuel BY NAME
(SELECT * FROM tmp 
WHERE NOT EXISTS (
    SELECT * FROM gen_by_fuel d
    WHERE d.hour_beginning = tmp.hour_beginning
    AND d.fuel_type = tmp.fuel_type
    )
)
ORDER BY hour_beginning, fuel_type;
"#,
            self.base_dir,
            year,
        );
        // println!("{}", sql);

        let output = Command::new("duckdb")
            .arg("-c")
            .arg(&sql)
            .arg(&self.duckdb_path)
            .output()
            .expect("Failed to invoke duckdb command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() {
            info!("{}", stdout);
            info!("done");
        } else {
            error!("Failed to update duckdb for month {}: {}", year, stderr);
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
struct Document {
    #[serde(rename = "DocHeader")]
    doc_header: DocHeader,
    #[serde(rename = "DocBody")]
    doc_body: DocBody,
}

#[derive(Debug, Deserialize, Serialize)]
struct DocHeader {
    #[serde(rename = "DocTitle")]
    doc_title: String,
    #[serde(rename = "DocRevision")]
    doc_revision: String,
    #[serde(rename = "DocConfidentiality")]
    doc_confidentiality: DocConfidentiality,
    #[serde(rename = "CreatedAt")]
    created_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct DocConfidentiality {
    #[serde(rename = "DocConfClass")]
    doc_conf_class: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct DocBody {
    #[serde(rename = "DeliveryYear")]
    delivery_year: String,
    #[serde(rename = "DailyData")]
    daily_data: Vec<DailyData>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DailyData {
    #[serde(rename = "Day")]
    day: String,
    #[serde(rename = "HourlyData")]
    hourly_data: Vec<HourlyData>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HourlyData {
    #[serde(rename = "Hour")]
    hour: i8,
    #[serde(rename = "FuelTotal")]
    fuel_total: Vec<FuelTotal>,
}

#[derive(Debug, Deserialize, Serialize)]
struct FuelTotal {
    #[serde(rename = "Fuel")]
    fuel: String,
    #[serde(rename = "EnergyValue")]
    energy_value: EnergyValue,
}

#[derive(Debug, Deserialize, Serialize)]
struct EnergyValue {
    #[serde(rename = "OutputQuality")]
    output_quality: i8,
    #[serde(rename = "Output")]
    output: Option<usize>,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Row {
    pub begin_hour: Zoned,
    pub fuel_type: String,
    pub output_quality: i8,
    pub mw: Option<usize>,
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path};

    use crate::{
        db::prod_db::ProdDb,
        interval::term::Term,
    };

    use super::*;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::ieso_generation_output_by_fuel();
        let term = "Cal15-Cal25".parse::<Term>().unwrap();
        for year in term.years() {
            // archive.download_file(year)?;
            archive.make_gzfile_for_year(year)?;
            archive.update_duckdb(year)?;
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn read_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::ieso_generation_output_by_fuel();
        let rows = archive.read_file(2025)?;
        assert_eq!(
            rows[0],
            Row {
                begin_hour: "2025-01-01T00:00:00-05:00[-05:00]".parse()?,
                fuel_type: "NUCLEAR".into(),
                output_quality: 0,
                mw: Some(10404),
            }
        );
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        // dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::ieso_generation_output_by_fuel();
        let term = "Cal15-Cal24".parse::<Term>().unwrap();
        for year in term.years() {
            archive.download_file(year)?;
        }
        Ok(())
    }
}
