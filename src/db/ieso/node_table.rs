use duckdb::Connection;
use jiff::civil::Date;
use log::{error, info};
use std::error::Error;
use std::fs;
use std::path::Path;
use std::str::FromStr;
use std::{collections::HashSet, fs::File};

use crate::db::prod_db::ProdDb;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct IesoNodeTableArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IesoNodeTableArchive {
    /// Return the csv filename for the day
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned() + "/Raw" + "/node_table_" + &date.to_string() + ".csv"
    }

    pub fn get_data(&self, date: &Date) -> Result<Vec<Row>, Box<dyn Error>> {
        let mut rows = Vec::new();
        rows.push(Row {
            r#type: LocationType::Area,
            name: "ONTARIO".to_string(),
        });

        let zone_archive = ProdDb::ieso_dalmp_zonal();
        let data = zone_archive.read_file(date)?;
        let zone_names = data
            .iter()
            .map(|row| row.location_name.clone())
            .collect::<HashSet<String>>();
        for name in zone_names {
            rows.push(Row {
                r#type: LocationType::Zone,
                name: name.to_string(),
            });
        }

        let node_archive = ProdDb::ieso_dalmp_nodes();
        let data = node_archive.read_file(date)?;
        let node_names = data
            .iter()
            .map(|row| row.location_name.clone())
            .collect::<HashSet<String>>();
        for name in node_names {
            rows.push(Row {
                r#type: LocationType::Node,
                name: name.to_string(),
            });
        }

        Ok(rows)
    }

    pub fn write_csv(&self, date: &Date) -> Result<(), Box<dyn Error>> {
        let path = self.filename(date);
        let dir = Path::new(&path).parent().unwrap();
        let _ = fs::create_dir_all(dir);

        let data = self.get_data(date)?;
        let file = File::create(path.clone())?;
        let mut writer = csv::WriterBuilder::new()
            .quote_style(csv::QuoteStyle::NonNumeric)
            .from_writer(file);
        // let mut writer = csv::Writer::from_writer(file);
        for row in data {
            writer.serialize(row)?;
        }
        writer.flush()?;
        info!("Wrote file {}", path);
        Ok(())
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.
    /// This method only works well for a few day.  For a lot of days, don't loop over days.
    /// Consider using DuckDB directly by globbing the file names.
    ///  
    pub fn update_duckdb(&self, day: &Date) -> Result<(), Box<dyn Error>> {
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
CREATE TABLE IF NOT EXISTS Locations (
    type ENUM('AREA', 'ZONE', 'NODE') NOT NULL,
    name VARCHAR NOT NULL
);",
        )?;

        let path = self.filename(day);
        if !Path::new(&path).exists() {
            panic!("No file for {}.  Create the .csv file first!", day);
        }

        // insert into duckdb
        conn.execute_batch(&format!(
            r#"
CREATE TEMPORARY TABLE tmp AS
    SELECT * FROM read_csv('{}', 
        header = true,
        columns = {{
            'type': "ENUM('AREA', 'ZONE', 'NODE') NOT NULL",
            'name': 'VARCHAR NOT NULL'
        }})
    ;"#,
            path
        ))?;

        let query = r"
INSERT INTO locations
    SELECT 
        type,
        name,
    FROM tmp
EXCEPT 
    SELECT * FROM locations
ORDER BY type, name;
";
        match conn.execute(query, []) {
            Ok(updated) => info!("{} rows were updated for day {}", updated, day),
            Err(e) => error!("{}", e),
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "UPPERCASE")]
pub enum LocationType {
    Area,
    Node,
    Zone,
}

impl FromStr for LocationType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "AREA" => Ok(LocationType::Area),
            "NODE" => Ok(LocationType::Node),
            "ZONE" => Ok(LocationType::Zone),
            _ => Err(format!("Invalid location type: {}", s)),
        }
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub r#type: LocationType,
    pub name: String,
}


#[cfg(test)]
mod tests {

    use jiff::civil::date;
    use std::{error::Error, path::Path, str::FromStr};

    use crate::db::prod_db::ProdDb;

    use super::LocationType;

    #[test]
    fn location_type_test() -> Result<(), Box<dyn Error>> {
        let area = LocationType::from_str("AREA")?;
        assert_eq!(area, LocationType::Area);
        Ok(())
    }



    #[ignore]
    #[test]
    fn make_csv_test() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::ieso_node_table();
        archive.write_csv(&date(2025, 5, 3))?;
        archive.update_duckdb(&date(2025, 5, 3))?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn read_file_test() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::ieso_node_table();
        let data = archive.get_data(&date(2025, 5, 6))?;
        assert_eq!(data.len(), 1008);
        Ok(())
    }
}
