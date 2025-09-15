// 15-minute data for total electricity demand in Quebec.
// https://donnees.hydroquebec.com/explore/dataset/demande-electricite-quebec/information/


use duckdb::Connection;
use flate2::read::GzDecoder;
use jiff::civil::*;
use jiff::Timestamp;
use jiff::Zoned;
use log::error;
use log::info;
use rust_decimal::Decimal;
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

use crate::interval::month::Month;

pub struct HqTotalDemandArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}



#[derive(Debug, Serialize)]
pub struct Row {
    pub zoned: Zoned,
    pub value: Decimal,
}

impl HqTotalDemandArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/total_demand_"
            + &date.to_string()
            + ".json"
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.   
    pub fn update_duckdb(&self, month: Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting HQ total system demand files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS total_demand (
    zoned TIMESTAMPTZ NOT NULL,
    value DECIMAL(9,2) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT 
       date::TIMESTAMPTZ AS zoned,
       valeurs_demandetotal::DECIMAL(9,2) AS value
    FROM (
        SELECT unnest(results, recursive := true)
        FROM read_json('~/Downloads/Archive/HQ/TotalDemand/Raw/2025/total_demand_{}-*.json.gz')
    )
    WHERE value IS NOT NULL
    ORDER BY zoned
;

INSERT INTO total_demand
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM total_demand d
        WHERE
            d.zoned = t.zoned AND
            d.value = t.value
    )
) ORDER BY zoned; 
            "#, month.strftime("%Y-%m"));

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
            error!("Failed to update duckdb for month {}: {}", month, stderr);
        }

        Ok(())
    }

    /// Data is updated on the website every 15 min
    pub fn download_file(&self) -> Result<(), Box<dyn Error>> {
        let url = "https://donnees.hydroquebec.com/api/explore/v2.1/catalog/datasets/demande-electricite-quebec/records?limit=100";
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
        let archive = ProdDb::hq_total_demand();
        // let days = vec![date(2024, 12, 4), date(2024, 12, 5), date(2024, 12, 6)];
        let days: Vec<Date> = date(2024, 12, 8).series(1.day()).take(5).collect();
        
        // archive.update_duckdb(days)
        Ok(())
    }

    // #[test]
    // fn process_hourly_level_data() -> Result<(), Box<dyn Error>> {
    //     let archive = ProdDb::hq_hydro_data();
    //     let day = date(2024, 12, 5);
    //     let path = archive.filename(&day) + ".gz";
    //     let xs = archive.process_hourly_observations(&path, Variable::WaterLevel)?;
    //     assert_eq!(xs.len(), 72324);
    //     Ok(())
    // }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::hq_total_demand();
        archive.download_file()?;
        Ok(())
    }
}
