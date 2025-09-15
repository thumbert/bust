// Hourly data for total electricity demand in Quebec.
// https://donnees.hydroquebec.com/explore/dataset/historique-demande-electricite-quebec/information/

use jiff::civil::*;
use jiff::Zoned;
use log::error;
use log::info;
use rust_decimal::Decimal;
use serde::Serialize;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;
use std::process::Command;

use crate::interval::month::Month;

/// This is finalized data.  
pub struct HqFinalizedTotalDemandArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Debug, Serialize)]
pub struct Row {
    pub zoned: Zoned,
    pub value: Decimal,
}

impl HqFinalizedTotalDemandArchive {
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
CREATE TABLE IF NOT EXISTS total_demand_final (
    hour_beginning TIMESTAMPTZ NOT NULL,
    value DECIMAL(9,2) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT 
       date::TIMESTAMPTZ - INTERVAL 1 HOUR AS hour_beginning,
       moyenne_mw::DECIMAL(9,2) AS value
    FROM (
        SELECT unnest(results, recursive := true)
        FROM read_json('~/Downloads/Archive/HQ/TotalDemandFinal/Raw/{}/total_demand_{}-*.json.gz')
    )
    WHERE value IS NOT NULL
    ORDER BY hour_beginning
;

INSERT INTO total_demand_final
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM total_demand_final d
        WHERE
            d.hour_beginning = t.hour_beginning AND
            d.value = t.value
    )
) ORDER BY hour_beginning; 
            "#,
            month.start_date().year(),
            month.strftime("%Y-%m")
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
            error!("Failed to update duckdb for month {}: {}", month, stderr);
        }

        Ok(())
    }

    /// New data is published annually! On 15-Sep-2025 only data up to end of 2023 is available.
    pub fn download_file(&self, day: &Date) -> Result<(), Box<dyn Error>> {
        let url = format!("https://donnees.hydroquebec.com/api/explore/v2.1/catalog/datasets/historique-demande-electricite-quebec/records?where=date%20%3E%3D%20date%27{}%27%20and%20date%20%3C%20date%27{}%27&limit=40", day, day.tomorrow().unwrap());
        let resp = reqwest::blocking::get(url).expect("request failed");
        let body = resp.text().expect("body invalid");
        let path = &self.filename(day);
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

    use std::error::Error;

    use crate::{db::prod_db::ProdDb, interval::term::Term};

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        let archive = ProdDb::hq_total_demand_final();
        let term = "Feb19".parse::<Term>().unwrap();
        for month in term.months() {
            archive.update_duckdb(month)?;
        }

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::hq_total_demand_final();
        let term = "Jan24".parse::<Term>().unwrap();
        for day in term.days() {
            archive.download_file(&day)?;
        }   
        Ok(())
    }
}
