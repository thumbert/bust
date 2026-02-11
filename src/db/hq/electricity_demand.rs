// 15-minute data for total electricity demand in Quebec from https://electricite-quebec.info/en#.
// This site allows downloading historical data!

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

/// This is preliminary data.  
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
    pub fn filename(&self, month: &Month) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &month.start_date().year().to_string()
            + "/demand_"
            + &month.strftime("%Y-%m").to_string()
            + ".json"
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.   
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting HQ total system demand files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS total_demand (
    start_15min TIMESTAMPTZ NOT NULL,
    value DECIMAL(9,2) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT 
       time::TIMESTAMPTZ AS start_15min,
       demand::DECIMAL(9,2) AS value
    FROM (
        SELECT time, demand
        FROM read_json('{}/Raw/{}/demand_{}.json.gz')
    )
    WHERE value IS NOT NULL
    ORDER BY start_15min
;

INSERT INTO total_demand
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM total_demand d
        WHERE
            d.start_15min = t.start_15min AND
            d.value = t.value
    )
) ORDER BY start_15min; 
            "#,
            self.base_dir,
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

    /// Data is updated on the website every 15 min
    pub fn download_file(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let url = format!(
            "https://electricite-quebec.info/data?start_date={}&end_date={}",
            month.start_date(),
            month.end_date()
        );
        println!("downloading from url: {}", url);
        let resp = reqwest::blocking::get(url).expect("request failed");
        let body = resp.text().expect("body invalid");
        let path = &self.filename(month);
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

    use crate::{db::prod_db::ProdDb, interval::month::month};

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        let archive = ProdDb::hq_total_demand();
        let months = month(2024, 5).up_to(month(2026, 2))?;
        for m in months {
            // archive.download_file(&m)?;
            archive.update_duckdb(&m)?;
        }

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::hq_total_demand();
        let months = month(2024, 4).up_to(month(2026, 2))?;
        for m in months {
            archive.download_file(&m)?;
        }
        Ok(())
    }
}
