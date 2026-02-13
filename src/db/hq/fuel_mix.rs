// 15-minute data for total electricity demand in Quebec.
// https://donnees.hydroquebec.com/explore/dataset/demande-electricite-quebec/information/

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

pub struct HqFuelMixArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Debug, Serialize)]
pub struct Row {
    pub zoned: Zoned,
    pub value: Decimal,
}

impl HqFuelMixArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/fuel_mix_"
            + &date.to_string()
            + ".json"
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.   
    pub fn update_duckdb(&self, month: Month) -> Result<(), Box<dyn Error>> {
        info!("inserting HQ fuel mix files for month {} ...", month);

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS fuel_mix (
    zoned TIMESTAMPTZ NOT NULL,
    total DECIMAL(9,2) NOT NULL,
    hydro DECIMAL(9,2) NOT NULL,
    wind DECIMAL(9,2),
    solar DECIMAL(9,2),
    other DECIMAL(9,2),
    thermal DECIMAL(9,2)
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT
        time::TIMESTAMPTZ AS zoned,
        total::DECIMAL(9,2) AS total,
        hydraulique::DECIMAL(9,2) AS hydro,
        eolien::DECIMAL(9,2) AS wind,
        solaire::DECIMAL(9,2) AS solar,
        autres::DECIMAL(9,2) AS other,
        thermique::DECIMAL(9,2) AS thermal,
    FROM (
        SELECT *
        FROM read_json('{}/Raw/{}/fuel_mix_{}-*.json.gz')
    )
    WHERE total != 0
    ORDER BY zoned
;

INSERT INTO fuel_mix
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM fuel_mix d
        WHERE
            d.zoned = t.zoned AND
            d.total = t.total AND
            d.hydro = t.hydro
    )
) ORDER BY zoned; 
            "#,
            self.base_dir,
            month.start_date().year(),
            month.strftime("%Y-%m")
        );

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
    pub fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        // this url has only the most recent 48 records, so not good for backfilling.
        // let _ = "https://donnees.hydroquebec.com/api/explore/v2.1/catalog/datasets/production-electricite-quebec/records?limit=100&order_by=date";
        // I switched to this url which has data from 2024-09-01 but uses a private key.  Not sure how stable
        // the key is, etc.
        let url = format!("https://electricite-quebec.info/gen_data?start_date={}&end_date={}&limit=100000&order_by=date&key={}", 
            date,
            date.tomorrow().unwrap(),
            std::env::var("HQ_API_KEY").expect("HQ_API_KEY not set")
        );
        info!(
            "Downloading HQ fuel mix data for {} from url: {}",
            date, url
        );
        let resp = reqwest::blocking::get(url).expect("request failed");
        let body = resp.text().expect("body invalid");
        let path = &self.filename(&date);
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

    use std::{error::Error, path::Path};


    use crate::{db::prod_db::ProdDb, interval::term::Term};

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        let archive = ProdDb::hq_fuel_mix();
        let term = "Jan25-Jan26".parse::<Term>().unwrap();
        for m in term.months() {
            archive.update_duckdb(m)?;
        }

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::hq_fuel_mix();
        let term = "1Feb26-12Feb26".parse::<Term>().unwrap();
        for day in term.days() {
            // wait for 3 seconds between downloads to avoid overwhelming the server
            std::thread::sleep(std::time::Duration::from_secs(3));
            archive.download_file(day)?;
        }
        Ok(())
    }
}
