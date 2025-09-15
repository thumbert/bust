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
        date::TIMESTAMPTZ AS zoned,
        valeurs_total::DECIMAL(9,2) AS total,
        valeurs_hydraulique::DECIMAL(9,2) AS hydro,
        valeurs_eolien::DECIMAL(9,2) AS wind,
        valeurs_solaire::DECIMAL(9,2) AS solar,
        valeurs_autres::DECIMAL(9,2) AS other,
        valeurs_thermique::DECIMAL(9,2) AS thermal,
    FROM (
        SELECT unnest(results, recursive := true)
        FROM read_json('~/Downloads/Archive/HQ/FuelMix/Raw/{}/fuel_mix_{}-*.json.gz')
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
    pub fn download_file(&self) -> Result<(), Box<dyn Error>> {
        let url = "https://donnees.hydroquebec.com/api/explore/v2.1/catalog/datasets/production-electricite-quebec/records?limit=100";
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

    use std::error::Error;

    use crate::db::prod_db::ProdDb;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        // let archive = ProdDb::hq_fuel_mix();
        // // let days = vec![date(2024, 12, 4), date(2024, 12, 5), date(2024, 12, 6)];
        // let days: Vec<Date> = date(2024, 12, 8).series(1.day()).take(5).collect();

        // archive.update_duckdb(days)
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::hq_fuel_mix();
        archive.download_file()?;
        Ok(())
    }
}
