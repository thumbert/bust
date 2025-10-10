use jiff::civil::Date;
use jiff::Zoned;
use log::{error, info};
use std::error::Error;
use std::path::Path;
use std::process::Command;

use crate::interval::month::Month;


#[derive(Clone)]
pub struct IsoneFuelMixArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IsoneFuelMixArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/genfuelmix_"
            + &date.strftime("%Y%m%d").to_string()
            + ".json"
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, day: &Date) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting fuel mix file for day {} ...",
            day
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS fuel_mix (
    timestamp TIMESTAMPTZ,
    mw INT32,
    fuel_category_rollup ENUM('Coal', 'Hydro', 'Natural Gas', 'Nuclear', 'Oil', 'Other', 'Renewables'),
    fuel_category ENUM( 'Coal', 'Hydro', 'Landfill Gas', 'Natural Gas', 'Nuclear', 'Oil', 'Other', 'Refuse', 'Solar', 'Wind', 'Wood'),
    marginal_flag BOOL
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT 
        BeginDate::TIMESTAMPTZ as timestamp, 
        GenMw:: INT32 as mw, 
        FuelCategoryRollup:: VARCHAR AS fuel_category_rollup, 
        FuelCategory::VARCHAR AS fuel_category,
        CASE MarginalFlag 
            WHEN 'Y' THEN TRUE
            WHEN 'N' THEN FALSE
            ELSE NULL
        END AS marginal_flag
        FROM (
            SELECT unnest(GenFuelMixes.GenFuelMix, recursive := true)
            FROM read_json('{}/Raw/{}/genfuelmix_{}.json.gz')
    )
    ORDER BY timestamp, fuel_category
;

INSERT INTO fuel_mix
(SELECT * FROM tmp 
WHERE NOT EXISTS (
    SELECT * FROM fuel_mix d
    WHERE d.timestamp = tmp.timestamp
    AND d.fuel_category = tmp.fuel_category
    AND d.mw = tmp.mw
    )
)
ORDER BY timestamp, fuel_category;
            "#,
            self.base_dir,
            day.year(),
            day.strftime("%Y%m%d"),
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
            error!("Failed to update duckdb for day {}: {}", day, stderr);
        }

        Ok(())
    }


    /// Data is usually published before 13:30 every day
    pub fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        super::lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/genfuelmix/day/{}",
                yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(&date)),
            true,
        )
    }

    /// Look for missing days.  Does not download current day. 
    pub fn download_missing_days(&self, month: Month) -> Result<(), Box<dyn Error>> {
        let last = Zoned::now().date();
        for day in month.days() {
            if day >= last {
                // incomplete day, don't download
                continue;
            }
            let fname = format!("{}.gz", self.filename(&day));
            if !Path::new(&fname).exists() {
                info!("Working on {}", day);
                self.download_file(day)?;
                info!("  downloaded file for {}", day);
            }
        }
        Ok(())
    }

}

#[cfg(test)]
mod tests {

    use jiff::civil::date;
    use log::info;
    use std::{error::Error, path::Path};

    use crate::{db::prod_db::ProdDb, interval::month::month};

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_fuel_mix();

        let months = month(2025, 1).up_to(month(2025, 9)).unwrap();
        for month in months {
            // std::thread::sleep(std::time::Duration::from_secs(30));
            archive.download_missing_days(month)?;
            for day in month.days() {
                info!("  updating day {}", day);
                archive.update_duckdb(&day)?;
            }
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_fuel_mix();
        archive.download_file(date(2023, 12, 31))?;
        Ok(())
    }
}




