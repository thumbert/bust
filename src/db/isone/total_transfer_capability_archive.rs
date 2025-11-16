use itertools::Itertools;
use jiff::civil::*;
use log::error;
use log::info;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::Command;

use crate::interval::month::Month;

// 
pub struct TotalTransferCapabilityArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl TotalTransferCapabilityArchive {

    /// All columns as of 2025-06-01.  This will change in the future once NECEC is operational.
    pub fn all_columns() -> Vec<String> {
        vec![
            "ny_north_import".to_string(),
            "ny_north_export".to_string(),
            "ny_northport_import".to_string(),
            "ny_northport_export".to_string(),
            "ny_csc_import".to_string(),
            "ny_csc_export".to_string(),
            "nb_import".to_string(),
            "nb_export".to_string(),
            "hq_highgate_import".to_string(),
            "hq_highgate_export".to_string(),
            "hq_phase2_import".to_string(),
            "hq_phase2_export".to_string(),
        ]
    }

    /// Path to the CSV file with the ISO report for a given day.
    /// ISO doesn't publish this data as part of their webservices API. 
    /// https://webservices.iso-ne.com/api/v1.1/totaltransfercapability/day/20250101
    pub fn filename(&self, date: Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/ttc_"
            + &date.strftime("%Y%m%d").to_string()
            + ".csv"
    }


    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!("inserting ISONE TTC daily files for month {} ...", month);

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS ttc_limits (
    hour_beginning TIMESTAMPTZ NOT NULL,
    ny_north_import int64 NOT NULL,
    ny_north_export int64 NOT NULL,
    ny_northport_import int64 NOT NULL,
    ny_northport_export int64 NOT NULL,
    ny_csc_import int64 NOT NULL,
    ny_csc_export int64 NOT NULL,
    nb_import int64 NOT NULL,
    nb_export int64 NOT NULL,
    hq_highgate_import int64 NOT NULL,
    hq_highgate_export int64 NOT NULL,
    hq_phase2_import int64 NOT NULL,
    hq_phase2_export int64 NOT NULL,
);


CREATE TEMPORARY TABLE tmp1 
AS 
SELECT column01 AS Day,
    column03 AS ny_north_import,
    column04 AS	ny_north_export,
    column05 AS	ny_northport_import,
    column06 AS ny_northport_export,
    column07 AS	ny_csc_import,
    column08 AS	ny_csc_export,
    column09 AS	nb_import,
    column10 AS	nb_export,
    column11 AS	hq_highgate_import,
    column12 AS	hq_highgate_export,
    column13 AS	hq_phase2_import,
    column14 AS	hq_phase2_export,
FROM read_csv('/home/adrian/Downloads/Archive/IsoExpress/Ttc/Raw/{}/ttc_{}*.csv.gz', 
    header = false, 
    skip = 6,
    ignore_errors = true,
    strict_mode = false,
    dateformat = '%m/%d/%Y');


CREATE TEMPORARY TABLE tmp AS
(SELECT day + INTERVAL (idx) HOUR AS hour_beginning, 
    * EXCLUDE (day, idx)
FROM (
    SELECT         
        row_number() OVER (PARTITION BY day) - 1 AS idx, -- 0 to 23 for each day
        *
    FROM tmp1
    )
ORDER BY hour_beginning    
);


INSERT INTO ttc_limits
(SELECT * FROM tmp t
WHERE NOT EXISTS (
    SELECT * FROM ttc_limits b
    WHERE
        b.hour_beginning = t.hour_beginning 
    )    
)
ORDER BY hour_beginning;
"#,
            month.start_date().year(),
            month.start_date().strftime("%Y%m")
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

    pub fn download_days(&self, days: Vec<Date>) -> Result<(), Box<dyn Error>> {
        if days.first().unwrap().year() != days.last().unwrap().year() {
            return Err("All days must be in the same year".into());
        }
        let dir = format!("{}/Raw/{}", self.base_dir, days.first().unwrap().year());
        let _ = fs::create_dir_all(&dir);

        let mut out = Command::new("python3")
            .args(["/home/adrian/Documents/repos/git/thumbert/elec-server/bin/python/isone_ttc_download.py", 
             &format!("--days={}", days.iter().map(|e| e.strftime("%Y%m%d")).join(","))])
            .current_dir(&dir)
            .stdout(std::process::Stdio::inherit())
            .spawn()
            .expect("downloads failed");
        let _ = out.wait();
        Ok(())
    }

    /// Check if the files for some days are missing, and download them.
    pub fn download_missing_days(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let days = month.days();
        let mut missing_days: Vec<Date> = Vec::new();
        for day in days {
            let file = self.filename(day);
            if !Path::new(&file).exists() {
                missing_days.push(day);
            }
        }
        if !missing_days.is_empty() {
            self.download_days(missing_days)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use jiff::civil::date;
    use std::error::Error;

    use crate::{
        db::prod_db::ProdDb,
        interval::{interval_base::DateExt, month::month},
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

        let archive = ProdDb::isone_ttc();
        // let days = vec![date(2024, 12, 4), date(2024, 12, 5), date(2024, 12, 6)];
        // let days = date(2024, 1, 1).up_to(date(2024, 12, 31));
        // for day in &days {
        //     archive.download_days(vec![day])?;
        // }
        let months = month(2025,1).up_to(month(2025, 5))?;
        for month in &months {
            println!("Updating DuckDB for month {}", month);
            archive.update_duckdb(month)?;
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_days() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::isone_ttc();
        archive.download_days(date(2025, 1, 1).up_to(date(2025, 5, 31)))?;
        Ok(())
    }
}
