use futures::StreamExt;
use jiff::civil::Date;
use jiff::Zoned;
use log::{error, info};
use reqwest::get;
use rust_decimal::Decimal;
use std::error::Error;
use std::path::Path;
use std::process::Command;
use tokio::fs::File;
use tokio_util::io::StreamReader;

use crate::db::nyiso::dalmp::LmpComponent;
use crate::interval::month::Month;

#[derive(Debug, PartialEq)]
pub struct Row {
    hour_beginning: Zoned,
    ptid: u32,
    lmp: Decimal,
    mcc: Decimal,
    mlc: Decimal,
}

// https://oasis.caiso.com/oasisapi/GroupZip?resultformat=6&version=3&groupid=PUB_DAM_GRP&startdatetime=20250911T07:00-0000

#[derive(Clone)]
pub struct CaisoDaLmpArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl CaisoDaLmpArchive {
    /// Return the csv filename for one component for the day.  Does not check if the file exists.  
    /// For example: 
    ///  - 20251206_20251206_PRC_LMP_DAM_LMP_v12.csv
    ///  - 20251206_20251206_PRC_LMP_DAM_MCC_v12.csv
    ///  - 20251206_20251206_PRC_LMP_DAM_MCL_v12.csv
    pub fn filename(&self, date: &Date, component: LmpComponent) -> String {
        let yyyymmdd = date.strftime("%Y%m%d");
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + format!(
                "/{}_{}_PRC_LMP_DAM_{}_v12.csv",
                yyyymmdd,
                yyyymmdd,
                component.to_string().to_uppercase()
            )
            .as_str()
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting daily DALMP hourly price files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS da_lmp (
    hour_beginning TIMESTAMPTZ NOT NULL,
    ptid UINTEGER NOT NULL,
    lmp DECIMAL(9,4) NOT NULL,
    mcc DECIMAL(9,4) NOT NULL,
    mcl DECIMAL(9,4) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT 
        BeginDate::TIMESTAMPTZ AS hour_beginning,
        "@LocId"::UINTEGER AS ptid,
        LmpTotal::DECIMAL(9,4) AS "lmp",
        CongestionComponent::DECIMAL(9,4) AS "mcc",
        LossComponent::DECIMAL(9,4) AS "mcl" 
    FROM (
        SELECT DISTINCT BeginDate, "@LocId", LmpTotal, CongestionComponent, LossComponent FROM (
            SELECT unnest(HourlyLmps.HourlyLmp, recursive := true)
            FROM read_json('{}/Raw/{}/WW_DALMP_ISO_{}*.json.gz')
        )
    )
    ORDER BY hour_beginning, ptid
;

INSERT INTO da_lmp
(SELECT * FROM tmp 
WHERE NOT EXISTS (
    SELECT * FROM da_lmp d
    WHERE d.hour_beginning = tmp.hour_beginning
    AND d.ptid = tmp.ptid
    )
)
ORDER BY hour_beginning, ptid;
"#,
            self.base_dir,
            month.start_date().year(),
            month.start_date().strftime("%Y%m"),
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

    /// Data is usually published before XX:XX every day
    /// Download the zip file for the date which contains 4 files (one per component)
    /// https://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=12&startdatetime=20251206T08:00-0000&enddatetime=20251207T08:00-0000&market_run_id=DAM&grp_type=ALL
    pub async fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        let start = date.at(0, 0, 0, 0).in_tz("America/Los_Angeles")?;
        let start_z = start.in_tz("UTC")?.strftime("%Y%m%dT%H:%M-0000");
        let url = format!("https://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=12&startdatetime={}T08:00-0000&enddatetime={}T08:00-0000&market_run_id=DAM&grp_type=ALL", start_z, start_z);

        let resp = get(&url).await?;
        let stream = resp.bytes_stream();
        let mut reader = StreamReader::new(
            stream.map(|r| r.map_err(std::io::Error::other)),
        );
        let out_path = format!("{}.zip", self.filename(&date, LmpComponent::Lmp));
        let mut out = File::create(out_path).await?;
        let out = tokio::io::copy(&mut reader, &mut out).await?;
        println!("downloaded {} bytes", out);

        Ok(())
    }

    /// Look for missing days
    pub async fn download_missing_days(&self, month: Month) -> Result<(), Box<dyn Error>> {
        let mut last = Zoned::now().date();
        if Zoned::now().hour() > 13 {
            last = last.tomorrow()?;
        }
        for day in month.days() {
            if day > last {
                continue;
            }
            let fname = format!("{}.gz", self.filename(&day, LmpComponent::Lmp));
            if !Path::new(&fname).exists() {
                info!("Working on {}", day);
                self.download_file(day).await?;
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
    #[tokio::test]
    async fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::caiso_dalmp();

        let months = month(2025, 12).up_to(month(2025, 12));
        for month in months.unwrap() {
            info!("Working on month {}", month);
            archive.download_missing_days(month).await?;
            // archive.update_duckdb(&month)?;
        }
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::caiso_dalmp();
        archive.download_file(date(2025, 11, 1)).await?;
        Ok(())
    }
}
