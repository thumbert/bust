use jiff::civil::Date;
use jiff::Zoned;
use log::{error, info};
use std::error::Error;
use std::path::Path;
use std::process::Command;

use crate::interval::month::Month;

#[derive(Clone)]
pub struct IsoneEventsCalendarArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IsoneEventsCalendarArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/events_"
            + &date.strftime("%Y%m%d").to_string()
            + ".json"
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting actual interface flow files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS flows (
    hour_beginning TIMESTAMPTZ NOT NULL,
    ptid UINTEGER NOT NULL,
    Net DECIMAL(9,2) NOT NULL,
    Purchase DECIMAL(9,2) NOT NULL,
    Sale DECIMAL(9,2) NOT NULL,
);

CREATE TEMPORARY TABLE tmp AS
    SELECT 
        BeginDate::TIMESTAMPTZ AS hour_beginning,
        "@LocId"::UINTEGER AS ptid,
        ActInterchange::DECIMAL(9,2) AS Net,
        Purchase::DECIMAL(9,2) AS Purchase,
        Sale::DECIMAL(9,2) AS Sale
    FROM (
        SELECT unnest(ActualInterchanges.ActualInterchange, recursive := true)
        FROM read_json('{}/Raw/{}/act_interchange_{}*.json.gz')
    )
ORDER BY hour_beginning, ptid;

INSERT INTO flows
(SELECT * FROM tmp 
WHERE NOT EXISTS (
    SELECT * FROM flows d
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


    /// https://www.iso-ne.com/api/1/services/events.json?sortBy=event_start_date_gmt+asc&fromDate=2026-02-02T00:00:00&toDate=2026-02-02T23:59:59
    pub fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        super::lib_isoexpress::download_file(
            format!(
                "https://www.iso-ne.com/api/1/services/events.json?sortBy=event_start_date_gmt+asc&fromDate={}&toDate={}",
                date.strftime("%Y-%m-%dT00:00:00"),
                date.strftime("%Y-%m-%dT23:59:59")
            ),
            false,
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
        let archive = ProdDb::isone_events_calendar();

        let months = month(2026, 1).up_to(month(2026, 1)).unwrap();
        for month in months {
            // archive.download_missing_days(month)?;
            archive.update_duckdb(&month)?;
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_events_calendar();
        archive.download_file(date(2025, 1, 1))?;
        Ok(())
    }
}




