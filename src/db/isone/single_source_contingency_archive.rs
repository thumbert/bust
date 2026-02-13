use duckdb::Connection;
use jiff::civil::*;
use log::{error, info};
use std::collections::HashSet;
use std::error::Error;
use std::path::Path;

#[derive(Clone)]
pub struct SingleSourceContingencyArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SingleSourceContingencyArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/ssc_"
            + &date.to_string()
            + ".json"
    }

    /// Data is updated every 5 min or so
    pub fn download_file(&self, date: &Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        super::lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/singlesrccontingencylimits/day/{}",
                yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(date)),
            true,
        )
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.
    /// This method only works well for a few day.  For a lot of days, don't loop over days.
    /// Consider using DuckDB directly by globbing the file names.
    ///  
    pub fn update_duckdb(&self, days: &HashSet<Date>) -> Result<(), Box<dyn Error>> {
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
CREATE TABLE IF NOT EXISTS ssc (
        BeginDate TIMESTAMPTZ NOT NULL,
        RtFlowMw DOUBLE NOT NULL,
        LowestLimitMw DOUBLE NOT NULL,
        DistributionFactor DOUBLE NOT NULL,
        InterfaceName VARCHAR NOT NULL,
        ActualMarginMw DOUBLE NOT NULL,
        AuthorizedMarginMw DOUBLE NOT NULL,
        BaseLimitMw DOUBLE NOT NULL,
        SingleSourceContingencyLimitMw DOUBLE NOT NULL,
);",
        )?;
        conn.execute_batch(
            r"
CREATE TEMPORARY TABLE tmp (
        BeginDate TIMESTAMPTZ NOT NULL,
        RtFlowMw DOUBLE NOT NULL,
        LowestLimitMw DOUBLE NOT NULL,
        DistributionFactor DOUBLE NOT NULL,
        InterfaceName VARCHAR NOT NULL,
        ActMarginMw DOUBLE NOT NULL,
        AuthorizedMarginMw DOUBLE NOT NULL,
        BaseLimitMw DOUBLE NOT NULL,
        SingleSrcContingencyMw DOUBLE NOT NULL,
);",
        )?;

        for day in days {
            let path = self.filename(day) + ".gz";
            if !Path::new(&path).exists() {
                info!("No file for {}.  Skipping", day);
                continue;
            }

            // insert into duckdb
            conn.execute_batch(&format!(
                "
INSERT INTO tmp
    SELECT unnest(SingleSrcContingencyLimits.SingleSrcContingencyLimit, recursive := true)
    FROM read_json('~/Downloads/Archive/IsoExpress/SingleSourceContingency/Raw/{}/ssc_{}.json.gz')
;",
                day.year(),
                day
            ))?;

            let query = r"
INSERT INTO ssc
    SELECT 
        BeginDate::TIMESTAMPTZ,
        RtFlowMw::DOUBLE,
        LowestLimitMw::DOUBLE,
        DistributionFactor::DOUBLE,
        InterfaceName::VARCHAR,
        ActMarginMw::DOUBLE as ActualMarginMw,
        AuthorizedMarginMw::DOUBLE,
        BaseLimitMw::DOUBLE,
        SingleSrcContingencyMw::DOUBLE as SingleSourceContingencyLimitMw,
    FROM tmp
EXCEPT 
    SELECT * FROM ssc
;";
            match conn.execute(query, []) {
                Ok(updated) => info!("{} rows were updated for day {}", updated, day),
                Err(e) => error!("{}", e),
            }
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use jiff::{civil::date, ToSpan, Zoned};
    use std::{error::Error, path::Path};

    use crate::db::prod_db::ProdDb;

    use super::*;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::isone_single_source_contingency();
        // let days = vec![date(2024, 12, 4), date(2024, 12, 5), date(2024, 12, 6)];
        // let days: Vec<Date> = date(2024, 1, 1).series(1.day()).take(366).collect();
        // let days: HashSet<Date> = date(2024, 4, 1)
        //     .series(1.day())
        //     .take_while(|e| e <= &date(2024, 12, 31))
        //     .collect();
        let today = Zoned::now().date();
        let days: HashSet<Date> = date(2025, 4, 29)
            .series(1.day())
            .take_while(|e| e <= &today)
            .collect();
        for day in &days {
            println!("Processing {}", day);
            archive.download_file(day)?;
        }
        archive.update_duckdb(&days)?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_single_source_contingency();
        archive.download_file(&date(2025, 1, 9))?;
        Ok(())
    }
}
