use csv::ReaderBuilder;
use duckdb::Connection;
use flate2::read::GzDecoder;
use jiff::tz::{self, TimeZone};
use jiff::{civil::*, Zoned};
use log::{error, info};
use std::error::Error;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::{collections::HashSet, fs::File};

use crate::db::isone::lib_isoexpress::download_file;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct IesoDaLmpNodalArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IesoDaLmpNodalArchive {
    /// Return the csv filename for the day
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/PUB_DAHourlyEnergyLMP_"
            + &date.strftime("%Y%m%d").to_string()
            + ".csv"
    }

    /// Data is published every day after 12PM
    pub fn download_file(&self, date: &Date) -> Result<(), Box<dyn Error>> {
        download_file(
            format!(
                "https://reports-public.ieso.ca/public/DAHourlyEnergyLMP/{}",
                Path::new(&self.filename(date))
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("")
            ),
            false,
            None,
            Path::new(&self.filename(date)),
            true,
        )
    }

    pub fn read_file(&self, date: &Date) -> Result<Vec<Row>, Box<dyn Error>> {
        let file = File::open(self.filename(date) + ".gz")?;
        let gz_decoder = GzDecoder::new(file);
        let buf_reader = BufReader::new(gz_decoder);

        // Skip the first two lines
        let mut lines = buf_reader.lines();
        let _ = lines.next(); // Skip the first line with the timestamp

        // Collect the remaining lines into a string
        let remaining_content: String = lines
            .collect::<Result<Vec<String>, _>>()?
            .join("\n");

        // Create a CSV reader from the remaining content
        let mut csv_reader = ReaderBuilder::new()
            .has_headers(true) // Set to false if your remaining data doesn't have headers
            .from_reader(remaining_content.as_bytes());

        // Deserialize and process each record
        let mut rows = Vec::new();
        for result in csv_reader.records() {
            let record = result?;
            let hour: i8 = record.get(0).unwrap().parse()?;
            let begin_hour = date
                .at(hour - 1, 0, 0, 0)
                .to_zoned(TimeZone::fixed(tz::offset(-5)))?;
            let location_name = record.get(1).unwrap_or_default().to_string();
            let lmp = record.get(2).unwrap_or_default().parse::<f64>()?;
            let mcc = record.get(3).unwrap_or_default().parse::<f64>()?;
            let mcl = record.get(4).unwrap_or_default().parse::<f64>()?;
            rows.push(Row {
                begin_hour,
                location_name,
                lmp,
                mcc,
                mcl,
            });
        }
        rows.sort_unstable_by_key(|e| (e.location_name.clone(), e.begin_hour.clone()));

        Ok(rows)
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


#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub location_name: String,
    pub begin_hour: Zoned,
    pub lmp: f64,
    pub mcc: f64,
    pub mcl: f64,
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
    fn read_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::ieso_dalmp_nodes();
        let rows = archive.read_file(&date(2025, 5, 5))?;

        let ab: Vec<&Row> = rows
            .iter()
            .filter(|row| row.location_name == "ABKENORA-LT.AG")
            .collect();
        assert_eq!(ab.len(), 24);
        assert_eq!(ab[16].lmp, 10.28);

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::ieso_dalmp_nodes();
        archive.download_file(&date(2025, 5, 5))?;
        Ok(())
    }
}
