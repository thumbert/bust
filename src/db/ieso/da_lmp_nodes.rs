use csv::ReaderBuilder;
use flate2::read::GzDecoder;
use jiff::tz::{self, TimeZone};
use jiff::{civil::*, Zoned};
use log::{error, info};
use rust_decimal::Decimal;
use std::error::Error;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use std::fs::File;

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
        let remaining_content: String = lines.collect::<Result<Vec<String>, _>>()?.join("\n");

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
            let lmp = Decimal::from_str(record.get(2).unwrap())?;
            let mcc = Decimal::from_str(record.get(3).unwrap())?;
            let mcl = Decimal::from_str(record.get(4).unwrap())?;
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

    /// Upload one individual day to DuckDB.
    /// Assumes a csv.gz file exists.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, day: &Date) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting DALMP hourly prices for IESO's hubs for day {} ...",
            day
        );
        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS da_lmp (
    location_type ENUM('AREA', 'HUB', 'NODE') NOT NULL,
    location_name VARCHAR NOT NULL,
    hour_beginning TIMESTAMPTZ NOT NULL,
    lmp DECIMAL(9,4) NOT NULL,
    mcc DECIMAL(9,4) NOT NULL,
    mcl DECIMAL(9,4) NOT NULL,
);

CREATE TEMPORARY TABLE tmp_n
AS
    SELECT 
        'NODE' AS location_type,
        "Pricing Location" AS location_name,
        ('{} ' ||  hour-1 || ':00:00.000-05:00')::TIMESTAMPTZ AS hour_beginning,
        "LMP" AS lmp,
        "Energy Loss Price" as mcl,
        "Energy Congestion Price" as mcc
    FROM read_csv('{}/Raw/{}/PUB_DAHourlyEnergyLMP_{}.csv.gz', 
    skip = 1,
    columns = {{
        'hour': "UINT8 NOT NULL",
        'Pricing Location': "VARCHAR NOT NULL",
        'LMP': "DECIMAL(9,4) NOT NULL",
        'Energy Loss Price': "DECIMAL(9,4) NOT NULL",
        'Energy Congestion Price': "DECIMAL(9,4) NOT NULL"
        }}
    )
;

INSERT INTO da_lmp BY NAME
(SELECT * FROM tmp_n 
WHERE NOT EXISTS (
    SELECT * FROM da_lmp d
    WHERE d.hour_beginning = tmp_n.hour_beginning
    AND d.location_name = tmp_n.location_name
    )
)
ORDER BY hour_beginning, location_name;
"#,
            day,
            self.base_dir,
            day.year(),
            day.strftime("%Y%m%d"),
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
            error!("Failed to update duckdb for day {}: {}", day, stderr);
        }

        Ok(())
    }
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub location_name: String,
    pub begin_hour: Zoned,
    pub lmp: Decimal,
    pub mcc: Decimal,
    pub mcl: Decimal,
}

#[cfg(test)]
mod tests {

    use jiff::civil::date;
    use rust_decimal_macros::dec;
    use std::{error::Error, path::Path};

    use crate::{db::prod_db::ProdDb, interval::term::Term};

    use super::*;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::ieso_dalmp_nodes();
        let term = "Jul25-Aug25".parse::<Term>()?;
        for day in &term.days() {
            println!("Processing {}", day);
            // archive.download_file(day)?;
            archive.update_duckdb(day)?;
        }
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
        assert_eq!(ab[16].lmp, dec!(10.28));

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::ieso_dalmp_nodes();

        let term = "1Jun25-27Aug25".parse::<Term>().unwrap();
        for day in term.days() {
            archive.download_file(&day)?;
        }
        Ok(())
    }
}
