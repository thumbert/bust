use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use duckdb::Connection;
use jiff::civil::Date;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use convert_case::{Case, Casing};
use jiff::Timestamp;
use jiff::{tz::TimeZone, Zoned};
use log::{error, info};
use rust_decimal::Decimal;
use std::fs::{self, File};
use std::io::Read;
use std::process::Command;
use std::str::FromStr;

use crate::interval::month::Month;
use crate::utils::serde_helpers::*;

#[derive(Clone)]
pub struct NyisoCapacityOffersArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl NyisoCapacityOffersArchive {
    /// Return the full file path of the zip file with data for the entire month  
    pub fn filename_zip(&self, month: &Month) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + month.year().to_string().as_str()
            + "/"
            + &month.strftime("%Y%m").to_string()
            + "01biddata_icapbids_csv.zip"
    }

    /// Return the file path of the csv file with data for one day
    pub fn filename_bids(&self, month: &Month) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + month.year().to_string().as_str()
            + "/"
            + &month.strftime("%Y%m").to_string()
            + "01biddata_icapbids_csv.zip"
    }

    /// Data is published around 10:30 every day
    /// See https://mis.nyiso.com/public/csv/biddata/20260201biddata_icapbids_csv.zip
    ///     https://mis.nyiso.com/public/csv/biddata/20260201biddata_icapbids_csv.zip
    /// Take the monthly zip file, extract it and compress each individual day as a gz file.
    pub fn download_file(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let binding = self.filename_zip(month);
        let zip_path = Path::new(&binding);
        // create the directory if it doesn't exist
        let dir = zip_path.parent().unwrap();
        fs::create_dir_all(dir)?;

        let url = format!(
            "https://mis.nyiso.com/public/csv/biddata/{}",
            zip_path.file_name().unwrap().to_str().unwrap()
        );
        println!("Downloading file from URL: {}", url);
        let mut resp = reqwest::blocking::get(url)?;
        let mut out = File::create(&binding)?;
        std::io::copy(&mut resp, &mut out)?;
        info!("downloaded file: {}", binding);

        // Unzip the file
        info!("Unzipping file {:?}", zip_path);
        let mut zip_file = File::open(zip_path)?;
        let mut zip_data = Vec::new();
        zip_file.read_to_end(&mut zip_data)?;
        let reader = std::io::Cursor::new(zip_data);
        let mut zip = zip::ZipArchive::new(reader)?;
        use std::fs::File as StdFile;
        use std::io::copy as std_copy;

        for i in 0..zip.len() {
            let mut file = zip.by_index(i)?;
            let out_path = match file.enclosed_name() {
                Some(path) => path.to_owned(),
                None => continue,
            };
            let month: Month = out_path.file_name().unwrap().to_str().unwrap()[0..6]
                .parse()
                .map_err(|_| format!("Invalid month in filename: {:?}", out_path))?;
            let out_path = self.base_dir.to_owned()
                + "/Raw/"
                + &month.year().to_string()
                + "/"
                + out_path.file_name().unwrap().to_str().unwrap();
            let dir = Path::new(&out_path).parent().unwrap();
            fs::create_dir_all(dir)?;

            // Use blocking std::fs::File and std::io::copy for extraction
            let mut outfile = StdFile::create(&out_path)?;
            std_copy(&mut file, &mut outfile)?;
            info!(" -- extracted file to {}", out_path);

            // Gzip the csv file
            let mut csv_file = File::open(&out_path)?;
            let mut csv_data = Vec::new();
            csv_file.read_to_end(&mut csv_data)?;
            let gz_path = format!("{}.gz", out_path);
            let mut gz_file = File::create(&gz_path)?;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            use std::io::Write;
            encoder.write_all(&csv_data)?;
            let compressed_data = encoder.finish()?;
            gz_file.write_all(&compressed_data)?;
            info!(" -- gzipped file to {}", gz_path);

            // Remove the original csv file
            std::fs::remove_file(&out_path)?;
        }

        // Remove the zip file
        std::fs::remove_file(zip_path)?;
        info!("removed zip file {:?}", zip_path);

        Ok(())
    }

    /// Update duckdb with published data for the month.  No checks are made to see
    /// if there are missing files.  Does not delete any existing data.  So if data
    /// is wrong for some reason, it needs to be manually deleted first!
    ///
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting da binding constraint files for the month {} ...",
            month
        );
        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS binding_constraints (
    market ENUM('DA', 'RT') NOT NULL,
    hour_beginning TIMESTAMPTZ NOT NULL,
    limiting_facility VARCHAR NOT NULL,
    facility_ptid INT64 NOT NULL,
    contingency VARCHAR NOT NULL,
    constraint_cost DECIMAL(9,4) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS (
    SELECT 
        'DA' AS market,
        case "TIME ZONE" 
            when 'EST' then strptime("Time Stamp" || ' -0500', '%m/%d/%Y %H:%M %z')::TIMESTAMPTZ
            when 'EDT' then strptime("Time Stamp" || ' -0400', '%m/%d/%Y %H:%M %z')::TIMESTAMPTZ
            else NULL
        end AS hour_beginning, 
        "Limiting Facility"::VARCHAR AS limiting_facility,
        "Facility PTID"::INT64 AS facility_ptid,
        "Contingency"::VARCHAR AS contingency,
        "Constraint Cost($)"::DECIMAL(9,4) AS constraint_cost
    FROM read_csv('{}/Raw/{}/{}*DAMLimitingConstraints.csv.gz', 
        header = true,
        types = {{'Facility PTID': 'INT64', 'Constraint Cost($)': 'DECIMAL(9,4)'}}
));


INSERT INTO binding_constraints
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM binding_constraints d
        WHERE
            d.market = t.market AND
            d.hour_beginning = t.hour_beginning AND
            d.limiting_facility = t.limiting_facility AND
            d.facility_ptid = t.facility_ptid AND
            d.contingency = t.contingency AND
            d.constraint_cost = t.constraint_cost
    )
);

        "#,
            self.base_dir,
            month.start_date().year(),
            &month.start_date().strftime("%Y%m"),
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
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::prod_db::ProdDb, interval::month::month};
    use duckdb::{AccessMode, Config, Connection};
    use std::error::Error;

    // #[test]
    // fn test_get_data() -> Result<(), Box<dyn Error>> {
    //     let config = Config::default().access_mode(AccessMode::ReadOnly)?;
    //     let conn =
    //         Connection::open_with_flags(ProdDb::nyiso_binding_constraints_da().duckdb_path, config)
    //             .unwrap();
    //     let filter = QueryFilterBuilder::new().build();
    //     let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
    //     conn.close().unwrap();
    //     assert_eq!(xs.len(), 5);
    //     Ok(())
    // }

    // #[ignore]
    // #[test]
    // fn update_db() -> Result<(), Box<dyn Error>> {
    //     let _ = env_logger::builder()
    //         .filter_level(log::LevelFilter::Info)
    //         .is_test(true)
    //         .try_init();
    //     dotenvy::from_path(Path::new(".env/test.env")).unwrap();
    //     let archive = ProdDb::nyiso_binding_constraints_da();

    //     let months = month(2020, 2).up_to(month(2026, 1))?;
    //     for month in months {
    //         println!("Processing month {}", month);
    //         archive.update_duckdb(month)?;
    //     }
    //     Ok(())
    // }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();

        let archive = ProdDb::nyiso_capacity_offers();
        let months = month(2024, 1).up_to(month(2024, 12))?;
        for month in months {
            archive.download_file(&month)?;
        }
        Ok(())
    }
}
