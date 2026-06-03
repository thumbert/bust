use jiff::civil::*;
use log::{error, info};
use std::error::Error;
use std::path::Path;
use std::process::Command;

use crate::api::isone::_api_isone_core::Market;
use crate::db::isone::lib_isoexpress;
use crate::interval::month::Month;

#[derive(Clone)]
pub struct ImportExportArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl ImportExportArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date, market: &Market) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/hbimportexport_"
            + &market.to_string().to_lowercase()
            + "_"
            + &date.to_string()
            + ".json"
    }

    /// https://webservices.iso-ne.com/api/v1.1/hbimportexport/marketType/da/day/20230104
    pub fn download_file(&self, date: &Date, market: &Market) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/hbimportexport/marketType/{}/day/{}",
                market, yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(date, market)),
            true,
        )
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting DA and RT daily files for the month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS bidsoffers (
    hour_beginning TIMESTAMPTZ NOT NULL,
    market_type ENUM('DA', 'RT') NOT NULL,
    masked_customer_id UINTEGER NOT NULL,
    masked_source_id UINTEGER NOT NULL,
    masked_sink_id UINTEGER NOT NULL,
    emergency_flag BOOLEAN NOT NULL,
    direction ENUM('IMPORT', 'EXPORT') NOT NULL,
    transaction_type ENUM('FIXED', 'DISPATCHABLE', 'UP-TO CONGESTION') NOT NULL,
    mw DECIMAL(9,2) NOT NULL,
    price DECIMAL(9,2),
);

LOAD icu;SET TimeZone = 'America/New_York';
CREATE TEMPORARY TABLE tmp AS
    SELECT 
        json_extract(aux, '$.BeginDate')::TIMESTAMPTZ AS hour_beginning,
        json_extract(aux, '$.MarketType')::ENUM('DA', 'RT') AS market_type,
        json_extract(aux, '$.MaskedCustomerId')::UINTEGER AS masked_customer_id,
        json_extract(aux, '$.MaskedSourceId')::UINTEGER AS masked_source_id,
        json_extract(aux, '$.MaskedSinkId')::UINTEGER AS masked_sink_id,
        IF(json_extract(aux, '$.EmergencyFlag') = '"Y"', TRUE, FALSE) AS emergency_flag,
        json_extract(aux, '$.Direction')::ENUM('IMPORT', 'EXPORT') AS direction,
        json_extract(aux, '$.TransactionType')::ENUM('FIXED', 'DISPATCHABLE', 'UP-TO CONGESTION') AS transaction_type,
        json_extract(aux, '$.Mw')::DECIMAL(9,2) AS mw,
        json_extract(aux, '$.Price')::DECIMAL(9,2) AS price
    FROM (
        SELECT unnest(HbImportExports.HbImportExport)::JSON as aux
        FROM read_json('{}/Raw/{}/hbimportexport_*_{}-*.json.gz')
    )
;

INSERT INTO bidsoffers
(SELECT * FROM tmp t
WHERE NOT EXISTS (
    SELECT * FROM bidsoffers b
    WHERE
        b.hour_beginning = t.hour_beginning AND
        b.market_type = t.market_type AND
        b.masked_customer_id = t.masked_customer_id AND
        b.masked_source_id = t.masked_source_id AND
        b.masked_sink_id = t.masked_sink_id AND
        b.emergency_flag = t.emergency_flag AND
        b.direction = t.direction AND
        b.transaction_type = t.transaction_type AND
        b.mw = t.mw AND
        b.price = t.price
    )    
)
ORDER BY hour_beginning, market_type, masked_customer_id;
"#,
            self.base_dir,
            month.start_date().year(),
            month
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
}

#[cfg(test)]
mod tests {

    use jiff::civil::date;
    use std::{error::Error, path::Path};

    use crate::{
        db::prod_db::ProdDb,
        interval::{interval_base::DateExt, month::month},
    };
    // use crate::interval::interval::DateExt;

    use super::*;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::isone_masked_import_export();
        let days = date(2023, 1, 1).up_to(date(2023, 12, 31));
        for day in &days {
            println!("Processing {}", day);
            archive.download_file(day, &Market::DA)?;
            archive.download_file(day, &Market::RT)?;
        }
        let months = month(2023, 3).up_to(month(2024, 12))?;
        for month in &months {
            println!("Updating DuckDB for month {}", month);
            archive.update_duckdb(month)?;
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_masked_import_export();
        archive.download_file(&date(2025, 1, 1), &Market::DA)?;
        Ok(())
    }
}
