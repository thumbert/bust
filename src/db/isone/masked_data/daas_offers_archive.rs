use jiff::civil::*;
use log::{error, info};
use std::error::Error;
use std::path::Path;
use std::process::Command;

use crate::db::isone::lib_isoexpress;
use crate::interval::month::Month;

#[derive(Clone)]
pub struct DaasEnergyOffersArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl DaasEnergyOffersArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
        + "/Raw/"
        + &date.year().to_string()
        + "/hbdaasenergyoffer_"
        + &date.to_string()
        + ".json"
    }
    
    /// https://webservices.iso-ne.com/docs/v1.1/hbdaasenergyoffer/day/20250301
    pub fn download_file(&self, date: &Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/hbdaasoffers/day/{}",
                yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(date)),
            true,
        )
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting daily DAAS energy offers files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS bidsoffers (
        HourBeginning TIMESTAMPTZ NOT NULL,
        MarketType ENUM('DA', 'RT') NOT NULL,
        MaskedCustomerId UINTEGER NOT NULL,
        MaskedSourceId UINTEGER NOT NULL,
        MaskedSinkId UINTEGER NOT NULL,
        EmergencyFlag BOOLEAN NOT NULL,
        Direction ENUM('IMPORT', 'EXPORT') NOT NULL,
        TransactionType ENUM('FIXED', 'DISPATCHABLE', 'UP-TO CONGESTION') NOT NULL,
        Mw DECIMAL(9,2) NOT NULL,
        Price DECIMAL(9,2),
);
CREATE TEMPORARY TABLE tmp AS
    SELECT unnest(HbImportExports.HbImportExport, recursive := true)
    FROM read_json('~/Downloads/Archive/IsoExpress/PricingReports/ImportExport/Raw/{}/hbimportexport_*_{}-*.json.gz')
;

CREATE TEMPORARY TABLE tmp1 AS
    (SELECT 
        BeginDate::TIMESTAMPTZ as HourBeginning,
        MarketType::ENUM('DA', 'RT') as MarketType,
        MaskedCustomerId::UINTEGER as MaskedCustomerId,
        MaskedSourceId::UINTEGER as MaskedSourceId,
        MaskedSinkId::UINTEGER as MaskedSinkId,
        IF(EmergencyFlag = 'Y', TRUE, FALSE) as EmergencyFlag,
        Direction::ENUM('IMPORT', 'EXPORT') as Direction,
        TransactionType::ENUM('FIXED', 'DISPATCHABLE', 'UP-TO CONGESTION') as TransactionType,
        Mw::DECIMAL(9,2) as Mw,
        Price::DECIMAL(9,2) as Price
    FROM tmp
    ORDER BY MarketType, HourBeginning, MaskedCustomerId);

INSERT INTO bidsoffers
(SELECT * FROM tmp1 t
WHERE NOT EXISTS (
    SELECT * FROM bidsoffers b
    WHERE
        b.HourBeginning = t.HourBeginning AND
        b.MarketType = t.MarketType AND
        b.MaskedCustomerId = t.MaskedCustomerId AND
        b.MaskedSourceId = t.MaskedSourceId AND
        b.MaskedSinkId = t.MaskedSinkId AND
        b.EmergencyFlag = t.EmergencyFlag AND
        b.Direction = t.Direction AND
        b.TransactionType = t.TransactionType AND
        b.Mw = t.Mw AND
        b.Price = t.Price
    )
)
ORDER BY HourBeginning, MarketType, MaskedCustomerId;"#,
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
        interval::{interval::DateExt, month::month},
    };


    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::isone_masked_daas_offers();
        let days = date(2023, 1, 1).up_to(date(2023, 12, 31));   
        // let days = vec![
        //     date(2023, 1, 5),
        //     date(2023, 1, 7),
        //     date(2023, 1, 8),
        //     date(2023, 1, 9),
        // ];
        for day in &days {
            println!("Processing {}", day);
            archive.download_file(day)?;
        }
        // let months = month(2023, 2).up_to(month(2023, 12))?;
        // for month in &months {
        //     println!("Updating DuckDB for month {}", month);
        //     archive.update_duckdb(month)?;
        // }
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_masked_daas_offers();
        archive.download_file(&date(2025, 1, 1))?;
        Ok(())
    }
}
