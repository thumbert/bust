use jiff::civil::*;
use log::{error, info};
use std::error::Error;
use std::path::Path;
use std::process::Command;

use crate::db::isone::lib_isoexpress;
use crate::interval::month::Month;

#[derive(Clone)]
pub struct DaasOffersArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl DaasOffersArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/hbdaasenergyoffer_"
            + &date.to_string()
            + ".json"
    }

    /// https://webservices.iso-ne.com/api/v1.1/hbdaasenergyoffer/day/20250301
    pub fn download_file(&self, date: &Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/hbdaasenergyoffer/day/{}",
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
CREATE TABLE IF NOT EXISTS offers (
    hour_beginning TIMESTAMPTZ NOT NULL,
    masked_lead_participant_id INTEGER NOT NULL,
    masked_asset_id INTEGER NOT NULL,
    offer_mw DECIMAL(9,2) NOT NULL,
    tmsr_offer_price DECIMAL(9,2) NOT NULL,
    tmnsr_offer_price DECIMAL(9,2) NOT NULL,
    tmor_offer_price DECIMAL(9,2) NOT NULL,
    eir_offer_price DECIMAL(9,2) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT * 
    FROM (
        SELECT unnest(isone_web_services.offer_publishing.day_ahead_ancillary_services.daas_gen_offer_data, recursive := true)
        FROM read_json('{}/Raw/{}/hbdaasenergyoffer_{}-*.json.gz')
    )
    ORDER BY local_day
;

INSERT INTO offers
(
    SELECT 
        local_day::TIMESTAMPTZ as hour_beginning,
        masked_lead_participant_id::INTEGER as masked_lead_participant_id,
        masked_asset_id::INTEGER as masked_asset_id,
        offer_mw::DECIMAL(9,2) as offer_mw,
        tmsr_offer_price::DECIMAL(9,2) as tmsr_offer_price,
        tmnsr_offer_price::DECIMAL(9,2) as tmnsr_offer_price,
        tmor_offer_price::DECIMAL(9,2) as tmor_offer_price,
        eir_offer_price::DECIMAL(9,2) as eir_offer_price
    FROM tmp t
WHERE NOT EXISTS (
        SELECT * FROM offers o
        WHERE
            o.hour_beginning = t.local_day AND
            o.masked_lead_participant_id = t.masked_lead_participant_id AND
            o.masked_asset_id = t.masked_asset_id AND
            o.tmsr_offer_price = t.tmsr_offer_price AND
            o.tmnsr_offer_price = t.tmnsr_offer_price AND
            o.tmor_offer_price = t.tmor_offer_price AND
            o.eir_offer_price = t.eir_offer_price
    )
) ORDER BY hour_beginning, masked_lead_participant_id, masked_asset_id; 
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
        interval::term::Term,
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
        let term = "Apr25-May25".parse::<Term>()?;
        for day in &term.days() {
            println!("Processing {}", day);
            archive.download_file(day)?;
        }
        let months = term.months();
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
        let archive = ProdDb::isone_masked_daas_offers();
        archive.download_file(&date(2025, 5, 26))?;
        Ok(())
    }
}
