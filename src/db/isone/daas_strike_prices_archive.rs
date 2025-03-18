use flate2::read::GzDecoder;
use jiff::{civil::*, Timestamp, Zoned};
use log::info;
use rust_decimal::Decimal;
use serde_json::Value;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::str::FromStr;

use crate::interval::month::Month;

#[derive(Debug, PartialEq)]
pub struct Row {
    hour_beginning: Zoned,
    strike_price: Decimal,
    strike_price_zoned: Zoned,
    spc_load_forecast_mw: Decimal,
    percentile_10_rt_hub_lmp: Decimal,
    percentile_25_rt_hub_lmp: Decimal,
    percentile_75_rt_hub_lmp: Decimal,
    percentile_90_rt_hub_lmp: Decimal,
    expected_rt_hub_lmp: Decimal,
    expected_closeout_charge: Decimal,
    expected_closeout_charge_override: Decimal,
}

#[derive(Clone)]
pub struct DaasStrikePricesArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl DaasStrikePricesArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/daas_strike_prices_"
            + &date.to_string()
            + ".json"
    }

    /// Data is published around 10:30 every day
    pub fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        super::lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/daasstrikeprices/day/{}",
                yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(&date)),
            true,
        )
    }

    /// Look for missing days
    pub fn download_missing_days(&self, month: Month) -> Result<(), Box<dyn Error>> {
        let mut last = Zoned::now().date();
        if Zoned::now().hour() > 13 {
            last = last.tomorrow()?;
        }
        for day in month.days() {
            info!("Working on {}", day);
            if day > last {
                continue;
            }
            let fname = format!("{}.gz", self.filename(&day));
            if !Path::new(&fname).exists() {
                self.download_file(day)?;
                info!("  downloaded file for {}", day);
            }
        }
        Ok(())
    }

    /// Read one json.gz file corresponding to one day.
    pub fn read_file(&self, path_gz: String) -> Result<Vec<Row>, Box<dyn Error>> {
        let mut file = GzDecoder::new(File::open(path_gz).unwrap());
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let doc: Value = serde_json::from_str(&buffer)?;
        let vs =
            doc["isone_web_services"]["day_ahead_strike_prices"]["day_ahead_strike_price"].clone();
        let mut rows: Vec<Row> = Vec::new();
        match vs {
            Value::Array(values) => {
                for v in values {
                    let timestamp: Timestamp = match v["market_hour"]["local_day"].clone() {
                        Value::String(s) => s.parse()?,
                        _ => panic!("local_day field is no longer a string"),
                    };
                    let strike_price_zoned: Zoned = match v["strike_price_timestamp"].clone() {
                        Value::String(s) => s.parse::<Timestamp>()?.in_tz("America/New_York")?,
                        _ => panic!("strike_price_timestamp field is no longer a string"),
                    };
                    let hour_beginning = timestamp.in_tz("America/New_York")?;
                    let row = Row {
                        hour_beginning,
                        strike_price: Decimal::from_str(&format!("{}", v["strike_price"]))?,
                        strike_price_zoned,
                        spc_load_forecast_mw: Decimal::from_str(&format!(
                            "{}",
                            v["total_ten_min_req_mw"]
                        ))?,
                        percentile_10_rt_hub_lmp: Decimal::from_str(&format!(
                            "{}",
                            v["percentile_10_rt_hub_lmp"]
                        ))?,
                        percentile_25_rt_hub_lmp: Decimal::from_str(&format!(
                            "{}",
                            v["percentile_25_rt_hub_lmp"]
                        ))?,
                        percentile_75_rt_hub_lmp: Decimal::from_str(&format!(
                            "{}",
                            v["percentile_75_rt_hub_lmp"]
                        ))?,
                        percentile_90_rt_hub_lmp: Decimal::from_str(&format!(
                            "{}",
                            v["percentile_90_rt_hub_lmp"]
                        ))?,
                        expected_rt_hub_lmp: Decimal::from_str(&format!(
                            "{}",
                            v["expected_rt_hub_lmp"]
                        ))?,

                        expected_closeout_charge: Decimal::from_str(&format!(
                            "{}",
                            v["expected_closeout_charge"]
                        ))?,
                        expected_closeout_charge_override: Decimal::from_str(&format!(
                            "{}",
                            v["expected_closeout_charge_override"]
                        ))?,
                    };
                    rows.push(row);
                }
            }
            _ => panic!("File format changed!"),
        };

        Ok(rows)
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
        let archive = ProdDb::isone_daas_strike_prices();
        // archive.setup()

        let month = month(2025, 3);
        archive.download_missing_days(month)?;
        // archive.update_duckdb(month)?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_daas_strike_prices();
        archive.download_file(date(2025, 3, 9))?;
        Ok(())
    }
}
