use duckdb::{params, Connection};
use flate2::read::GzDecoder;
use jiff::{civil::*, Timestamp, Zoned};
use log::{error, info};
use rust_decimal::Decimal;
use serde_json::Value;
use std::error::Error;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::str::FromStr;

use crate::interval::month::Month;

#[derive(Debug, PartialEq)]
pub struct Row {
    hour_beginning: Zoned,
    ten_min_spin_req_mw: Decimal,
    total_ten_min_req_mw: Decimal,
    total_thirty_min_req_mw: Decimal,
    forecasted_energy_req_mw: Decimal,
    tmsr_clearing_price: Decimal,
    tmnsr_clearing_price: Decimal,
    tmor_clearing_price: Decimal,
    fer_clearing_price: Decimal,
    tmsr_designation_mw: Decimal,
    tmnsr_designation_mw: Decimal,
    tmor_designation_mw: Decimal,
    eir_designation_mw: Decimal,
}


#[derive(Clone)]
pub struct DaasReserveDataArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl DaasReserveDataArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/daas_reserve_data_"
            + &date.to_string()
            + ".json"
    }

    /// Read one json.gz file corresponding to one day.
    pub fn read_file(&self, path_gz: String) -> Result<Vec<Row>, Box<dyn Error>> {
        let mut file = GzDecoder::new(File::open(path_gz).unwrap());
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let doc: Value = serde_json::from_str(&buffer)?;
        let vs = doc["isone_web_services"]["day_ahead_reserves"]["day_ahead_reserve"].clone();
        let mut rows: Vec<Row> = Vec::new();
        match vs {
            Value::Array(values) => {
                for v in values {
                    let timestamp: Timestamp = match v["market_hour"]["local_day"].clone() {
                        Value::String(s) => s.parse()?,
                        _ => panic!("local_day field is no longer a string"),
                    };
                    let hour_beginning = timestamp.in_tz("America/New_York")?;
                    // println!("{}", hour_beginning);
                    // println!("10min: {}", &v["ten_min_spin_req_mw"].to_string());
                    // println!("10min: {}", Decimal::from_str(&format!("{}", v["ten_min_spin_req_mw"]))?);
                    let row = Row {
                        hour_beginning,
                        ten_min_spin_req_mw: Decimal::from_str(&format!(
                            "{}",
                            v["ten_min_spin_req_mw"]
                        ))?,
                        total_ten_min_req_mw: Decimal::from_str(&format!(
                            "{}",
                            v["total_ten_min_req_mw"]
                        ))?,
                        total_thirty_min_req_mw: Decimal::from_str(&format!(
                            "{}",
                            v["total_thirty_min_req_mw"]
                        ))?,
                        forecasted_energy_req_mw: Decimal::from_str(&format!(
                            "{}",
                            v["forecasted_energy_req_mw"]
                        ))?,
                        tmsr_clearing_price: Decimal::from_str(&format!(
                            "{}",
                            v["tmsr_clearing_price"]
                        ))?,
                        tmnsr_clearing_price: Decimal::from_str(&format!(
                            "{}",
                            v["tmnsr_clearing_price"]
                        ))?,
                        tmor_clearing_price: Decimal::from_str(&format!(
                            "{}",
                            v["tmor_clearing_price"]
                        ))?,
                        fer_clearing_price: Decimal::from_str(&format!(
                            "{}",
                            v["fer_clearing_price"]
                        ))?,
                        tmsr_designation_mw: Decimal::from_str(&format!(
                            "{}",
                            v["tmsr_designation_mw"]
                        ))?,
                        tmnsr_designation_mw: Decimal::from_str(&format!(
                            "{}",
                            v["tmnsr_designation_mw"]
                        ))?,
                        tmor_designation_mw: Decimal::from_str(&format!(
                            "{}",
                            v["tmor_designation_mw"]
                        ))?,
                        eir_designation_mw: Decimal::from_str(&format!(
                            "{}",
                            v["eir_designation_mw"]
                        ))?,
                    };
                    rows.push(row);
                }
            }
            _ => panic!("File format changed!"),
        };

        Ok(rows)
    }

    /// Data is published around 10:30 every day
    pub fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        super::lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/daasreservedata/day/{}",
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
            if day > last {
                continue;
            }
            info!("Working on {}", day);
            let fname = format!("{}.gz", self.filename(&day));
            if !Path::new(&fname).exists() {
                self.download_file(day)?;
                info!("  downloaded file for {}", day);
            }
        }
        Ok(())
    }

    pub fn setup(&self) -> Result<(), Box<dyn Error>> {
        info!("initializing {} archive ...", "daasreservedata");
        if fs::exists(&self.duckdb_path)? {
            fs::remove_file(&self.duckdb_path)?;
        }
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(r"
    BEGIN;
    CREATE TABLE IF NOT EXISTS reserve_data (
        hour_beginning TIMESTAMPTZ NOT NULL,
        ten_min_spin_req_mw DECIMAL(9,2) NOT NULL,
        total_ten_min_req_mw DECIMAL(9,2) NOT NULL,
        total_thirty_min_req_mw DECIMAL(9,2) NOT NULL,
        forecasted_energy_req_mw DECIMAL(9,2) NOT NULL,
        tmsr_clearing_price DECIMAL(9,2) NOT NULL,
        tmnsr_clearing_price DECIMAL(9,2) NOT NULL,
        tmor_clearing_price DECIMAL(9,2) NOT NULL,
        fer_clearing_price DECIMAL(9,2) NOT NULL,
        tmsr_designation_mw DECIMAL(9,2) NOT NULL,
        tmnsr_designation_mw DECIMAL(9,2) NOT NULL,
        tmor_designation_mw DECIMAL(9,2) NOT NULL,
        eir_designation_mw DECIMAL(9,2) NOT NULL,
    );
    CREATE INDEX idx ON reserve_data (hour_beginning);    
    COMMENT ON TABLE reserve_data IS 'Data is from ISONE webservices, end point: daasreservedata/day';
    COMMIT;
        ")?;
        Ok(())
    }

    /// Update duckdb with published data for the month.  No checks are made to see
    /// if there are missing files.
    ///  
    pub fn update_duckdb(&self, month: Month) -> Result<(), Box<dyn Error>> {
        let conn = Connection::open(self.duckdb_path.clone())?;

        info!("loading all json.gz files for month {} ...", month);
        let sql = format!(
            r"
        CREATE TEMPORARY TABLE tmp
        AS
            SELECT * 
            FROM (
                SELECT unnest(isone_web_services.day_ahead_reserves.day_ahead_reserve, recursive := true)
                FROM read_json('{}/Raw/{}/daas_reserve_data_{}-*.json.gz')
            )
            ORDER BY local_day;
        ",
            self.base_dir,
            month.start().year(),
            month
        );
        match conn.execute(&sql, params![]) {
            Ok(_) => info!("    created tmp table"),
            Err(e) => error!("{:?}", e),
        }

        let sql = r"
        INSERT INTO reserve_data
        SELECT 
            local_day::TIMESTAMPTZ as hour_beginning,
            ten_min_spin_req_mw::DECIMAL(9,2) as ten_min_spin_req_mw,
            total_ten_min_req_mw::DECIMAL(9,2) as total_ten_min_req_mw,
            total_thirty_min_req_mw::DECIMAL(9,2) as total_thirty_min_req_mw,
            forecasted_energy_req_mw::DECIMAL(9,2) as forecasted_energy_req_mw,
            tmsr_clearing_price::DECIMAL(9,2) as tmsr_clearing_price,
            tmnsr_clearing_price::DECIMAL(9,2) as tmnsr_clearing_price,
            tmor_clearing_price::DECIMAL(9,2) as tmor_clearing_price,
            fer_clearing_price::DECIMAL(9,2) as fer_clearing_price,
            tmsr_designation_mw::DECIMAL(9,2) as tmsr_designation_mw,
            tmnsr_designation_mw::DECIMAL(9,2) as tmnsr_designation_mw,
            tmor_designation_mw::DECIMAL(9,2) as tmor_designation_mw,
            eir_designation_mw::DECIMAL(9,2) as eir_designation_mw
        FROM tmp
        EXCEPT 
            SELECT * FROM reserve_data;            
        ";
        match conn.execute(sql, params![]) {
            Ok(n) => info!("    inserted {} rows into reserve_data table", n),
            Err(e) => error!("{:?}", e),
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
        let archive = ProdDb::isone_daas_reserve_data();
        // archive.setup()

        let month = month(2025, 3);
        archive.download_missing_days(month)?;
        archive.update_duckdb(month)?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn read_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_daas_reserve_data();
        let filename = archive.filename(&date(2025, 3, 1));
        let rows = archive.read_file(filename + ".gz")?;
        assert_eq!(rows.len(), 24);
        println!("{:?}", rows.first().unwrap());
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_daas_reserve_data();
        archive.download_file(date(2025, 3, 9))?;
        Ok(())
    }
}
