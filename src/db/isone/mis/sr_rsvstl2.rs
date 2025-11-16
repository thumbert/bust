use std::{error::Error, fs};

use crate::interval::month::month;

use duckdb::{params, Connection};
use jiff::{civil::Date, Timestamp, Zoned};
use log::{error, info};
use serde::{Deserialize, Serialize};

use super::lib_mis::*;



// Asset section
#[derive(Debug, Serialize, Deserialize)]
pub struct RowTab3 {
    pub account_id: usize,
    pub report_date: Date,
    pub version: Timestamp,
    pub hour_beginning: Zoned,
    pub asset_id: u32,
    pub asset_name: String,
    pub subaccount_id: Option<String>,
    pub subaccount_name: Option<String>,
    pub rt_tmsr_credit: f64,
    pub rt_tmnsr_credit: f64,
    pub rt_tmor_credit: f64,
    pub rt_reserve_credit: f64,
}


pub struct SrRsvstl2Report {
    pub info: MisReportInfo,
    pub lines: Vec<String>,
}

impl MisReport for SrRsvstl2Report {}

impl SrRsvstl2Report {
    pub fn process_tab3(&self) -> Result<Vec<RowTab3>, Box<dyn Error>> {
        let mut out: Vec<RowTab3> = Vec::new();
        let tab3 = extract_tab(3, &self.lines).unwrap();
        let data = tab3.lines.join("\n");
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        for result in rdr.records() {
            let record = result?;            
            let hour_beginning = parse_hour_ending(&self.info.report_date, &record[1]);
            let asset_id: u32 = record[2].parse()?;
            let asset_name: String = record[3].to_owned();
            let subaccount_id: Option<String> = record[4].parse().ok();
            let subaccount_name: Option<String> = record[5].parse().ok();
            let rt_tmsr_credit: f64 = record[6].parse()?;
            let rt_tmnsr_credit: f64 = record[7].parse()?;
            let rt_tmor_credit: f64 = record[8].parse()?;
            let rt_reserve_credit: f64 = record[9].parse()?;

            out.push(RowTab3 {
                account_id: self.info.account_id,
                report_date: self.info.report_date,
                version: self.info.version,
                subaccount_id,
                subaccount_name,
                hour_beginning,
                asset_id,
                asset_name,
                rt_tmsr_credit,
                rt_tmnsr_credit,
                rt_tmor_credit,
                rt_reserve_credit, 
            });
        }

        Ok(out)
    }

    pub fn export_csv(&self, archive: &SrRsvstl2Archive) -> Result<(), Box<dyn Error>> {
        // tab 3
        let mut wtr = csv::Writer::from_path(archive.filename(3, &self.info))?;
        let records = self.process_tab3().unwrap();
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;

        Ok(())
    }
}

#[derive(Clone)]
pub struct SrRsvstl2Archive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SrRsvstl2Archive {}

impl MisArchiveDuckDB for SrRsvstl2Archive {
    fn report_name(&self) -> String {
        "SR_RSVSTL2".to_string()
    }

    fn first_month(&self) -> crate::interval::month::Month {
        month(2025, 3)
    }

    /// Path to the monthly CSV file with the ISO report for a given tab
    fn filename(&self, tab: u8, info: &MisReportInfo) -> String {
        self.base_dir.to_owned() + "/tmp/" + &format!("tab{}_", tab) + &info.filename_iso()
    }


    fn setup(&self) -> Result<(), Box<dyn Error>> {
        info!("initializing {} archive ...", self.report_name());
        if fs::exists(&self.duckdb_path)? {
            fs::remove_file(&self.duckdb_path)?;
        }
        if !fs::exists(&self.base_dir)? {
            fs::create_dir_all(&self.base_dir)?;
            fs::create_dir_all(format!("{}/Raw", &self.base_dir))?;
            fs::create_dir_all(format!("{}/tmp", &self.base_dir))?;
        }
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
    BEGIN;
    CREATE TABLE IF NOT EXISTS tab3 (
        account_id UINTEGER NOT NULL,
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        hour_beginning TIMESTAMPTZ NOT NULL,
        asset_id UINTEGER NOT NULL,
        asset_name VARCHAR NOT NULL,
        subaccount_id VARCHAR,
        subaccount_name VARCHAR,
        rt_tmsr_credit DOUBLE NOT NULL,
        rt_tmnsr_credit DOUBLE NOT NULL,
        rt_tmor_credit DOUBLE NOT NULL,
        rt_reserve_credit DOUBLE NOT NULL,
    );
    CREATE INDEX idx ON tab3 (report_date);
    COMMIT;
    ",
        )?;

        conn.close().unwrap();
        Ok(())
    }

    fn update_duckdb(&self, files: Vec<String>) -> Result<(), Box<dyn Error>> {
        // get all reports in the db first
        let existing = self.get_reports_duckdb(3, &self.duckdb_path).unwrap();
        fs::remove_dir_all(format!("{}/tmp", &self.base_dir))?;
        fs::create_dir_all(format!("{}/tmp", &self.base_dir))?;

        for filename in files.iter() {
            let info = &MisReportInfo::from(filename.clone());
            if existing.contains(info) {
                continue;
            }
            let lines = read_report(filename.as_str()).unwrap();
            let report = SrRsvstl2Report {
                info: info.clone(),
                lines,
            };
            report.export_csv(self)?;
            info!("Wrote file {}", self.filename(3, info));
        }

        // list all the files and add them to the db, in order
        let mut paths: Vec<_> = fs::read_dir(self.base_dir.clone() + "/tmp")
            .unwrap()
            .map(|e| e.unwrap())
            .collect();
        paths.sort_by_key(|e| e.path());

        if paths.is_empty() {
            info!("No files to upload to DuckDB.  Exiting...");
            return Ok(());
        } else {
            info!("Inserting {} files into DuckDB...", paths.len());
        }

        let conn = Connection::open(&self.duckdb_path)?;
        let sql = format!(
            r"
            INSERT INTO tab3
            SELECT account_id, 
                report_date, 
                version, 
                strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
                asset_id,
                asset_name,
                subaccount_id,
                subaccount_name,
                rt_tmsr_credit,
                rt_tmnsr_credit,
                rt_tmor_credit,
                rt_reserve_credit,
            FROM read_csv(
                '{}/tmp/tab3_*.CSV', 
                header = true, 
                timestampformat = '%Y-%m-%dT%H:%M:%SZ'
            );
            ",
            self.base_dir,
        );
        match conn.execute(&sql, params![]) {
            Ok(n) => info!("  inserted {} rows into {} tab3 table", n, self.report_name()),
            Err(e) => error!("{:?}", e),
        }

        info!("Done\n");
        Ok(())
    }

}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use jiff::{civil::date, Zoned};

    use crate::db::{
        isone::mis::lib_mis::*,
        prod_db::ProdDb,
    };


    #[ignore]
    #[test]
    fn update_test() -> Result<(), Box<dyn Error>> {
        env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .init();

        let path = "/home/adrian/Documents/repos/git/thumbert/elec-server/test/_assets/sr_rsvstl2_000000002_2024111500_20250108194854.csv"
            .to_string();
        let archive = ProdDb::sr_rsvstl2();
        archive.setup()?;
        archive.update_duckdb(vec![path])?;
        Ok(())
    }


    #[test]
    fn months_test() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::sr_rsvstl2();
        let months = archive.get_months();
        if Zoned::now().date() > date(2025, 3, 1) {
            assert!(!months.is_empty())
        }
        Ok(())
    }

}
