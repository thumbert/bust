use std::{error::Error, fs};

use crate::interval::month::{month, Month};

use duckdb::{params, Connection};
use jiff::{civil::Date, Timestamp, Zoned};
use log::{error, info};
use serde::{Deserialize, Serialize};

use super::lib_mis::*;


// // Reserve zone section
// #[derive(Debug, Serialize, Deserialize)]
// pub struct RowTab0 {
//     pub trading_interval: Zoned,
//     pub product_type: String,
//     pub reserve_zone_id: u32,
//     pub reserve_zone_name: String,
//     pub rt_reserve_credit: f64,
// }

// // Load zone section
// #[derive(Debug, Serialize, Deserialize)]
// pub struct RowTab1 {
//     pub account_id: usize,
//     pub report_date: Date,
//     pub version: Timestamp,
//     pub hour_beginning: Zoned,
//     pub load_zone_id: u32,
//     pub load_zone_name: String,
//     pub load_zone_rt_load_obligation: f64,
//     pub load_zone_ard_reserve_designation: f64,
//     pub external_sale_load_obligation_mw: f64,
//     pub total_load_zone_reserve_charge_allocation_mw: f64,
//     pub total_load_zone_rt_reserve_charge: f64,
// }

// Subaccount section
#[derive(Debug, Serialize, Deserialize)]
pub struct RowTab5 {
    pub account_id: usize,
    pub report_date: Date,
    pub version: Timestamp,
    pub subaccount_id: u32,
    pub subaccount_name: String,
    pub hour_beginning: Zoned,
    pub load_zone_id: u32,
    pub load_zone_name: String,
    pub rt_load_obligation: f64,
    pub ard_reserve_designation: f64,
    pub external_sale_load_obligation_mw: f64,
    pub reserve_charge_allocation_mw: f64,
    pub total_rt_reserve_charge: f64,
}


pub struct SrRsvcharge2Report {
    pub info: MisReportInfo,
    pub lines: Vec<String>,
}

impl MisReport for SrRsvcharge2Report {}

impl SrRsvcharge2Report {
    fn process_tab5(&self) -> Result<Vec<RowTab5>, Box<dyn Error>> {
        let mut out: Vec<RowTab5> = Vec::new();
        let tab5 = extract_tab(5, &self.lines).unwrap();
        let data = tab5.lines.join("\n");
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        for result in rdr.records() {
            let record = result?;            
            let subaccount_id: u32 = record[1].parse()?;
            let subaccount_name: String = record[2].to_owned();
            let hour_beginning = parse_hour_ending(&self.info.report_date, &record[3]);
            let load_zone_id: u32 = record[4].parse()?;
            let load_zone_name: String = record[5].to_owned();
            let rt_load_obligation: f64 = record[6].parse()?;
            let ard_reserve_designation: f64 = record[7].parse()?;
            let external_sale_load_obligation_mw: f64 = record[8].parse()?;
            let reserve_charge_allocation_mw: f64 = record[9].parse()?;
            let total_rt_reserve_charge: f64 = record[10].parse()?;

            out.push(RowTab5 {
                account_id: self.info.account_id,
                report_date: self.info.report_date,
                version: self.info.version,
                subaccount_id,
                subaccount_name,
                hour_beginning,
                load_zone_id,
                load_zone_name,
                rt_load_obligation,
                ard_reserve_designation,
                external_sale_load_obligation_mw,
                reserve_charge_allocation_mw,
                total_rt_reserve_charge,
                
            });
        }

        Ok(out)
    }

    fn export_csv(&self, archive: &SrRsvcharge2Archive) -> Result<(), Box<dyn Error>> {
        // tab 5
        let mut wtr = csv::Writer::from_path(archive.filename(5, &self.info))?;
        let records = self.process_tab5().unwrap();
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;

        Ok(())
    }
}

pub struct SrRsvcharge2Archive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SrRsvcharge2Archive {
    /// Which months to archive.  Default implementation.
    fn get_months(&self) -> Vec<Month> {
        MisArchiveDuckDB::get_months(self)
            .into_iter()
            .filter(|e| e >= &self.first_month())
            .collect()
    }
}

impl MisArchiveDuckDB for SrRsvcharge2Archive {
    fn report_name(&self) -> String {
        "SR_RSVCHARGE2".to_string()
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
    CREATE TABLE IF NOT EXISTS tab5 (
        account_id UINTEGER NOT NULL,
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        subaccount_id UINTEGER,
        subaccount_name VARCHAR,
        hour_beginning TIMESTAMPTZ NOT NULL,
        load_zone_id UINTEGER NOT NULL,
        load_zone_name VARCHAR NOT NULL,
        rt_load_obligation DOUBLE,
        ard_reserve_designation DOUBLE,
        external_sale_load_obligation_mw DOUBLE,
        reserve_charge_allocation_mw DOUBLE,
        total_rt_reserve_charge DOUBLE,
    );
    CREATE INDEX idx ON tab5 (report_date);
    COMMIT;
    ",
        )?;

        conn.close().unwrap();
        Ok(())
    }

    fn update_duckdb(&self, files: Vec<String>) -> Result<(), Box<dyn Error>> {
        // get all reports in the db first
        let existing = self.get_reports_duckdb(5, &self.duckdb_path).unwrap();
        fs::remove_dir_all(format!("{}/tmp", &self.base_dir))?;
        fs::create_dir_all(format!("{}/tmp", &self.base_dir))?;

        for filename in files.iter() {
            let info = &MisReportInfo::from(filename.clone());
            if existing.contains(info) {
                continue;
            }
            let lines = read_report(filename.as_str()).unwrap();
            let report = SrRsvcharge2Report {
                info: info.clone(),
                lines,
            };
            report.export_csv(self)?;
            info!("Wrote file {}", self.filename(0, info));
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
            INSERT INTO tab5
            SELECT account_id, 
                report_date, 
                version, 
                subaccount_id,
                subaccount_name,
                strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
                load_zone_id,
                load_zone_name,
                rt_load_obligation,
                ard_reserve_designation,
                external_sale_load_obligation_mw,
                reserve_charge_allocation_mw,
                total_rt_reserve_charge,
            FROM read_csv(
                '{}/tmp/tab5_*.CSV', 
                header = true, 
                timestampformat = '%Y-%m-%dT%H:%M:%SZ'
            );
            ",
            self.base_dir,
        );
        match conn.execute(&sql, params![]) {
            Ok(n) => info!("  inserted {} rows into {} tab5 table", n, self.report_name()),
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

        let path = "../elec-server/test/_assets/sr_rsvcharge2_000000002_2024111500_20250108194854.csv"
            .to_string();
        let archive = ProdDb::sr_rsvcharge2();
        archive.setup()?;
        archive.update_duckdb(vec![path])?;
        Ok(())
    }


    #[test]
    fn months_test() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::sr_rsvcharge2();
        let months = archive.get_months();
        if Zoned::now().date() > date(2025, 3, 1) {
            assert!(months.is_empty())
        }
        Ok(())
    }

}
