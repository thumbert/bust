use std::{
    collections::HashSet,
    error::Error,
    fs::{self},
    str::FromStr,
};

use duckdb::{params, Connection};
use jiff::{civil::Date, Timestamp, ToSpan, Zoned};
use log::{error, info};
use serde::{Deserialize, Serialize};

use crate::db::prod_db::ProdDb;

use super::lib_mis::*;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
enum AssetSubType {
    #[serde(rename = "LOSSES")]
    Losses,
    #[serde(rename = "NORMAL")]
    Normal,
    #[serde(rename = "STATION SERVICE")]
    StationService,
    #[serde(rename = "ENERGY STORAGE")]
    EnergyStorage,
    #[serde(rename = "PUMP STORAGE")]
    PumpStorage,
}

impl FromStr for AssetSubType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "LOSSES" => Ok(AssetSubType::Losses),
            "NORMAL" => Ok(AssetSubType::Normal),
            "STATION SERVICE" => Ok(AssetSubType::StationService),
            "ENERGY STORAGE" => Ok(AssetSubType::EnergyStorage),
            "PUMP STORAGE" => Ok(AssetSubType::PumpStorage),
            _ => Err(format!("Failed to parse {s} as AssetSubType")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
enum LocationType {
    #[serde(rename = "METERING DOMAIN")]
    MeteringDomain,
    #[serde(rename = "NETWORK NODE")]
    NetworkNode,
}

impl FromStr for LocationType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "METERING DOMAIN" => Ok(LocationType::MeteringDomain),
            "NETWORK NODE" => Ok(LocationType::NetworkNode),
            _ => Err(format!("Failed to parse {s} as LocationType")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RowTab0 {
    account_id: usize,
    report_date: Date,
    version: Timestamp,
    hour_beginning: Zoned,
    asset_name: String,
    asset_id: u32,
    asset_subtype: AssetSubType,
    location_id: u32,
    location_name: String,
    location_type: LocationType,
    load_reading: f64,
    ownership_share: f32,
    share_of_load_reading: f64,
    subaccount_id: Option<u32>,
    subaccount_name: Option<String>,
}

pub struct SdRtloadReport {
    info: MisReportInfo,
    lines: Vec<String>,
}

impl MisReport for SdRtloadReport {}

impl SdRtloadReport {
    fn process_tab0(&self) -> Result<Vec<RowTab0>, Box<dyn Error>> {
        let mut out: Vec<RowTab0> = Vec::new();
        let tab0 = extract_tab(0, &self.lines).unwrap();
        let data = tab0.lines.join("\n");
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        for result in rdr.records() {
            let record = result?;

            let hour_beginning = parse_hour_ending(&self.info.report_date, &record[1]);
            let asset_name: String = record[2].to_owned();
            let asset_id: u32 = record[3].parse()?;
            let asset_subtype: AssetSubType = record[4].parse()?;
            let location_id: u32 = record[5].parse()?;
            let location_name: String = record[6].to_owned();
            let location_type: LocationType = record[7].parse()?;
            let load_reading: f64 = record[8].parse()?;
            let ownership_share: f32 = record[9].parse()?;
            let share_of_load_reading: f64 = record[10].parse()?;
            let subaccount_id: Option<u32> = record[11].parse().ok();
            let subaccount_name: Option<String> = record[12].parse().ok();

            out.push(RowTab0 {
                account_id: self.info.account_id,
                report_date: self.info.report_date,
                version: self.info.version,
                hour_beginning,
                asset_name,
                asset_id,
                asset_subtype,
                location_id,
                location_name,
                location_type,
                load_reading,
                ownership_share,
                share_of_load_reading,
                subaccount_id,
                subaccount_name,
            });
        }

        Ok(out)
    }

    fn export_csv(&self, archive: &SdRtloadArchive) -> Result<(), Box<dyn Error>> {
        // tab 0
        let mut wtr = csv::Writer::from_path(archive.filename(0, &self.info))?;
        let records = self.process_tab0().unwrap();
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;

        Ok(())
    }
}

pub struct SdRtloadArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SdRtloadArchive {
    
}

impl MisArchiveDuckDB for SdRtloadArchive {
    fn filename(&self, tab: u8, info: &MisReportInfo) -> String {
        self.base_dir.to_owned() + "/tmp/" + &format!("tab{}_", tab) + &info.filename_iso()
    }

    fn get_reports_duckdb() -> Result<HashSet<MisReportInfo>, Box<dyn Error>> {
        let archive = ProdDb::sd_rtload();
        let conn = Connection::open(archive.duckdb_path)?;
        let query = r#"
        SELECT DISTINCT account_id, report_date, version
        FROM tab0;
        "#;
        let mut stmt = conn.prepare(query).unwrap();
        let res_iter = stmt.query_map([], |row| {
            let n = 719528 + row.get::<usize, i32>(1).unwrap();
            let microseconds: i64 = row.get(2).unwrap();
            Ok(MisReportInfo {
                report_name: "SD_RTLOAD".to_string(),
                account_id: row.get::<usize, usize>(0).unwrap(),
                report_date: Date::ZERO.checked_add(n.days()).unwrap(),
                version: Timestamp::from_microsecond(microseconds).unwrap(),
            })
        })?;
        let res: HashSet<MisReportInfo> = res_iter.map(|e| e.unwrap()).collect();

        Ok(res)
    }


    fn setup_duckdb(&self) -> Result<(), Box<dyn Error>> {
        info!("initializing SD_RTLOAD archive ...");
        fs::remove_file(&self.duckdb_path)?;
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
    BEGIN;
    CREATE TABLE IF NOT EXISTS tab0 (
        account_id UINTEGER NOT NULL,
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        hour_beginning TIMESTAMPTZ NOT NULL,
        asset_name VARCHAR NOT NULL,
        asset_id UINTEGER NOT NULL,
        asset_subtype ENUM ('LOSSES', 'NORMAL', 'STATION SERVICE', 'ENERGY STORAGE', 'PUMP STORAGE'),
        location_id UINTEGER NOT NULL,
        location_name VARCHAR NOT NULL,
        location_type ENUM ('METERING DOMAIN', 'NETWORK NODE'),
        load_reading DOUBLE NOT NULL,
        ownership_share FLOAT NOT NULL,
        share_of_load_reading DOUBLE NOT NULL,
        subaccount_id UINTEGER,
        subaccount_name VARCHAR,
    );
    CREATE INDEX idx ON tab0 (report_date);
    COMMIT;
    ",
        )?;

        conn.close().unwrap();
        Ok(())
    }


    fn update_duckdb(&self, files: Vec<String>) -> Result<(), Box<dyn Error>> {
        // get all reports in the db first
        let existing = SdRtloadArchive::get_reports_duckdb().unwrap();
        fs::remove_dir_all(format!("{}/tmp", &self.base_dir))?;
        fs::create_dir_all(format!("{}/tmp", &self.base_dir))?;

        for filename in files.iter() {
            let info = &MisReportInfo::from(filename.clone());
            if existing.contains(info) {
                continue;
            }
            let lines = read_report(filename.as_str()).unwrap();
            let report = SdRtloadReport {
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
            info!("Inserting {} files into DucDB.", paths.len());
        }

        let archive = ProdDb::sd_rtload();
        let conn = Connection::open(archive.duckdb_path)?;
        let sql = format!(
            r"
            INSERT INTO tab0 
            SELECT account_id, report_date, version, 
                strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
                asset_name,
                asset_id,
                asset_subtype,
                location_id,
                location_name,
                location_type,
                load_reading,
                ownership_share,
                share_of_load_reading,
                subaccount_id,
                subaccount_name
            FROM read_csv(
                '{}/tmp/tab0_*.CSV', 
                header = true, 
                timestampformat = '%Y-%m-%dT%H:%M:%SZ'
            );
            ",
            self.base_dir,
        );
        match conn.execute(&sql, params![]) {
            Ok(n) => info!("  inserted {} rows in SD_RTLOAD tab0 table", n),
            Err(e) => error!("{:?}", e),
        }

        info!("done\n");

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, str::FromStr};

    use crate::db::{
        isone::mis::{lib_mis::*, sd_rtload::*},
        prod_db::ProdDb,
    };

    #[test]
    fn update_tab0_test() -> Result<(), Box<dyn Error>> {
        let path = "../elec-server/test/_assets/sd_rtload_000000003_2013060100_20140228135257.csv";
        let info = MisReportInfo::from(path.to_string());
        let lines = read_report(path).unwrap();
        // println!("{}", lines.len());
        assert_eq!(lines.len(), 127);

        let report = SdRtloadReport { info, lines };
        // let rows = report.process_tab0()?;
        // println!("{:?}", rows);

        let archive = ProdDb::sd_rtload();
        report.export_csv(&archive)?;

        Ok(())
    }

    #[test]
    fn parse_enums_test() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            AssetSubType::from_str("LOSSES").unwrap(),
            AssetSubType::Losses
        );
        Ok(())
    }
}
