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

use crate::interval::month::*;

use super::lib_mis::*;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
enum AssetType {
    #[serde(rename = "GENERATOR")]
    Generator,
    #[serde(rename = "ASSET RELATED DEMAND")]
    AssetRelatedDemand,
    #[serde(rename = "DEMAND RESPONSE RESOURCE")]
    DemandResponseResource,
}

impl FromStr for AssetType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GENERATOR" => Ok(AssetType::Generator),
            "ASSET RELATED DEMAND" => Ok(AssetType::AssetRelatedDemand),
            "DEMAND RESPONSE_RESOURCE" => Ok(AssetType::DemandResponseResource),
            _ => Err(format!("Failed to parse {s} as AssetType")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
enum ProductType {
    #[serde(rename = "DA_TMSR")]
    Tmsr,
    #[serde(rename = "DA_TMNSR")]
    Tmnsr,
    #[serde(rename = "DA_TMOR")]
    Tmor,
    #[serde(rename = "DA_EIR")]
    Eir,
}

impl FromStr for ProductType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DA_TMSR" => Ok(ProductType::Tmsr),
            "DA_TMNSR" => Ok(ProductType::Tmnsr),
            "DA_TMOR" => Ok(ProductType::Tmor),
            "DA_EIR" => Ok(ProductType::Eir),
            _ => Err(format!("Failed to parse {s} as ProductType")),
        }
    }
}

/// Asset FRS credits and closeout charges
#[derive(Debug, Serialize, Deserialize)]
struct RowTab0 {
    account_id: usize,
    report_date: Date,
    version: Timestamp,
    hour_beginning: Zoned,
    asset_id: u32,
    asset_name: String,
    subaccount_id: u32,
    subaccount_name: String,
    asset_type: AssetType,
    ownership_share: f32,
    product_type: Option<ProductType>,
    product_obligation: Option<f64>,
    product_clearing_price: Option<f64>,
    product_credit: Option<f64>,
    customer_share_of_product_credit: Option<f64>,
    strike_price: Option<f64>,
    hub_rt_lmp: Option<f64>,
    product_closeout_charge: Option<f64>,
    customer_share_of_product_closeout_charge: Option<f64>,
}

/// Asset FER credits
#[derive(Debug, Serialize, Deserialize)]
struct RowTab1 {
    account_id: usize,
    report_date: Date,
    version: Timestamp,
    hour_beginning: Zoned,
    asset_id: u32,
    asset_name: String,
    subaccount_id: u32,
    subaccount_name: String,
    asset_type: AssetType,
    ownership_share: f32,
    da_cleared_energy: f64,
    fer_price: f64,
    asset_fer_credit: f64,
    customer_share_of_asset_fer_credit: f64,
}

/// Subaccount FRS Credits & Charges Section
#[derive(Debug, Serialize, Deserialize)]
struct RowTab6 {
    account_id: usize,
    report_date: Date,
    version: Timestamp,
    subaccount_id: u32,
    subaccount_name: String,
    hour_beginning: Zoned,
    rt_load_obligation: f64,
    rt_external_node_load_obligation: f64,
    rt_dard_load_obligation_reduction: f64,
    rt_load_obligation_for_frs_charge_allocation: f64,
    pool_rt_load_obligation_for_frs_charge_allocation: f64,
    pool_da_tmsr_credit: f64,
    da_tmsr_charge: f64,
    pool_da_tmnsr_credit: f64,
    da_tmnsr_charge: f64,
    pool_da_tmor_credit: f64,
    da_tmor_charge: f64,
    pool_da_tmsr_closeout_charge: f64,
    da_tmsr_closeout_credit: f64,
    pool_da_tmnsr_closeout_charge: f64,
    da_tmnsr_closeout_credit: f64,
    pool_da_tmor_closeout_charge: f64,
    da_tmor_closeout_credit: f64,
}

/// Subaccount DA EIR Credits & Charges Section
#[derive(Debug, Serialize, Deserialize)]
struct RowTab7 {
    account_id: usize,
    report_date: Date,
    version: Timestamp,
    subaccount_id: u32,
    subaccount_name: String,
    hour_beginning: Zoned,
    rt_load_obligation: f64,
    rt_external_node_load_obligation: f64,
    rt_dard_load_obligation_reduction: f64,
    rt_load_obligation_for_da_eir_charge_allocation: f64,
    pool_rt_load_obligation_for_da_eir_charge_allocation: f64,
    pool_da_eir_credit: f64,
    pool_fer_credit: f64,
    pool_export_fer_charge: f64,
    pool_fer_and_da_eir_net_credits: f64,
    fer_and_da_eir_charge: f64,
    pool_da_eir_closeout_charge: f64,
    da_eir_closeout_credit: f64,
}

pub struct SdDaasdtReport {
    info: MisReportInfo,
    lines: Vec<String>,
}

impl MisReport for SdDaasdtReport {}

impl SdDaasdtReport {
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
            let asset_id: u32 = record[2].parse()?;
            let asset_name: String = record[3].to_owned();
            let subaccount_id: u32 = record[4].parse()?;
            let subaccount_name: String = record[5].to_owned();
            let asset_type: AssetType = record[6].parse()?;
            let ownership_share: f32 = record[7].parse()?;
            let product_type: Option<ProductType> = record[8].parse().ok();
            let product_obligation: Option<f64> = record[9].parse().ok();
            let product_clearing_price: Option<f64> = record[10].parse().ok();
            let product_credit: Option<f64> = record[11].parse().ok();
            let customer_share_of_product_credit: Option<f64> = record[12].parse().ok();
            let strike_price: Option<f64> = record[13].parse().ok();
            let hub_rt_lmp: Option<f64> = record[14].parse().ok();
            let product_closeout_charge: Option<f64> = record[15].parse().ok();
            let customer_share_of_product_closeout_charge: Option<f64> = record[16].parse().ok();

            out.push(RowTab0 {
                account_id: self.info.account_id,
                report_date: self.info.report_date,
                version: self.info.version,
                hour_beginning,
                asset_id,
                asset_name,
                subaccount_id,
                subaccount_name,
                asset_type,
                ownership_share,
                product_type,
                product_obligation,
                product_clearing_price,
                product_credit,
                customer_share_of_product_credit,
                strike_price,
                hub_rt_lmp,
                product_closeout_charge,
                customer_share_of_product_closeout_charge,
            });
        }

        Ok(out)
    }

    fn process_tab1(&self) -> Result<Vec<RowTab1>, Box<dyn Error>> {
        let mut out: Vec<RowTab1> = Vec::new();
        let tab1 = extract_tab(1, &self.lines).unwrap();
        let data = tab1.lines.join("\n");
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        for result in rdr.records() {
            let record = result?;

            let hour_beginning = parse_hour_ending(&self.info.report_date, &record[1]);
            let asset_id: u32 = record[2].parse()?;
            let asset_name: String = record[3].to_owned();
            let subaccount_id: u32 = record[4].parse()?;
            let subaccount_name: String = record[5].to_owned();
            let asset_type: AssetType = record[6].parse()?;
            let ownership_share: f32 = record[7].parse()?;
            let da_cleared_energy: f64 = record[8].parse()?;
            let fer_price: f64 = record[9].parse()?;
            let asset_fer_credit: f64 = record[10].parse()?;
            let customer_share_of_asset_fer_credit: f64 = record[11].parse()?;

            out.push(RowTab1 {
                account_id: self.info.account_id,
                report_date: self.info.report_date,
                version: self.info.version,
                hour_beginning,
                asset_id,
                asset_name,
                subaccount_id,
                subaccount_name,
                asset_type,
                ownership_share,
                da_cleared_energy,
                fer_price,
                asset_fer_credit,
                customer_share_of_asset_fer_credit,
            });
        }

        Ok(out)
    }

    fn process_tab6(&self) -> Result<Vec<RowTab6>, Box<dyn Error>> {
        let mut out: Vec<RowTab6> = Vec::new();
        let tab6 = extract_tab(6, &self.lines).unwrap();
        let data = tab6.lines.join("\n");
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        for result in rdr.records() {
            let record = result?;

            let subaccount_id: u32 = record[1].parse()?;
            let subaccount_name: String = record[2].to_owned();
            let hour_beginning = parse_hour_ending(&self.info.report_date, &record[3]);
            let rt_load_obligation: f64 = record[4].parse()?;
            let rt_external_node_load_obligation: f64 = record[5].parse()?;
            let rt_dard_load_obligation_reduction: f64 = record[6].parse()?;
            let rt_load_obligation_for_frs_charge_allocation: f64 = record[7].parse()?;
            let pool_rt_load_obligation_for_frs_charge_allocation: f64 = record[8].parse()?;
            let pool_da_tmsr_credit: f64 = record[9].parse()?;
            let da_tmsr_charge: f64 = record[10].parse()?;
            let pool_da_tmnsr_credit: f64 = record[11].parse()?;
            let da_tmnsr_charge: f64 = record[12].parse()?;
            let pool_da_tmor_credit: f64 = record[13].parse()?;
            let da_tmor_charge: f64 = record[14].parse()?;
            let pool_da_tmsr_closeout_charge: f64 = record[15].parse()?;
            let da_tmsr_closeout_credit: f64 = record[16].parse()?;
            let pool_da_tmnsr_closeout_charge: f64 = record[17].parse()?;
            let da_tmnsr_closeout_credit: f64 = record[18].parse()?;
            let pool_da_tmor_closeout_charge: f64 = record[19].parse()?;
            let da_tmor_closeout_credit: f64 = record[20].parse()?;

            out.push(RowTab6 {
                account_id: self.info.account_id,
                report_date: self.info.report_date,
                version: self.info.version,
                subaccount_id,
                subaccount_name,
                hour_beginning,
                rt_load_obligation,
                rt_external_node_load_obligation,
                rt_dard_load_obligation_reduction,
                rt_load_obligation_for_frs_charge_allocation,
                pool_rt_load_obligation_for_frs_charge_allocation,
                pool_da_tmsr_credit,
                da_tmsr_charge,
                pool_da_tmnsr_credit,
                da_tmnsr_charge,
                pool_da_tmor_credit,
                da_tmor_charge,
                pool_da_tmsr_closeout_charge,
                da_tmsr_closeout_credit,
                pool_da_tmnsr_closeout_charge,
                da_tmnsr_closeout_credit,
                pool_da_tmor_closeout_charge,
                da_tmor_closeout_credit,
            });
        }

        Ok(out)
    }

    fn process_tab7(&self) -> Result<Vec<RowTab7>, Box<dyn Error>> {
        let mut out: Vec<RowTab7> = Vec::new();
        let tab6 = extract_tab(7, &self.lines).unwrap();
        let data = tab6.lines.join("\n");
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        for result in rdr.records() {
            let record = result?;

            let subaccount_id: u32 = record[1].parse()?;
            let subaccount_name: String = record[2].to_owned();
            let hour_beginning = parse_hour_ending(&self.info.report_date, &record[3]);
            let rt_load_obligation: f64 = record[4].parse()?;
            let rt_external_node_load_obligation: f64 = record[5].parse()?;
            let rt_dard_load_obligation_reduction: f64 = record[6].parse()?;
            let rt_load_obligation_for_da_eir_charge_allocation: f64 = record[7].parse()?;
            let pool_rt_load_obligation_for_da_eir_charge_allocation: f64 = record[8].parse()?;
            let pool_da_eir_credit: f64 = record[9].parse()?;
            let pool_fer_credit: f64 = record[10].parse()?;
            let pool_export_fer_charge: f64 = record[11].parse()?;
            let pool_fer_and_da_eir_net_credits: f64 = record[12].parse()?;
            let fer_and_da_eir_charge: f64 = record[13].parse()?;
            let pool_da_eir_closeout_charge: f64 = record[14].parse()?;
            let da_eir_closeout_credit: f64 = record[15].parse()?;

            out.push(RowTab7 {
                account_id: self.info.account_id,
                report_date: self.info.report_date,
                version: self.info.version,
                subaccount_id,
                subaccount_name,
                hour_beginning,
                rt_load_obligation,
                rt_external_node_load_obligation,
                rt_dard_load_obligation_reduction,
                rt_load_obligation_for_da_eir_charge_allocation,
                pool_rt_load_obligation_for_da_eir_charge_allocation,
                pool_da_eir_credit,
                pool_fer_credit,
                pool_export_fer_charge,
                pool_fer_and_da_eir_net_credits,
                fer_and_da_eir_charge,
                pool_da_eir_closeout_charge,
                da_eir_closeout_credit,
            });
        }

        Ok(out)
    }

    fn export_csv(&self, archive: &SdDaasdtArchive) -> Result<(), Box<dyn Error>> {
        // tab 0
        let mut wtr = csv::Writer::from_path(archive.filename(0, &self.info))?;
        let records = self.process_tab0().unwrap();
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;

        // tab 1
        let mut wtr = csv::Writer::from_path(archive.filename(1, &self.info))?;
        let records = self.process_tab1().unwrap();
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;

        // tab 6
        let mut wtr = csv::Writer::from_path(archive.filename(6, &self.info))?;
        let records = self.process_tab6().unwrap();
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;

        // tab 7
        let mut wtr = csv::Writer::from_path(archive.filename(7, &self.info))?;
        let records = self.process_tab7().unwrap();
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;

        Ok(())
    }
}

pub struct SdDaasdtArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SdDaasdtArchive {}

impl MisArchiveDuckDB for SdDaasdtArchive {
    fn report_name(&self) -> String {
        "SD_DAASDT".to_string()
    }

    fn first_month(&self) -> crate::interval::month::Month {
        month(2025, 3)
    }


    /// Which months to archive.  Default implementation.
    fn get_months(&self) -> Vec<Month> {
        // let months = MisArchiveDuckDB::get_months(self);
        MisArchiveDuckDB::get_months(self)
            .into_iter()
            .filter(|e| e.is_after(self.first_month()).unwrap() || *e == self.first_month())
            .collect()
        // months
    }

    /// Path to the monthly CSV file with the ISO report for a given tab
    fn filename(&self, tab: u8, info: &MisReportInfo) -> String {
        self.base_dir.to_owned() + "/tmp/" + &format!("tab{}_", tab) + &info.filename_iso()
    }

    fn get_reports_duckdb(&self) -> Result<HashSet<MisReportInfo>, Box<dyn Error>> {
        let conn = Connection::open(&self.duckdb_path)?;
        let query = r#"
        SELECT DISTINCT account_id, report_date, version
        FROM tab0;
        "#;
        let mut stmt = conn.prepare(query).unwrap();
        let res_iter = stmt.query_map([], |row| {
            let n = 719528 + row.get::<usize, i32>(1).unwrap();
            let microseconds: i64 = row.get(2).unwrap();
            Ok(MisReportInfo {
                report_name: self.report_name(),
                account_id: row.get::<usize, usize>(0).unwrap(),
                report_date: Date::ZERO.checked_add(n.days()).unwrap(),
                version: Timestamp::from_microsecond(microseconds).unwrap(),
            })
        })?;
        let res: HashSet<MisReportInfo> = res_iter.map(|e| e.unwrap()).collect();

        Ok(res)
    }

    fn setup_duckdb(&self) -> Result<(), Box<dyn Error>> {
        info!("initializing {} archive ...", self.report_name());
        if fs::exists(&self.duckdb_path)? {
            fs::remove_file(&self.duckdb_path)?;
        }
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
        let existing = self.get_reports_duckdb().unwrap();
        fs::remove_dir_all(format!("{}/tmp", &self.base_dir))?;
        fs::create_dir_all(format!("{}/tmp", &self.base_dir))?;

        for filename in files.iter() {
            let info = &MisReportInfo::from(filename.clone());
            if existing.contains(info) {
                continue;
            }
            let lines = read_report(filename.as_str()).unwrap();
            let report = SdDaasdtReport {
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
            info!("Inserting {} files into DuckDB.", paths.len());
        }

        let conn = Connection::open(&self.duckdb_path)?;
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
            Ok(n) => info!("  inserted {} rows in {} tab0 table", n, self.report_name()),
            Err(e) => error!("{:?}", e),
        }

        info!("done\n");

        Ok(())
    }
    
}

#[cfg(test)]
mod tests {
    use std::{error::Error, str::FromStr};

    use crate::db::{isone::mis::{
        lib_mis::*,
        sd_daasdt::{AssetType, SdDaasdtReport},
    }, prod_db::ProdDb};

    #[test]
    fn months_test() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::sd_daasdt();
        println!("{:?}", archive.get_months());


        Ok(())
    }


    #[test]
    fn read_tab0_test() -> Result<(), Box<dyn Error>> {
        let path = "../elec-server/test/_assets/sd_daasdt_000000002_2024111500_20241203135151.csv";
        let info = MisReportInfo::from(path.to_string());
        let lines = read_report(path).unwrap();
        assert_eq!(lines.len(), 198);

        let report = SdDaasdtReport { info, lines };
        let rows = report.process_tab0()?;
        println!("{:?}", rows);

        Ok(())
    }

    #[test]
    fn parse_enums_test() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            AssetType::from_str("GENERATOR").unwrap(),
            AssetType::Generator
        );
        Ok(())
    }
}
