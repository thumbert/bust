use std::{
    error::Error,
    fs::{self},
    str::FromStr,
};

use duckdb::{params, Connection};
use jiff::{civil::Date, Timestamp, Zoned};
use log::{error, info};
use serde::{Deserialize, Serialize};

use crate::interval::month::*;

use super::lib_mis::*;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
pub enum AssetType {
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
pub enum ProductType {
    #[serde(rename = "DA TMSR")]
    Tmsr,
    #[serde(rename = "DA TMNSR")]
    Tmnsr, 
    #[serde(rename = "DA TMOR")]
    Tmor,
    #[serde(rename = "DA EIR")]
    Eir,
}

impl FromStr for ProductType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DA TMSR" => Ok(ProductType::Tmsr),
            "DA TMNSR" => Ok(ProductType::Tmnsr),
            "DA TMOR" => Ok(ProductType::Tmor),
            "DA EIR" => Ok(ProductType::Eir),
            _ => Err(format!("Failed to parse {s} as ProductType")),
        }
    }
}

/// Asset FRS credits and closeout charges
#[derive(Debug, Serialize, Deserialize)]
pub struct RowTab0 {
    pub account_id: usize,
    pub report_date: Date,
    pub version: Timestamp,
    pub hour_beginning: Zoned,
    pub asset_id: u32,
    pub asset_name: String,
    pub subaccount_id: u32,
    pub subaccount_name: String,
    pub asset_type: AssetType,
    pub ownership_share: f32,
    pub product_type: ProductType,
    pub product_obligation: f64,
    pub product_clearing_price: f64,
    pub product_credit: f64,
    pub customer_share_of_product_credit: f64,
    pub strike_price: f64,
    pub hub_rt_lmp: f64,
    pub product_closeout_charge: f64,
    pub customer_share_of_product_closeout_charge: f64,
}

/// Asset FER credits
#[derive(Debug, Serialize, Deserialize)]
pub struct RowTab1 {
    pub account_id: usize,
    pub report_date: Date,
    pub version: Timestamp,
    pub hour_beginning: Zoned,
    pub asset_id: u32,
    pub asset_name: String,
    pub subaccount_id: u32,
    pub subaccount_name: String,
    pub asset_type: AssetType,
    pub ownership_share: f32,
    pub da_cleared_energy: f64,
    pub fer_price: f64,
    pub asset_fer_credit: f64,
    pub customer_share_of_asset_fer_credit: f64,
}

/// Subaccount FRS Credits & Charges Section
#[derive(Debug, Serialize, Deserialize)]
pub struct RowTab6 {
    pub account_id: usize,
    pub report_date: Date,
    pub version: Timestamp,
    pub subaccount_id: u32,
    pub subaccount_name: String,
    pub hour_beginning: Zoned,
    pub rt_load_obligation: f64,
    pub rt_external_node_load_obligation: f64,
    pub rt_dard_load_obligation_reduction: f64,
    pub rt_load_obligation_for_frs_charge_allocation: f64,
    pub pool_rt_load_obligation_for_frs_charge_allocation: f64,
    pub pool_da_tmsr_credit: f64,
    pub da_tmsr_charge: f64,
    pub pool_da_tmnsr_credit: f64,
    pub da_tmnsr_charge: f64,
    pub pool_da_tmor_credit: f64,
    pub da_tmor_charge: f64,
    pub pool_da_tmsr_closeout_charge: f64,
    pub da_tmsr_closeout_credit: f64,
    pub pool_da_tmnsr_closeout_charge: f64,
    pub da_tmnsr_closeout_credit: f64,
    pub pool_da_tmor_closeout_charge: f64,
    pub da_tmor_closeout_credit: f64,
}

/// Subaccount DA EIR Credits & Charges Section
#[derive(Debug, Serialize, Deserialize)]
pub struct RowTab7 {
    pub account_id: usize,
    pub report_date: Date,
    pub version: Timestamp,
    pub subaccount_id: u32,
    pub subaccount_name: String,
    pub hour_beginning: Zoned,
    pub rt_load_obligation: f64,
    pub rt_external_node_load_obligation: f64,
    pub rt_dard_load_obligation_reduction: f64,
    pub rt_load_obligation_for_da_eir_charge_allocation: f64,
    pub pool_rt_load_obligation_for_da_eir_charge_allocation: f64,
    pub pool_da_eir_credit: f64,
    pub pool_fer_credit: f64,
    pub pool_export_fer_charge: f64,
    pub pool_fer_and_da_eir_net_credits: f64,
    pub fer_and_da_eir_charge: f64,
    pub pool_da_eir_closeout_charge: f64,
    pub da_eir_closeout_credit: f64,
}

pub struct SdDaasdtReport {
    pub info: MisReportInfo,
    pub lines: Vec<String>,
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

            // only insert into table the non empty rows
            let product_type: Option<ProductType> = record[8].parse().ok();
            if product_type.is_none() {
                continue;
            }
            let hour_beginning = parse_hour_ending(&self.info.report_date, &record[1]);
            let asset_id: u32 = record[2].parse()?;
            let asset_name: String = record[3].to_owned();
            let subaccount_id: u32 = record[4].parse()?;
            let subaccount_name: String = record[5].to_owned();
            let asset_type: AssetType = record[6].parse()?;
            let ownership_share: f32 = record[7].parse()?;

            let product_obligation: f64 = record[9].parse()?;
            let product_clearing_price: f64 = record[10].parse()?;
            let product_credit: f64 = record[11].parse()?;
            let customer_share_of_product_credit: f64 = record[12].parse()?;
            let strike_price: f64 = record[13].parse()?;
            let hub_rt_lmp: f64 = record[14].parse()?;
            let product_closeout_charge: f64 = record[15].parse()?;
            let customer_share_of_product_closeout_charge: f64 = record[16].parse()?;

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
                product_type: product_type.ok_or("invalid product type")?,
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

#[derive(Clone)]
pub struct SdDaasdtArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SdDaasdtArchive {
    // /// Which months to archive.  Default implementation.
    // fn get_months(&self) -> Vec<Month> {
    //     MisArchiveDuckDB::get_months(self)
    //         .into_iter()
    //         .filter(|e| e >= &self.first_month())
    //         .collect()
    // }
}

impl MisArchiveDuckDB for SdDaasdtArchive {
    fn report_name(&self) -> String {
        "SD_DAASDT".to_string()
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
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
    BEGIN;
    CREATE TABLE IF NOT EXISTS tab0 (
        account_id UINTEGER NOT NULL,
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        hour_beginning TIMESTAMPTZ NOT NULL,
        asset_id UINTEGER NOT NULL,
        asset_name VARCHAR NOT NULL,
        subaccount_id UINTEGER,
        subaccount_name VARCHAR,
        asset_type ENUM ('GENERATOR', 'ASSET RELATED DEMAND', 'DEMAND RESPONSE RESOURCE') NOT NULL,
        ownership_share FLOAT NOT NULL,
        product_type ENUM ('DA TMSR', 'DA TMNSR', 'DA TMOR', 'DA EIR') NOT NULL,
        product_obligation DOUBLE NOT NULL,
        product_clearing_price DOUBLE NOT NULL,
        product_credit DOUBLE NOT NULL,
        customer_share_of_product_credit DOUBLE NOT NULL,
        strike_price DOUBLE NOT NULL,
        hub_rt_lmp DOUBLE NOT NULL,
        product_closeout_charge DOUBLE NOT NULL,
        customer_share_of_product_closeout_charge DOUBLE NOT NULL,
    );
    CREATE INDEX idx ON tab0 (report_date);

    CREATE TABLE IF NOT EXISTS tab1 (
        account_id UINTEGER NOT NULL,
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        hour_beginning TIMESTAMPTZ NOT NULL,
        asset_id UINTEGER NOT NULL,
        asset_name VARCHAR NOT NULL,
        subaccount_id UINTEGER,
        subaccount_name VARCHAR,
        asset_type ENUM ('GENERATOR', 'ASSET RELATED DEMAND', 'DEMAND RESPONSE RESOURCE') NOT NULL,
        ownership_share FLOAT NOT NULL,
        da_cleared_energy DOUBLE,
        fer_price DOUBLE,
        asset_fer_credit DOUBLE,    
        customer_share_of_asset_fer_credit DOUBLE,
    );
    CREATE INDEX idx ON tab1 (report_date);

    CREATE TABLE IF NOT EXISTS tab6 (
        account_id UINTEGER NOT NULL,
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        subaccount_id UINTEGER,
        subaccount_name VARCHAR,
        hour_beginning TIMESTAMPTZ NOT NULL,
        rt_load_obligation DOUBLE,
        rt_external_node_load_obligation DOUBLE,
        rt_dard_load_obligation_reduction DOUBLE,
        rt_load_obligation_for_frs_charge_allocation DOUBLE,
        pool_rt_load_obligation_for_frs_charge_allocation DOUBLE,
        pool_da_tmsr_credit DOUBLE,
        da_tmsr_charge DOUBLE,
        pool_da_tmnsr_credit DOUBLE,
        da_tmnsr_charge DOUBLE,
        pool_da_tmor_credit DOUBLE,
        da_tmor_charge DOUBLE,
        pool_da_tmsr_closeout_charge DOUBLE,
        da_tmsr_closeout_credit DOUBLE,
        pool_da_tmnsr_closeout_charge DOUBLE,
        da_tmnsr_closeout_credit DOUBLE,
        pool_da_tmor_closeout_charge DOUBLE,
        da_tmor_closeout_credit DOUBLE,
    );
    CREATE INDEX idx ON tab6 (report_date);

    CREATE TABLE IF NOT EXISTS tab7 (
        account_id UINTEGER NOT NULL,
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        subaccount_id UINTEGER,
        subaccount_name VARCHAR,
        hour_beginning TIMESTAMPTZ NOT NULL,
        rt_load_obligation DOUBLE,
        rt_external_node_load_obligation DOUBLE,
        rt_dard_load_obligation_reduction DOUBLE,
        rt_load_obligation_for_da_eir_charge_allocation DOUBLE,
        pool_rt_load_obligation_for_da_eir_charge_allocation DOUBLE,
        pool_da_eir_credit DOUBLE,
        pool_fer_credit DOUBLE,
        pool_export_fer_charge DOUBLE,
        pool_fer_and_da_eir_net_credits DOUBLE,
        fer_and_da_eir_charge DOUBLE,
        pool_da_eir_closeout_charge DOUBLE,
        da_eir_closeout_credit DOUBLE,
    );
    CREATE INDEX idx ON tab7 (report_date);
    COMMIT;
    ",
        )?;

        conn.close().unwrap();
        Ok(())
    }

    fn update_duckdb(&self, files: Vec<String>) -> Result<(), Box<dyn Error>> {
        // get all reports in the db first
        let existing = self.get_reports_duckdb(0, &self.duckdb_path).unwrap();
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
            info!(
                "No new {} files to upload to DuckDB.  Continue.",
                self.report_name()
            );
            return Ok(());
        } else {
            info!("Inserting {} files into DuckDB...", paths.len());
        }

        let conn = Connection::open(&self.duckdb_path)?;
        let sql = format!(
            r"
            INSERT INTO tab0
            SELECT account_id, 
                report_date, 
                version, 
                strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
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
            FROM read_csv(
                '{}/tmp/tab0_*.CSV', 
                header = true, 
                timestampformat = '%Y-%m-%dT%H:%M:%SZ'
            );
            ",
            self.base_dir,
        );
        match conn.execute(&sql, params![]) {
            Ok(n) => info!(
                "  inserted {} rows into {} tab0 table",
                n,
                self.report_name()
            ),
            Err(e) => error!("{:?}", e),
        }

        let sql = format!(
            r"
            INSERT INTO tab1
            SELECT account_id, 
                report_date, 
                version, 
                strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
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
            FROM read_csv(
                '{}/tmp/tab1_*.CSV', 
                header = true, 
                timestampformat = '%Y-%m-%dT%H:%M:%SZ'
            );
            ",
            self.base_dir,
        );
        match conn.execute(&sql, params![]) {
            Ok(n) => info!(
                "  inserted {} rows into {} tab1 table",
                n,
                self.report_name()
            ),
            Err(e) => error!("{:?}", e),
        }

        let sql = format!(
            r"
            INSERT INTO tab6
            SELECT account_id, 
                report_date, 
                version, 
                subaccount_id,
                subaccount_name,
                strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
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
            FROM read_csv(
                '{}/tmp/tab6_*.CSV', 
                header = true, 
                timestampformat = '%Y-%m-%dT%H:%M:%SZ'
            );
            ",
            self.base_dir,
        );
        match conn.execute(&sql, params![]) {
            Ok(n) => info!(
                "  inserted {} rows into {} tab6 table",
                n,
                self.report_name()
            ),
            Err(e) => error!("{:?}", e),
        }

        let sql = format!(
            r"
            INSERT INTO tab7
            SELECT account_id, 
                report_date, 
                version, 
                subaccount_id,
                subaccount_name,
                strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
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
            FROM read_csv(
                '{}/tmp/tab7_*.CSV', 
                header = true, 
                timestampformat = '%Y-%m-%dT%H:%M:%SZ'
            );
            ",
            self.base_dir,
        );
        match conn.execute(&sql, params![]) {
            Ok(n) => info!(
                "  inserted {} rows into {} tab7 table",
                n,
                self.report_name()
            ),
            Err(e) => error!("{:?}", e),
        }

        info!("Done\n");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, str::FromStr};

    use csv::StringRecord;
    use jiff::{civil::date, Zoned};

    use crate::db::{
        isone::mis::{lib_mis::*, sd_daasdt::*},
        prod_db::ProdDb,
    };

    #[test]
    fn months_test() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::sd_daasdt();
        let months = archive.get_months();
        if Zoned::now().date() > date(2025, 3, 1) {
            assert!(!months.is_empty());
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn update_test() -> Result<(), Box<dyn Error>> {
        env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .init();

        let path = "../elec-server/test/_assets/sd_daasdt_000000002_2024111500_20241203135151.csv"
            .to_string();
        let archive = ProdDb::sd_daasdt();
        archive.setup()?;
        archive.update_duckdb(vec![path])?;

        // let info = MisReportInfo::from(path.clone());
        // let lines = read_report(&path).unwrap();
        // assert_eq!(lines.len(), 198);
        // let report = SdDaasdtReport { info, lines };
        // report.export_csv(&archive)?;

        Ok(())
    }

    #[test]
    fn parse_product_test() -> Result<(), Box<dyn Error>> {
        let rec = StringRecord::from(vec!["DA TMNSR"]);
        let product: Option<ProductType> = rec[0].parse().ok();
        assert_eq!(product, Some(ProductType::Tmnsr));
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
