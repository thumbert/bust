// Auto-generated Rust stub for DuckDB table: tab1
// Created on 2026-09-14 with Dart package reduct

use std::collections::HashMap;
use std::error::Error;
use std::fs;

use duckdb::Connection;
use log::info;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use crate::db::isone::mis::lib_mis::*;
use crate::interval::month::month;
use crate::utils::serde_helpers::*;
use convert_case::{Case, Casing};
use jiff::Timestamp;
use jiff::{civil::Date, ToSpan};
use jiff::{tz::TimeZone, Zoned};
use std::str::FromStr;

#[derive(Clone)]
pub struct SrDalocsumArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SrDalocsumArchive {
    /// Path to the temporary CSV file with the ISO report for a given tab,
    /// that will be inserted into DuckDB as is.
    #[allow(dead_code)]
    fn filename(&self, tab: u8, info: &MisReportInfo) -> String {
        self.base_dir.to_owned() + "/tmp/" + &format!("tab{}_", tab) + &info.filename_iso()
    }
}

impl MisArchive for SrDalocsumArchive {
    fn report_name(&self) -> String {
        "SR_DALOCSUM".to_string()
    }

    fn first_month(&self) -> crate::interval::month::Month {
        month(2001, 1)
    }

    fn update_duckdb(&self, files: Vec<String>) -> Result<(), Box<dyn Error>> {
        info!("Updating DuckDB with files: {:?}", files);
        Ok(())
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
    CREATE TABLE IF NOT EXISTS tab1 (
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        account_id UINTEGER NOT NULL,
        subaccount_id UINTEGER NOT NULL,
        subaccount_name VARCHAR NOT NULL,
        hour_beginning TIMESTAMPTZ NOT NULL,
        location_id UINTEGER NOT NULL,
        location_name VARCHAR NOT NULL,
        location_type ENUM ('LOAD ZONE', 'NETWORK NODE', 'HUB', 'DRR AGGREGATION ZONE') NOT NULL,
        da_cleared_generation DOUBLE,
        da_cleared_increments DOUBLE,
        da_cleared_imports DOUBLE,
        da_generation_obligation DOUBLE,
        da_cleared_demand_bids DOUBLE, 
        da_cleared_decrements DOUBLE, 
        da_cleared_exports DOUBLE, 
        da_load_obligation DOUBLE,
        da_internal_bilateral_for_purchases DOUBLE,
        da_internal_bilateral_for_sales DOUBLE,
        da_adjusted_load_obligation DOUBLE,
        da_adjusted_net_interchange DOUBLE,
        da_energy_component DOUBLE,
        da_congestion_component DOUBLE,
        da_marginal_loss_component DOUBLE,
        da_energy_charge_or_credit DOUBLE,
        da_congestion_charge_or_credit DOUBLE,
        da_marginal_loss_charge_or_credit DOUBLE,
        da_cleared_asset_related_demand_bids DOUBLE,
        da_load_obligation_for_charge_allocation DOUBLE,
        da_demand_reduction DOUBLE,
        da_demand_reduction_obligation DOUBLE,
        da_cleared_sodera_load DOUBLE,
        da_cleared_sodera_generation DOUBLE,
    );
    CREATE INDEX idx ON tab1 (report_date, account_id, subaccount_id);
    COMMIT;
    ",
        )?;

        conn.close().unwrap();
        Ok(())
    }

    // fn update_duckdb(&self, files: Vec<String>) -> Result<(), Box<dyn Error>> {
    //     // get all reports in the db first
    //     let existing = self.get_reports_duckdb(0, &self.duckdb_path).unwrap();
    //     fs::remove_dir_all(format!("{}/tmp", self.base_dir))?;
    //     fs::create_dir_all(format!("{}/tmp", self.base_dir))?;

    //     for filename in files.iter() {
    //         let info = &MisReportInfo::from(filename.clone());
    //         if existing.contains(info) {
    //             continue;
    //         }
    //         let lines = read_report(filename.as_str()).unwrap();
    //         let report = SdDaasdtReport {
    //             info: info.clone(),
    //             lines,
    //         };
    //         report.export_csv(self)?;
    //         info!("Wrote file {}", self.filename(0, info));
    //     }

    //     // list all the files and add them to the db, in order
    //     let mut paths: Vec<_> = fs::read_dir(self.base_dir.clone() + "/tmp")
    //         .unwrap()
    //         .map(|e| e.unwrap())
    //         .collect();
    //     paths.sort_by_key(|e| e.path());

    //     if paths.is_empty() {
    //         info!(
    //             "No new {} files to upload to DuckDB.  Continue.",
    //             self.report_name()
    //         );
    //         return Ok(());
    //     } else {
    //         info!("Inserting {} files into DuckDB...", paths.len());
    //     }

    //     let conn = Connection::open(&self.duckdb_path)?;
    //     let sql = format!(
    //         r"
    //         INSERT INTO tab0
    //         SELECT account_id,
    //             report_date,
    //             version,
    //             strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
    //             asset_id,
    //             asset_name,
    //             subaccount_id,
    //             subaccount_name,
    //             asset_type,
    //             ownership_share,
    //             product_type,
    //             product_obligation,
    //             product_clearing_price,
    //             product_credit,
    //             customer_share_of_product_credit,
    //             strike_price,
    //             hub_rt_lmp,
    //             product_closeout_charge,
    //             customer_share_of_product_closeout_charge,
    //         FROM read_csv(
    //             '{}/tmp/tab0_*.CSV',
    //             header = true,
    //             timestampformat = '%Y-%m-%dT%H:%M:%SZ'
    //         );
    //         ",
    //         self.base_dir,
    //     );
    //     match conn.execute(&sql, params![]) {
    //         Ok(n) => info!(
    //             "  inserted {} rows into {} tab0 table",
    //             n,
    //             self.report_name()
    //         ),
    //         Err(e) => error!("{:?}", e),
    //     }

    //     let sql = format!(
    //         r"
    //         INSERT INTO tab1
    //         SELECT account_id,
    //             report_date,
    //             version,
    //             strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
    //             asset_id,
    //             asset_name,
    //             subaccount_id,
    //             subaccount_name,
    //             asset_type,
    //             ownership_share,
    //             da_cleared_energy,
    //             fer_price,
    //             asset_fer_credit,
    //             customer_share_of_asset_fer_credit,
    //         FROM read_csv(
    //             '{}/tmp/tab1_*.CSV',
    //             header = true,
    //             timestampformat = '%Y-%m-%dT%H:%M:%SZ'
    //         );
    //         ",
    //         self.base_dir,
    //     );
    //     match conn.execute(&sql, params![]) {
    //         Ok(n) => info!(
    //             "  inserted {} rows into {} tab1 table",
    //             n,
    //             self.report_name()
    //         ),
    //         Err(e) => error!("{:?}", e),
    //     }

    //     let sql = format!(
    //         r"
    //         INSERT INTO tab6
    //         SELECT account_id,
    //             report_date,
    //             version,
    //             subaccount_id,
    //             subaccount_name,
    //             strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
    //             rt_load_obligation,
    //             rt_external_node_load_obligation,
    //             rt_dard_load_obligation_reduction,
    //             rt_load_obligation_for_frs_charge_allocation,
    //             pool_rt_load_obligation_for_frs_charge_allocation,
    //             pool_da_tmsr_credit,
    //             da_tmsr_charge,
    //             pool_da_tmnsr_credit,
    //             da_tmnsr_charge,
    //             pool_da_tmor_credit,
    //             da_tmor_charge,
    //             pool_da_tmsr_closeout_charge,
    //             da_tmsr_closeout_credit,
    //             pool_da_tmnsr_closeout_charge,
    //             da_tmnsr_closeout_credit,
    //             pool_da_tmor_closeout_charge,
    //             da_tmor_closeout_credit,
    //         FROM read_csv(
    //             '{}/tmp/tab6_*.CSV',
    //             header = true,
    //             timestampformat = '%Y-%m-%dT%H:%M:%SZ'
    //         );
    //         ",
    //         self.base_dir,
    //     );
    //     match conn.execute(&sql, params![]) {
    //         Ok(n) => info!(
    //             "  inserted {} rows into {} tab6 table",
    //             n,
    //             self.report_name()
    //         ),
    //         Err(e) => error!("{:?}", e),
    //     }

    //     let sql = format!(
    //         r"
    //         INSERT INTO tab7
    //         SELECT account_id,
    //             report_date,
    //             version,
    //             subaccount_id,
    //             subaccount_name,
    //             strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
    //             rt_load_obligation,
    //             rt_external_node_load_obligation,
    //             rt_dard_load_obligation_reduction,
    //             rt_load_obligation_for_da_eir_charge_allocation,
    //             pool_rt_load_obligation_for_da_eir_charge_allocation,
    //             pool_da_eir_credit,
    //             pool_fer_credit,
    //             pool_export_fer_charge,
    //             pool_fer_and_da_eir_net_credits,
    //             fer_and_da_eir_charge,
    //             pool_da_eir_closeout_charge,
    //             da_eir_closeout_credit,
    //         FROM read_csv(
    //             '{}/tmp/tab7_*.CSV',
    //             header = true,
    //             timestampformat = '%Y-%m-%dT%H:%M:%SZ'
    //         );
    //         ",
    //         self.base_dir,
    //     );
    //     match conn.execute(&sql, params![]) {
    //         Ok(n) => info!(
    //             "  inserted {} rows into {} tab7 table",
    //             n,
    //             self.report_name()
    //         ),
    //         Err(e) => error!("{:?}", e),
    //     }

    //     info!("Done\n");
    //     Ok(())
    // }
}

pub struct SrDalocsumReport {
    pub info: MisReportInfo,
    pub lines: Vec<String>,
}

impl MisReport for SrDalocsumReport {}

impl SrDalocsumReport {
    #[allow(dead_code)]
    fn process_tab1(&self) -> Result<Vec<Record>, Box<dyn Error>> {
        let mut out: Vec<Record> = Vec::new();
        let tab1 = extract_tab(1, &self.lines).unwrap();
        let data = tab1.lines.join("\n");
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        for result in rdr.records() {
            let record = result?;
            let hour_beginning = parse_hour_ending(&self.info.report_date, &record[3]);

            out.push(Record {
                account_id: self.info.account_id as u32,
                report_date: self.info.report_date,
                version: self.info.version,
                hour_beginning,
                subaccount_id: record[1].parse()?,
                subaccount_name: record[2].to_owned(),
                location_id: record[4].parse()?,
                location_name: record[5].to_owned(),
                location_type: record[6].parse()?,
                da_cleared_generation: parse_opt_f64(&record[7])?,
                da_cleared_increments: parse_opt_f64(&record[8])?,
                da_cleared_imports: parse_opt_f64(&record[9])?,
                da_generation_obligation: parse_opt_f64(&record[10])?,
                da_cleared_demand_bids: parse_opt_f64(&record[11])?,
                da_cleared_decrements: parse_opt_f64(&record[12])?,
                da_cleared_exports: parse_opt_f64(&record[13])?,
                da_load_obligation: parse_opt_f64(&record[14])?,
                da_internal_bilateral_for_purchases: parse_opt_f64(&record[15])?,
                da_internal_bilateral_for_sales: parse_opt_f64(&record[16])?,
                da_adjusted_load_obligation: parse_opt_f64(&record[17])?,
                da_adjusted_net_interchange: parse_opt_f64(&record[18])?,
                da_energy_component: parse_opt_f64(&record[19])?,
                da_congestion_component: parse_opt_f64(&record[20])?,
                da_marginal_loss_component: parse_opt_f64(&record[21])?,
                da_energy_charge_or_credit: parse_opt_f64(&record[22])?,
                da_congestion_charge_or_credit: parse_opt_f64(&record[23])?,
                da_marginal_loss_charge_or_credit: parse_opt_f64(&record[24])?,
                da_cleared_asset_related_demand_bids: parse_opt_f64(&record[25])?,
                da_load_obligation_for_charge_allocation: parse_opt_f64(&record[26])?,
                da_demand_reduction: parse_opt_f64(&record[27])?,
                da_demand_reduction_obligation: parse_opt_f64(&record[28])?,
                da_cleared_sodera_load: parse_opt_f64(&record[29])?,
                da_cleared_sodera_generation: parse_opt_f64(&record[30])?,
            });
        }

        Ok(out)
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub report_date: Date,
    pub version: Timestamp,
    pub account_id: u32,
    pub subaccount_id: u32,
    pub subaccount_name: String,
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    pub hour_beginning: Zoned,
    pub location_id: u32,
    pub location_name: String,
    pub location_type: LocationType,
    pub da_cleared_generation: Option<f64>,
    pub da_cleared_increments: Option<f64>,
    pub da_cleared_imports: Option<f64>,
    pub da_generation_obligation: Option<f64>,
    pub da_cleared_demand_bids: Option<f64>,
    pub da_cleared_decrements: Option<f64>,
    pub da_cleared_exports: Option<f64>,
    pub da_load_obligation: Option<f64>,
    pub da_internal_bilateral_for_purchases: Option<f64>,
    pub da_internal_bilateral_for_sales: Option<f64>,
    pub da_adjusted_load_obligation: Option<f64>,
    pub da_adjusted_net_interchange: Option<f64>,
    pub da_energy_component: Option<f64>,
    pub da_congestion_component: Option<f64>,
    pub da_marginal_loss_component: Option<f64>,
    pub da_energy_charge_or_credit: Option<f64>,
    pub da_congestion_charge_or_credit: Option<f64>,
    pub da_marginal_loss_charge_or_credit: Option<f64>,
    pub da_cleared_asset_related_demand_bids: Option<f64>,
    pub da_load_obligation_for_charge_allocation: Option<f64>,
    pub da_demand_reduction: Option<f64>,
    pub da_demand_reduction_obligation: Option<f64>,
    pub da_cleared_sodera_load: Option<f64>,
    pub da_cleared_sodera_generation: Option<f64>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum LocationType {
    DrrAggregationZone,
    Hub,
    LoadZone,
    NetworkNode,
}

impl std::str::FromStr for LocationType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "DRR_AGGREGATION_ZONE" => Ok(LocationType::DrrAggregationZone),
            "HUB" => Ok(LocationType::Hub),
            "LOAD_ZONE" => Ok(LocationType::LoadZone),
            "NETWORK_NODE" => Ok(LocationType::NetworkNode),
            _ => Err(format!("Invalid value for LocationType: {}", s)),
        }
    }
}

impl std::fmt::Display for LocationType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            LocationType::DrrAggregationZone => write!(f, "DRR AGGREGATION ZONE"),
            LocationType::Hub => write!(f, "HUB"),
            LocationType::LoadZone => write!(f, "LOAD ZONE"),
            LocationType::NetworkNode => write!(f, "NETWORK NODE"),
        }
    }
}

impl serde::Serialize for LocationType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            LocationType::DrrAggregationZone => "DRR AGGREGATION ZONE",
            LocationType::Hub => "HUB",
            LocationType::LoadZone => "LOAD ZONE",
            LocationType::NetworkNode => "NETWORK NODE",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for LocationType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        LocationType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
    limit: Option<usize>,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    report_date,
    version,
    account_id,
    subaccount_id,
    subaccount_name,
    hour_beginning,
    location_id,
    location_name,
    location_type,
    da_cleared_generation,
    da_cleared_increments,
    da_cleared_imports,
    da_generation_obligation,
    da_cleared_demand_bids,
    da_cleared_decrements,
    da_cleared_exports,
    da_load_obligation,
    da_internal_bilateral_for_purchases,
    da_internal_bilateral_for_sales,
    da_adjusted_load_obligation,
    da_adjusted_net_interchange,
    da_energy_component,
    da_congestion_component,
    da_marginal_loss_component,
    da_energy_charge_or_credit,
    da_congestion_charge_or_credit,
    da_marginal_loss_charge_or_credit,
    da_cleared_asset_related_demand_bids,
    da_load_obligation_for_charge_allocation,
    da_demand_reduction,
    da_demand_reduction_obligation,
    da_cleared_sodera_load,
    da_cleared_sodera_generation
FROM tab1 WHERE 1=1"#,
    );
    if let Some(report_date) = &query_filter.report_date {
        query.push_str(&format!(
            "
    AND report_date = '{}'",
            report_date
        ));
    }
    if let Some(report_date_in) = &query_filter.report_date_in {
        query.push_str(&format!(
            "
    AND report_date IN ('{}')",
            report_date_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(report_date_gte) = &query_filter.report_date_gte {
        query.push_str(&format!(
            "
    AND report_date >= '{}'",
            report_date_gte
        ));
    }
    if let Some(report_date_lte) = &query_filter.report_date_lte {
        query.push_str(&format!(
            "
    AND report_date <= '{}'",
            report_date_lte
        ));
    }
    if let Some(account_id) = &query_filter.account_id {
        query.push_str(&format!(
            "
    AND account_id = {}",
            account_id
        ));
    }
    if let Some(account_id_in) = &query_filter.account_id_in {
        query.push_str(&format!(
            "
    AND account_id IN ({})",
            account_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(account_id_gte) = &query_filter.account_id_gte {
        query.push_str(&format!(
            "
    AND account_id >= {}",
            account_id_gte
        ));
    }
    if let Some(account_id_lte) = &query_filter.account_id_lte {
        query.push_str(&format!(
            "
    AND account_id <= {}",
            account_id_lte
        ));
    }
    if let Some(subaccount_id) = &query_filter.subaccount_id {
        query.push_str(&format!(
            "
    AND subaccount_id = {}",
            subaccount_id
        ));
    }
    if let Some(subaccount_id_in) = &query_filter.subaccount_id_in {
        query.push_str(&format!(
            "
    AND subaccount_id IN ({})",
            subaccount_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(subaccount_id_gte) = &query_filter.subaccount_id_gte {
        query.push_str(&format!(
            "
    AND subaccount_id >= {}",
            subaccount_id_gte
        ));
    }
    if let Some(subaccount_id_lte) = &query_filter.subaccount_id_lte {
        query.push_str(&format!(
            "
    AND subaccount_id <= {}",
            subaccount_id_lte
        ));
    }
    if let Some(location_id) = &query_filter.location_id {
        query.push_str(&format!(
            "
    AND location_id = {}",
            location_id
        ));
    }
    if let Some(location_id_in) = &query_filter.location_id_in {
        query.push_str(&format!(
            "
    AND location_id IN ({})",
            location_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(location_id_gte) = &query_filter.location_id_gte {
        query.push_str(&format!(
            "
    AND location_id >= {}",
            location_id_gte
        ));
    }
    if let Some(location_id_lte) = &query_filter.location_id_lte {
        query.push_str(&format!(
            "
    AND location_id <= {}",
            location_id_lte
        ));
    }
    match limit {
        Some(l) => {
            query.push_str(&format!(
                "
LIMIT {};",
                l
            ));
        }
        None => {
            query.push(';');
        }
    }

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let _n0 = 719528 + row.get::<usize, i32>(0)?;
        let report_date = Date::ZERO + _n0.days();
        let _micros1: i64 = row.get::<usize, i64>(1)?;
        let version = Timestamp::from_microsecond(_micros1).unwrap();
        let account_id: u32 = row.get::<usize, u32>(2)?;
        let subaccount_id: u32 = row.get::<usize, u32>(3)?;
        let subaccount_name: String = row.get::<usize, String>(4)?;
        let _micros5: i64 = row.get::<usize, i64>(5)?;
        let hour_beginning = Zoned::new(
            Timestamp::from_microsecond(_micros5).unwrap(),
            TimeZone::get("America/New_York").unwrap(),
        );
        let location_id: u32 = row.get::<usize, u32>(6)?;
        let location_name: String = row.get::<usize, String>(7)?;
        let _n8 = match row.get_ref_unwrap(8).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum location_type"),
        };
        let location_type = LocationType::from_str(&_n8).unwrap();
        let da_cleared_generation: Option<f64> = row.get::<usize, Option<f64>>(9)?;
        let da_cleared_increments: Option<f64> = row.get::<usize, Option<f64>>(10)?;
        let da_cleared_imports: Option<f64> = row.get::<usize, Option<f64>>(11)?;
        let da_generation_obligation: Option<f64> = row.get::<usize, Option<f64>>(12)?;
        let da_cleared_demand_bids: Option<f64> = row.get::<usize, Option<f64>>(13)?;
        let da_cleared_decrements: Option<f64> = row.get::<usize, Option<f64>>(14)?;
        let da_cleared_exports: Option<f64> = row.get::<usize, Option<f64>>(15)?;
        let da_load_obligation: Option<f64> = row.get::<usize, Option<f64>>(16)?;
        let da_internal_bilateral_for_purchases: Option<f64> = row.get::<usize, Option<f64>>(17)?;
        let da_internal_bilateral_for_sales: Option<f64> = row.get::<usize, Option<f64>>(18)?;
        let da_adjusted_load_obligation: Option<f64> = row.get::<usize, Option<f64>>(19)?;
        let da_adjusted_net_interchange: Option<f64> = row.get::<usize, Option<f64>>(20)?;
        let da_energy_component: Option<f64> = row.get::<usize, Option<f64>>(21)?;
        let da_congestion_component: Option<f64> = row.get::<usize, Option<f64>>(22)?;
        let da_marginal_loss_component: Option<f64> = row.get::<usize, Option<f64>>(23)?;
        let da_energy_charge_or_credit: Option<f64> = row.get::<usize, Option<f64>>(24)?;
        let da_congestion_charge_or_credit: Option<f64> = row.get::<usize, Option<f64>>(25)?;
        let da_marginal_loss_charge_or_credit: Option<f64> = row.get::<usize, Option<f64>>(26)?;
        let da_cleared_asset_related_demand_bids: Option<f64> =
            row.get::<usize, Option<f64>>(27)?;
        let da_load_obligation_for_charge_allocation: Option<f64> =
            row.get::<usize, Option<f64>>(28)?;
        let da_demand_reduction: Option<f64> = row.get::<usize, Option<f64>>(29)?;
        let da_demand_reduction_obligation: Option<f64> = row.get::<usize, Option<f64>>(30)?;
        let da_cleared_sodera_load: Option<f64> = row.get::<usize, Option<f64>>(31)?;
        let da_cleared_sodera_generation: Option<f64> = row.get::<usize, Option<f64>>(32)?;
        Ok(Record {
            report_date,
            version,
            account_id,
            subaccount_id,
            subaccount_name,
            hour_beginning,
            location_id,
            location_name,
            location_type,
            da_cleared_generation,
            da_cleared_increments,
            da_cleared_imports,
            da_generation_obligation,
            da_cleared_demand_bids,
            da_cleared_decrements,
            da_cleared_exports,
            da_load_obligation,
            da_internal_bilateral_for_purchases,
            da_internal_bilateral_for_sales,
            da_adjusted_load_obligation,
            da_adjusted_net_interchange,
            da_energy_component,
            da_congestion_component,
            da_marginal_loss_component,
            da_energy_charge_or_credit,
            da_congestion_charge_or_credit,
            da_marginal_loss_charge_or_credit,
            da_cleared_asset_related_demand_bids,
            da_load_obligation_for_charge_allocation,
            da_demand_reduction,
            da_demand_reduction_obligation,
            da_cleared_sodera_load,
            da_cleared_sodera_generation,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub report_date: Option<Date>,
    pub report_date_in: Option<Vec<Date>>,
    pub report_date_gte: Option<Date>,
    pub report_date_lte: Option<Date>,
    pub account_id: Option<u32>,
    pub account_id_in: Option<Vec<u32>>,
    pub account_id_gte: Option<u32>,
    pub account_id_lte: Option<u32>,
    pub subaccount_id: Option<u32>,
    pub subaccount_id_in: Option<Vec<u32>>,
    pub subaccount_id_gte: Option<u32>,
    pub subaccount_id_lte: Option<u32>,
    pub location_id: Option<u32>,
    pub location_id_in: Option<Vec<u32>>,
    pub location_id_gte: Option<u32>,
    pub location_id_lte: Option<u32>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.report_date {
            params.insert("report_date", value.to_string());
        }
        if let Some(value) = &self.report_date_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("report_date_in", joined);
        }
        if let Some(value) = &self.report_date_gte {
            params.insert("report_date_gte", value.to_string());
        }
        if let Some(value) = &self.report_date_lte {
            params.insert("report_date_lte", value.to_string());
        }
        if let Some(value) = &self.account_id {
            params.insert("account_id", value.to_string());
        }
        if let Some(value) = &self.account_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("account_id_in", joined);
        }
        if let Some(value) = &self.account_id_gte {
            params.insert("account_id_gte", value.to_string());
        }
        if let Some(value) = &self.account_id_lte {
            params.insert("account_id_lte", value.to_string());
        }
        if let Some(value) = &self.subaccount_id {
            params.insert("subaccount_id", value.to_string());
        }
        if let Some(value) = &self.subaccount_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("subaccount_id_in", joined);
        }
        if let Some(value) = &self.subaccount_id_gte {
            params.insert("subaccount_id_gte", value.to_string());
        }
        if let Some(value) = &self.subaccount_id_lte {
            params.insert("subaccount_id_lte", value.to_string());
        }
        if let Some(value) = &self.location_id {
            params.insert("location_id", value.to_string());
        }
        if let Some(value) = &self.location_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("location_id_in", joined);
        }
        if let Some(value) = &self.location_id_gte {
            params.insert("location_id_gte", value.to_string());
        }
        if let Some(value) = &self.location_id_lte {
            params.insert("location_id_lte", value.to_string());
        }
        form_urlencoded::Serializer::new(String::new())
            .extend_pairs(&params)
            .finish()
    }
}

#[derive(Default)]
pub struct QueryFilterBuilder {
    inner: QueryFilter,
}

impl QueryFilterBuilder {
    pub fn new() -> Self {
        Self {
            inner: QueryFilter::default(),
        }
    }

    pub fn build(self) -> QueryFilter {
        self.inner
    }

    pub fn report_date(mut self, value: Date) -> Self {
        self.inner.report_date = Some(value);
        self
    }

    pub fn report_date_in(mut self, values_in: Vec<Date>) -> Self {
        self.inner.report_date_in = Some(values_in);
        self
    }

    pub fn report_date_gte(mut self, value: Date) -> Self {
        self.inner.report_date_gte = Some(value);
        self
    }

    pub fn report_date_lte(mut self, value: Date) -> Self {
        self.inner.report_date_lte = Some(value);
        self
    }

    pub fn account_id(mut self, value: u32) -> Self {
        self.inner.account_id = Some(value);
        self
    }

    pub fn account_id_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.account_id_in = Some(values_in);
        self
    }

    pub fn account_id_gte(mut self, value: u32) -> Self {
        self.inner.account_id_gte = Some(value);
        self
    }

    pub fn account_id_lte(mut self, value: u32) -> Self {
        self.inner.account_id_lte = Some(value);
        self
    }

    pub fn subaccount_id(mut self, value: u32) -> Self {
        self.inner.subaccount_id = Some(value);
        self
    }

    pub fn subaccount_id_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.subaccount_id_in = Some(values_in);
        self
    }

    pub fn subaccount_id_gte(mut self, value: u32) -> Self {
        self.inner.subaccount_id_gte = Some(value);
        self
    }

    pub fn subaccount_id_lte(mut self, value: u32) -> Self {
        self.inner.subaccount_id_lte = Some(value);
        self
    }

    pub fn location_id(mut self, value: u32) -> Self {
        self.inner.location_id = Some(value);
        self
    }

    pub fn location_id_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.location_id_in = Some(values_in);
        self
    }

    pub fn location_id_gte(mut self, value: u32) -> Self {
        self.inner.location_id_gte = Some(value);
        self
    }

    pub fn location_id_lte(mut self, value: u32) -> Self {
        self.inner.location_id_lte = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::prod_db::ProdDb;
    use duckdb::{AccessMode, Config, Connection};
    use std::error::Error;

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::scratch().duckdb_path, config).unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }
}
