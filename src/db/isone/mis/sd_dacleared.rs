// Auto-generated Rust stub for DuckDB table: tab0
// Created on 2026-09-15 with Dart package reduct

use std::collections::HashMap;
use std::error::Error;
use std::fs;
use std::process::Command;

use duckdb::Connection;
use log::{error, info};
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use crate::db::isone::mis::lib_mis::MisArchive;
use crate::interval::month::month;
use crate::utils::serde_helpers::*;
use convert_case::{Case, Casing};
use jiff::Timestamp;
use jiff::{civil::Date, ToSpan};
use jiff::{tz::TimeZone, Zoned};
use rust_decimal::Decimal;
use std::str::FromStr;

#[derive(Clone)]
pub struct SdDaclearedArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SdDaclearedArchive {}

impl MisArchive for SdDaclearedArchive {
    fn report_name(&self) -> String {
        "SD_DACLEARED".to_string()
    }

    fn first_month(&self) -> crate::interval::month::Month {
        month(2001, 1)
    }

    fn update_duckdb(&self, files: Vec<String>) -> Result<(), Box<dyn Error>> {
        if files.is_empty() {
            info!("... No files to process");
            return Ok(());
        }
        let mut sql_files = String::from("SET VARIABLE files = array_value(\n");
        for file in &files {
            sql_files.push_str(&format!("    '{}',\n", file));
        }
        sql_files.push_str(");\n");

        let sql = format!(
            r#"
{}            
WITH raw AS (
    SELECT
        regexp_extract(filename, 'SD_DACLEARED_([^/]+)\.csv$', 1) AS base_name,
        column1 AS trading_interval,
        column2::UINTEGER AS location_id,
        column3 AS location_name,
        column4 AS location_type,
        column5 AS bid_offer_type,
        column6::VARCHAR AS reference_id,
        column7::UINTEGER AS asset_id,
        column8::DECIMAL(9,4) AS ownership_share,
        column9::DECIMAL(9,1) AS cleared_mw
    FROM read_csv(
        getvariable('files')::VARCHAR[],
        header = false,
        all_varchar = true,
        filename = true
    )
    WHERE column0 = 'D'
),
parsed AS (
    SELECT *,
        strptime(substr(split_part(base_name, '_', 2), 1, 8), '%Y%m%d')::DATE AS report_date,
        strptime(split_part(base_name, '_', 3), '%Y%m%d%H%M%S') AS version,
        split_part(base_name, '_', 1)::UINTEGER AS account_id,
        regexp_replace(trading_interval, '[Xx]$', '')::UTINYINT AS base_hour,
        trading_interval ILIKE '%X' AS is_extra_hour
    FROM raw
),
positioned AS (
    SELECT *,
        max(is_extra_hour::INT) OVER (PARTITION BY report_date, location_id) AS has_extra_hour,
        max((base_hour = 2 AND NOT is_extra_hour)::INT) OVER (PARTITION BY report_date, location_id) AS has_hour2,
    FROM parsed
),
to_insert AS (
    SELECT
        report_date,
        version,
        account_id,
        location_id,
        location_name,
        location_type,
        bid_offer_type,
        reference_id,
        asset_id,
        (report_date AT TIME ZONE 'America/New_York')
            + (
                CASE
                    WHEN is_extra_hour THEN 2
                    WHEN base_hour <= 2 THEN base_hour - 1
                    ELSE base_hour - 1 + has_extra_hour
                        - CASE WHEN has_extra_hour = 0 AND has_hour2 = 0 THEN 1 ELSE 0 END
                END
            ) * INTERVAL '1 hour' AS hour_beginning,
        ownership_share,
        cleared_mw
    FROM positioned
)
-- skip rows already present, keyed on (account_id, report_date, version, hour_beginning, location_id)
INSERT INTO tab0
SELECT ti.*
FROM to_insert ti
WHERE NOT EXISTS (
    SELECT 1 FROM tab0 t
    WHERE t.account_id = ti.account_id
    AND t.report_date = ti.report_date
    AND t.version = ti.version
    AND t.hour_beginning = ti.hour_beginning
    AND t.location_id = ti.location_id
    AND t.reference_id = ti.reference_id
);
"#,
            sql_files
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
            error!("Failed to update duckdb: {}", stderr);
        }

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
    CREATE TABLE IF NOT EXISTS tab0 (
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        account_id UINTEGER NOT NULL,
        location_id UINTEGER NOT NULL,
        location_name VARCHAR NOT NULL,
        location_type ENUM ('LOAD ZONE', 'NETWORK NODE', 'HUB', 'DRR AGGREGATION ZONE') NOT NULL,
        bid_offer_type ENUM ('DEMAND BID', 'GEN OFFER', 'INC', 'DEC', 'REDUCTION OFFER') NOT NULL,
        reference_id VARCHAR,
        asset_id UINTEGER NOT NULL,
        hour_beginning TIMESTAMPTZ NOT NULL,
        ownership_share DECIMAL(9,4) NOT NULL,
        cleared_mw DECIMAL(9,1) NOT NULL
    );
    CREATE INDEX idx ON tab0 (report_date, account_id, location_id);
    COMMIT;
    ",
        )?;

        conn.close().unwrap();
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub report_date: Date,
    pub version: Timestamp,
    pub account_id: u32,
    pub location_id: u32,
    pub location_name: String,
    pub location_type: LocationType,
    pub bid_offer_type: BidOfferType,
    pub reference_id: Option<String>,
    pub asset_id: u32,
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    pub hour_beginning: Zoned,
    #[serde(with = "rust_decimal::serde::float")]
    pub ownership_share: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub cleared_mw: Decimal,
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

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BidOfferType {
    Dec,
    DemandBid,
    GenOffer,
    Inc,
    ReductionOffer,
}

impl std::str::FromStr for BidOfferType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "DEC" => Ok(BidOfferType::Dec),
            "DEMAND_BID" => Ok(BidOfferType::DemandBid),
            "GEN_OFFER" => Ok(BidOfferType::GenOffer),
            "INC" => Ok(BidOfferType::Inc),
            "REDUCTION_OFFER" => Ok(BidOfferType::ReductionOffer),
            _ => Err(format!("Invalid value for BidOfferType: {}", s)),
        }
    }
}

impl std::fmt::Display for BidOfferType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BidOfferType::Dec => write!(f, "DEC"),
            BidOfferType::DemandBid => write!(f, "DEMAND BID"),
            BidOfferType::GenOffer => write!(f, "GEN OFFER"),
            BidOfferType::Inc => write!(f, "INC"),
            BidOfferType::ReductionOffer => write!(f, "REDUCTION OFFER"),
        }
    }
}

impl serde::Serialize for BidOfferType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            BidOfferType::Dec => "DEC",
            BidOfferType::DemandBid => "DEMAND BID",
            BidOfferType::GenOffer => "GEN OFFER",
            BidOfferType::Inc => "INC",
            BidOfferType::ReductionOffer => "REDUCTION OFFER",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for BidOfferType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        BidOfferType::from_str(&s).map_err(serde::de::Error::custom)
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
    location_id,
    location_name,
    location_type,
    bid_offer_type,
    reference_id,
    asset_id,
    hour_beginning,
    ownership_share,
    cleared_mw
FROM tab0 WHERE 1=1"#,
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
    if let Some(bid_offer_type) = &query_filter.bid_offer_type {
        query.push_str(&format!(
            "
    AND bid_offer_type = '{}'",
            bid_offer_type
        ));
    }
    if let Some(bid_offer_type_in) = &query_filter.bid_offer_type_in {
        query.push_str(&format!(
            "
    AND bid_offer_type IN ('{}')",
            bid_offer_type_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
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
        let location_id: u32 = row.get::<usize, u32>(3)?;
        let location_name: String = row.get::<usize, String>(4)?;
        let _n5 = match row.get_ref_unwrap(5).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum location_type"),
        };
        let location_type = LocationType::from_str(&_n5).unwrap();
        let _n6 = match row.get_ref_unwrap(6).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum bid_offer_type"),
        };
        let bid_offer_type = BidOfferType::from_str(&_n6).unwrap();
        let reference_id: Option<String> = row.get::<usize, Option<String>>(7)?;
        let asset_id: u32 = row.get::<usize, u32>(8)?;
        let _micros9: i64 = row.get::<usize, i64>(9)?;
        let hour_beginning = Zoned::new(
            Timestamp::from_microsecond(_micros9).unwrap(),
            TimeZone::get("America/New_York").unwrap(),
        );
        let ownership_share: Decimal = match row.get_ref_unwrap(10) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let cleared_mw: Decimal = match row.get_ref_unwrap(11) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Record {
            report_date,
            version,
            account_id,
            location_id,
            location_name,
            location_type,
            bid_offer_type,
            reference_id,
            asset_id,
            hour_beginning,
            ownership_share,
            cleared_mw,
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
    pub location_id: Option<u32>,
    pub location_id_in: Option<Vec<u32>>,
    pub location_id_gte: Option<u32>,
    pub location_id_lte: Option<u32>,
    pub bid_offer_type: Option<BidOfferType>,
    pub bid_offer_type_in: Option<Vec<BidOfferType>>,
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
        if let Some(value) = &self.bid_offer_type {
            params.insert("bid_offer_type", value.to_string());
        }
        if let Some(value) = &self.bid_offer_type_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("bid_offer_type_in", joined);
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

    pub fn bid_offer_type(mut self, value: BidOfferType) -> Self {
        self.inner.bid_offer_type = Some(value);
        self
    }

    pub fn bid_offer_type_in(mut self, values_in: Vec<BidOfferType>) -> Self {
        self.inner.bid_offer_type_in = Some(values_in);
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
        let conn = Connection::open_with_flags(ProdDb::sd_dacleared().duckdb_path, config).unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }
}
