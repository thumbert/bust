// Auto-generated Rust stub for DuckDB table: tab1
// Created on 2026-09-14 with Dart package reduct

use std::collections::HashMap;
use std::error::Error;

use duckdb::Connection;
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
pub struct SrRtlocsumArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SrRtlocsumArchive {
    /// Path to the temporary CSV file with the ISO report for a given tab,
    /// that will be inserted into DuckDB as is.
    fn filename(&self, tab: u8, info: &MisReportInfo) -> String {
        self.base_dir.to_owned() + "/tmp/" + &format!("tab{}_", tab) + &info.filename_iso()
    }
}

impl MisArchive for SrRtlocsumArchive {
    fn report_name(&self) -> String {
        "SR_RTLOCSUM".to_string()
    }

    fn first_month(&self) -> crate::interval::month::Month {
        month(2001, 1)
    }

    fn setup(&self) -> Result<(), Box<dyn Error>> {
        todo!()
    }

    fn update_duckdb(&self, files: Vec<String>) -> Result<(), Box<dyn Error>> {
        todo!()
    }
}

pub struct SrRtlocsumReport {
    pub info: MisReportInfo,
    pub lines: Vec<String>,
}

impl MisReport for SrRtlocsumReport {}

impl SrRtlocsumReport {
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
            let asset_id: u32 = record[2].parse()?;
            let asset_name: String = record[3].to_owned();
            let subaccount_id: u32 = record[1].parse()?;
            let subaccount_name: String = record[2].to_owned();

            out.push(Record {
                account_id: self.info.account_id as u32,
                report_date: self.info.report_date,
                version: self.info.version,
                hour_beginning,
                subaccount_id,
                subaccount_name,
                location_id: record[4].parse()?,
                location_name: record[5].to_owned(),
                location_type: record[6].parse()?,
                revenue_metered_generation: todo!(),
                scheduled_imports: todo!(),
                rt_generation_obligation: todo!(),
                revenue_metered_load: todo!(),
                scheduled_exports: todo!(),
                internal_bilateral_for_load: todo!(),
                rt_load_obligation: todo!(),
                rt_internal_bilateral_for_market_purchases: todo!(),
                rt_internal_bilateral_for_market_sales: todo!(),
                rt_adjusted_load_obligation: todo!(),
                rt_adjusted_net_interchange: todo!(),
                adjusted_net_interchange_deviation: todo!(),
                rt_energy_component: todo!(),
                rt_congestion_component: todo!(),
                rt_marginal_loss_component: todo!(),
                rt_energy_charge_or_credit: todo!(),
                rt_congestion_charge_or_credit: todo!(),
                rt_loss_charge_or_credit: todo!(),
                rt_internal_bilateral_for_market_purchases_impacting_mlrlo: todo!(),
                rt_internal_bilateral_for_market_sales_impacting_mlrlo: todo!(),
                marginal_loss_revenue_load_obligation: todo!(),
                rt_generation_obligation_for_charge_allocation: todo!(),
                rt_load_obligation_for_charge_allocation: todo!(),
                rt_adjusted_net_interchange_for_charge_allocation: todo!(),
                rt_demand_reduction_obligation: todo!(),
                rt_load_obligation_for_demand_reduction_allocation: todo!(),
                demand_reduction_obligation_deviation: todo!(),
                rt_demand_reduction_credit: todo!(),
                rt_demand_reduction_charge: todo!(),
                rt_satoa_obligation: todo!(),
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
    pub revenue_metered_generation: Option<f64>,
    pub scheduled_imports: Option<f64>,
    pub rt_generation_obligation: Option<f64>,
    pub revenue_metered_load: Option<f64>,
    pub scheduled_exports: Option<f64>,
    pub internal_bilateral_for_load: Option<f64>,
    pub rt_load_obligation: Option<f64>,
    pub rt_internal_bilateral_for_market_purchases: Option<f64>,
    pub rt_internal_bilateral_for_market_sales: Option<f64>,
    pub rt_adjusted_load_obligation: Option<f64>,
    pub rt_adjusted_net_interchange: Option<f64>,
    pub adjusted_net_interchange_deviation: Option<f64>,
    pub rt_energy_component: Option<f64>,
    pub rt_congestion_component: Option<f64>,
    pub rt_marginal_loss_component: Option<f64>,
    pub rt_energy_charge_or_credit: Option<f64>,
    pub rt_congestion_charge_or_credit: Option<f64>,
    pub rt_loss_charge_or_credit: Option<f64>,
    pub rt_internal_bilateral_for_market_purchases_impacting_mlrlo: Option<f64>,
    pub rt_internal_bilateral_for_market_sales_impacting_mlrlo: Option<f64>,
    pub marginal_loss_revenue_load_obligation: Option<f64>,
    pub rt_generation_obligation_for_charge_allocation: Option<f64>,
    pub rt_load_obligation_for_charge_allocation: Option<f64>,
    pub rt_adjusted_net_interchange_for_charge_allocation: Option<f64>,
    pub rt_demand_reduction_obligation: Option<f64>,
    pub rt_load_obligation_for_demand_reduction_allocation: Option<f64>,
    pub demand_reduction_obligation_deviation: Option<f64>,
    pub rt_demand_reduction_credit: Option<f64>,
    pub rt_demand_reduction_charge: Option<f64>,
    pub rt_satoa_obligation: Option<f64>,
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
    revenue_metered_generation,
    scheduled_imports,
    rt_generation_obligation,
    revenue_metered_load,
    scheduled_exports,
    internal_bilateral_for_load,
    rt_load_obligation,
    rt_internal_bilateral_for_market_purchases,
    rt_internal_bilateral_for_market_sales,
    rt_adjusted_load_obligation,
    rt_adjusted_net_interchange,
    adjusted_net_interchange_deviation,
    rt_energy_component,
    rt_congestion_component,
    rt_marginal_loss_component,
    rt_energy_charge_or_credit,
    rt_congestion_charge_or_credit,
    rt_loss_charge_or_credit,
    rt_internal_bilateral_for_market_purchases_impacting_mlrlo,
    rt_internal_bilateral_for_market_sales_impacting_mlrlo,
    marginal_loss_revenue_load_obligation,
    rt_generation_obligation_for_charge_allocation,
    rt_load_obligation_for_charge_allocation,
    rt_adjusted_net_interchange_for_charge_allocation,
    rt_demand_reduction_obligation,
    rt_load_obligation_for_demand_reduction_allocation,
    demand_reduction_obligation_deviation,
    rt_demand_reduction_credit,
    rt_demand_reduction_charge,
    rt_satoa_obligation
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
        let revenue_metered_generation: Option<f64> = row.get::<usize, Option<f64>>(9)?;
        let scheduled_imports: Option<f64> = row.get::<usize, Option<f64>>(10)?;
        let rt_generation_obligation: Option<f64> = row.get::<usize, Option<f64>>(11)?;
        let revenue_metered_load: Option<f64> = row.get::<usize, Option<f64>>(12)?;
        let scheduled_exports: Option<f64> = row.get::<usize, Option<f64>>(13)?;
        let internal_bilateral_for_load: Option<f64> = row.get::<usize, Option<f64>>(14)?;
        let rt_load_obligation: Option<f64> = row.get::<usize, Option<f64>>(15)?;
        let rt_internal_bilateral_for_market_purchases: Option<f64> =
            row.get::<usize, Option<f64>>(16)?;
        let rt_internal_bilateral_for_market_sales: Option<f64> =
            row.get::<usize, Option<f64>>(17)?;
        let rt_adjusted_load_obligation: Option<f64> = row.get::<usize, Option<f64>>(18)?;
        let rt_adjusted_net_interchange: Option<f64> = row.get::<usize, Option<f64>>(19)?;
        let adjusted_net_interchange_deviation: Option<f64> = row.get::<usize, Option<f64>>(20)?;
        let rt_energy_component: Option<f64> = row.get::<usize, Option<f64>>(21)?;
        let rt_congestion_component: Option<f64> = row.get::<usize, Option<f64>>(22)?;
        let rt_marginal_loss_component: Option<f64> = row.get::<usize, Option<f64>>(23)?;
        let rt_energy_charge_or_credit: Option<f64> = row.get::<usize, Option<f64>>(24)?;
        let rt_congestion_charge_or_credit: Option<f64> = row.get::<usize, Option<f64>>(25)?;
        let rt_loss_charge_or_credit: Option<f64> = row.get::<usize, Option<f64>>(26)?;
        let rt_internal_bilateral_for_market_purchases_impacting_mlrlo: Option<f64> =
            row.get::<usize, Option<f64>>(27)?;
        let rt_internal_bilateral_for_market_sales_impacting_mlrlo: Option<f64> =
            row.get::<usize, Option<f64>>(28)?;
        let marginal_loss_revenue_load_obligation: Option<f64> =
            row.get::<usize, Option<f64>>(29)?;
        let rt_generation_obligation_for_charge_allocation: Option<f64> =
            row.get::<usize, Option<f64>>(30)?;
        let rt_load_obligation_for_charge_allocation: Option<f64> =
            row.get::<usize, Option<f64>>(31)?;
        let rt_adjusted_net_interchange_for_charge_allocation: Option<f64> =
            row.get::<usize, Option<f64>>(32)?;
        let rt_demand_reduction_obligation: Option<f64> = row.get::<usize, Option<f64>>(33)?;
        let rt_load_obligation_for_demand_reduction_allocation: Option<f64> =
            row.get::<usize, Option<f64>>(34)?;
        let demand_reduction_obligation_deviation: Option<f64> =
            row.get::<usize, Option<f64>>(35)?;
        let rt_demand_reduction_credit: Option<f64> = row.get::<usize, Option<f64>>(36)?;
        let rt_demand_reduction_charge: Option<f64> = row.get::<usize, Option<f64>>(37)?;
        let rt_satoa_obligation: Option<f64> = row.get::<usize, Option<f64>>(38)?;
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
            revenue_metered_generation,
            scheduled_imports,
            rt_generation_obligation,
            revenue_metered_load,
            scheduled_exports,
            internal_bilateral_for_load,
            rt_load_obligation,
            rt_internal_bilateral_for_market_purchases,
            rt_internal_bilateral_for_market_sales,
            rt_adjusted_load_obligation,
            rt_adjusted_net_interchange,
            adjusted_net_interchange_deviation,
            rt_energy_component,
            rt_congestion_component,
            rt_marginal_loss_component,
            rt_energy_charge_or_credit,
            rt_congestion_charge_or_credit,
            rt_loss_charge_or_credit,
            rt_internal_bilateral_for_market_purchases_impacting_mlrlo,
            rt_internal_bilateral_for_market_sales_impacting_mlrlo,
            marginal_loss_revenue_load_obligation,
            rt_generation_obligation_for_charge_allocation,
            rt_load_obligation_for_charge_allocation,
            rt_adjusted_net_interchange_for_charge_allocation,
            rt_demand_reduction_obligation,
            rt_load_obligation_for_demand_reduction_allocation,
            demand_reduction_obligation_deviation,
            rt_demand_reduction_credit,
            rt_demand_reduction_charge,
            rt_satoa_obligation,
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
        let conn = Connection::open_with_flags(ProdDb::sr_rtlocsum().duckdb_path, config).unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }
}
