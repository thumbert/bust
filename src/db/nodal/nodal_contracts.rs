// Auto-generated Rust stub for DuckDB table: contracts
// Created on 2026-05-01 with Dart package reduct

use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use duckdb::Connection;
use url::form_urlencoded;

use rust_decimal::Decimal;

#[derive(Clone)]
pub struct NodalContractsArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub physical_commodity_code: String,
    pub contract_long_name: String,
    pub contract_short_name: String,
    pub product_type: String,
    pub product_group: String,
    pub settlement_type: String,
    pub lot_limit_group: String,
    pub group_commodity_code: String,
    pub count_of_expiries: i32,
    #[serde(with = "rust_decimal::serde::float")]
    pub block_exchange_fee: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub screen_exchange_fee: Decimal,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub efp_exchange_fee: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::float")]
    pub clearing_fee: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub settlement_or_option_exercise_assignment_fee: Decimal,
    pub gmi_exch: String,
    pub gmi_fc: String,
    pub description: String,
    pub reporting_level: Option<String>,
    pub spot_month_position_limit_lots: i32,
    pub single_month_accountability_level_lots: i32,
    pub all_month_accountability_level_lots: i32,
    pub aggregation_group: Option<i32>,
    pub aggregation_group_type: Option<String>,
    pub parent_contract_flag: Option<bool>,
    pub cftc_referenced_contract: bool,
}

pub fn get_data(conn: &Connection, query_filter: &QueryFilter, limit: Option<usize>) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
   let mut query = String::from(r#"
SELECT
    physical_commodity_code,
    contract_long_name,
    contract_short_name,
    product_type,
    product_group,
    settlement_type,
    lot_limit_group,
    group_commodity_code,
    count_of_expiries,
    block_exchange_fee,
    screen_exchange_fee,
    efp_exchange_fee,
    clearing_fee,
    settlement_or_option_exercise_assignment_fee,
    gmi_exch,
    gmi_fc,
    description,
    reporting_level,
    spot_month_position_limit_lots,
    single_month_accountability_level_lots,
    all_month_accountability_level_lots,
    aggregation_group,
    aggregation_group_type,
    parent_contract_flag,
    cftc_referenced_contract
FROM contracts WHERE 1=1"#);
    if let Some(product_group) = &query_filter.product_group {
        query.push_str(&format!("
    AND product_group = '{}'", product_group));
    }
    if let Some(product_group_like) = &query_filter.product_group_like {
        query.push_str(&format!("
    AND product_group LIKE '{}'", product_group_like));
    }
    if let Some(product_group_in) = &query_filter.product_group_in {
        query.push_str(&format!("
    AND product_group IN ('{}')", product_group_in.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("','")));
    }
    match limit {
        Some(l) => {
            query.push_str(&format!("
LIMIT {};", l));
        },
        None => {
            query.push(';');
        },
    }

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let physical_commodity_code: String = row.get::<usize, String>(0)?;
        let contract_long_name: String = row.get::<usize, String>(1)?;
        let contract_short_name: String = row.get::<usize, String>(2)?;
        let product_type: String = row.get::<usize, String>(3)?;
        let product_group: String = row.get::<usize, String>(4)?;
        let settlement_type: String = row.get::<usize, String>(5)?;
        let lot_limit_group: String = row.get::<usize, String>(6)?;
        let group_commodity_code: String = row.get::<usize, String>(7)?;
        let count_of_expiries: i32 = row.get::<usize, i32>(8)?;
        let block_exchange_fee: Decimal = match row.get_ref_unwrap(9) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let screen_exchange_fee: Decimal = match row.get_ref_unwrap(10) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let efp_exchange_fee: Option<Decimal> = match row.get_ref_unwrap(11) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let clearing_fee: Decimal = match row.get_ref_unwrap(12) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let settlement_or_option_exercise_assignment_fee: Decimal = match row.get_ref_unwrap(13) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let gmi_exch: String = row.get::<usize, String>(14)?;
        let gmi_fc: String = row.get::<usize, String>(15)?;
        let description: String = row.get::<usize, String>(16)?;
        let reporting_level: Option<String> = row.get::<usize, Option<String>>(17)?;
        let spot_month_position_limit_lots: i32 = row.get::<usize, i32>(18)?;
        let single_month_accountability_level_lots: i32 = row.get::<usize, i32>(19)?;
        let all_month_accountability_level_lots: i32 = row.get::<usize, i32>(20)?;
        let aggregation_group: Option<i32> = row.get::<usize, Option<i32>>(21)?;
        let aggregation_group_type: Option<String> = row.get::<usize, Option<String>>(22)?;
        let parent_contract_flag: Option<bool> = row.get::<usize, Option<bool>>(23)?;
        let cftc_referenced_contract: bool = row.get::<usize, bool>(24)?;
        Ok(Record {
            physical_commodity_code,
            contract_long_name,
            contract_short_name,
            product_type,
            product_group,
            settlement_type,
            lot_limit_group,
            group_commodity_code,
            count_of_expiries,
            block_exchange_fee,
            screen_exchange_fee,
            efp_exchange_fee,
            clearing_fee,
            settlement_or_option_exercise_assignment_fee,
            gmi_exch,
            gmi_fc,
            description,
            reporting_level,
            spot_month_position_limit_lots,
            single_month_accountability_level_lots,
            all_month_accountability_level_lots,
            aggregation_group,
            aggregation_group_type,
            parent_contract_flag,
            cftc_referenced_contract,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub product_group: Option<String>,
    pub product_group_like: Option<String>,
    pub product_group_in: Option<Vec<String>>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.product_group {
            params.insert("product_group", value.to_string());
        }
        if let Some(value) = &self.product_group_like {
            params.insert("product_group_like", value.to_string());
        }
        if let Some(value) = &self.product_group_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("product_group_in", joined);
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

    pub fn product_group<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.product_group = Some(value.into());
        self
    }

    pub fn product_group_like(mut self, value_like: String) -> Self {
        self.inner.product_group_like = Some(value_like);
        self
    }

    pub fn product_group_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.product_group_in = Some(values_in);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use duckdb::{AccessMode, Config, Connection};
    use crate::db::prod_db::ProdDb;
    use super::*;

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::nodal_contracts().duckdb_path, config).unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }
}
