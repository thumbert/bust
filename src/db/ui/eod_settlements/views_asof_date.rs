// Auto-generated Rust stub for DuckDB table: views_asof_date
// Created on 2026-04-15 with Dart package reduct

use std::collections::HashMap;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use jiff::{civil::Date, ToSpan};

#[derive(Clone)]
pub struct UiEodSettlementsAsOfDateArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub user_id: String,
    pub view_name: String,
    pub row_id: u32,
    pub source: String,
    pub ice_category: Option<String>,
    pub ice_hub: Option<String>,
    pub ice_product: Option<String>,
    pub endur_curve_name: Option<String>,
    pub nodal_contract_name: Option<String>,
    pub as_of_date: Date,
    pub strip: Option<String>,
    pub unit_conversion: Option<String>,
    pub label: Option<String>,
}

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
    limit: Option<usize>,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    user_id,
    view_name,
    row_id,
    source,
    ice_category,
    ice_hub,
    ice_product,
    endur_curve_name,
    nodal_contract_name,
    as_of_date,
    strip,
    unit_conversion,
    label
FROM views_asof_date WHERE 1=1"#,
    );
    if let Some(user_id) = &query_filter.user_id {
        query.push_str(&format!(
            "
    AND user_id = '{}'",
            user_id
        ));
    }
    if let Some(user_id_like) = &query_filter.user_id_like {
        query.push_str(&format!(
            "
    AND user_id LIKE '{}'",
            user_id_like
        ));
    }
    if let Some(user_id_in) = &query_filter.user_id_in {
        query.push_str(&format!(
            "
    AND user_id IN ('{}')",
            user_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(view_name) = &query_filter.view_name {
        query.push_str(&format!(
            "
    AND view_name = '{}'",
            view_name
        ));
    }
    if let Some(view_name_like) = &query_filter.view_name_like {
        query.push_str(&format!(
            "
    AND view_name LIKE '{}'",
            view_name_like
        ));
    }
    if let Some(view_name_in) = &query_filter.view_name_in {
        query.push_str(&format!(
            "
    AND view_name IN ('{}')",
            view_name_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(row_id) = &query_filter.row_id {
        query.push_str(&format!(
            "
    AND row_id = {}",
            row_id
        ));
    }
    if let Some(row_id_in) = &query_filter.row_id_in {
        query.push_str(&format!(
            "
    AND row_id IN ({})",
            row_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(row_id_gte) = &query_filter.row_id_gte {
        query.push_str(&format!(
            "
    AND row_id >= {}",
            row_id_gte
        ));
    }
    if let Some(row_id_lte) = &query_filter.row_id_lte {
        query.push_str(&format!(
            "
    AND row_id <= {}",
            row_id_lte
        ));
    }
    if let Some(source) = &query_filter.source {
        query.push_str(&format!(
            "
    AND source = '{}'",
            source
        ));
    }
    if let Some(source_like) = &query_filter.source_like {
        query.push_str(&format!(
            "
    AND source LIKE '{}'",
            source_like
        ));
    }
    if let Some(source_in) = &query_filter.source_in {
        query.push_str(&format!(
            "
    AND source IN ('{}')",
            source_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(ice_category) = &query_filter.ice_category {
        query.push_str(&format!(
            "
    AND ice_category = '{}'",
            ice_category
        ));
    }
    if let Some(ice_category_like) = &query_filter.ice_category_like {
        query.push_str(&format!(
            "
    AND ice_category LIKE '{}'",
            ice_category_like
        ));
    }
    if let Some(ice_category_in) = &query_filter.ice_category_in {
        query.push_str(&format!(
            "
    AND ice_category IN ('{}')",
            ice_category_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(ice_hub) = &query_filter.ice_hub {
        query.push_str(&format!(
            "
    AND ice_hub = '{}'",
            ice_hub
        ));
    }
    if let Some(ice_hub_like) = &query_filter.ice_hub_like {
        query.push_str(&format!(
            "
    AND ice_hub LIKE '{}'",
            ice_hub_like
        ));
    }
    if let Some(ice_hub_in) = &query_filter.ice_hub_in {
        query.push_str(&format!(
            "
    AND ice_hub IN ('{}')",
            ice_hub_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(ice_product) = &query_filter.ice_product {
        query.push_str(&format!(
            "
    AND ice_product = '{}'",
            ice_product
        ));
    }
    if let Some(ice_product_like) = &query_filter.ice_product_like {
        query.push_str(&format!(
            "
    AND ice_product LIKE '{}'",
            ice_product_like
        ));
    }
    if let Some(ice_product_in) = &query_filter.ice_product_in {
        query.push_str(&format!(
            "
    AND ice_product IN ('{}')",
            ice_product_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(endur_curve_name) = &query_filter.endur_curve_name {
        query.push_str(&format!(
            "
    AND endur_curve_name = '{}'",
            endur_curve_name
        ));
    }
    if let Some(endur_curve_name_like) = &query_filter.endur_curve_name_like {
        query.push_str(&format!(
            "
    AND endur_curve_name LIKE '{}'",
            endur_curve_name_like
        ));
    }
    if let Some(endur_curve_name_in) = &query_filter.endur_curve_name_in {
        query.push_str(&format!(
            "
    AND endur_curve_name IN ('{}')",
            endur_curve_name_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(nodal_contract_name) = &query_filter.nodal_contract_name {
        query.push_str(&format!(
            "
    AND nodal_contract_name = '{}'",
            nodal_contract_name
        ));
    }
    if let Some(nodal_contract_name_like) = &query_filter.nodal_contract_name_like {
        query.push_str(&format!(
            "
    AND nodal_contract_name LIKE '{}'",
            nodal_contract_name_like
        ));
    }
    if let Some(nodal_contract_name_in) = &query_filter.nodal_contract_name_in {
        query.push_str(&format!(
            "
    AND nodal_contract_name IN ('{}')",
            nodal_contract_name_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(as_of_date) = &query_filter.as_of_date {
        query.push_str(&format!(
            "
    AND as_of_date = '{}'",
            as_of_date
        ));
    }
    if let Some(as_of_date_in) = &query_filter.as_of_date_in {
        query.push_str(&format!(
            "
    AND as_of_date IN ('{}')",
            as_of_date_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(as_of_date_gte) = &query_filter.as_of_date_gte {
        query.push_str(&format!(
            "
    AND as_of_date >= '{}'",
            as_of_date_gte
        ));
    }
    if let Some(as_of_date_lte) = &query_filter.as_of_date_lte {
        query.push_str(&format!(
            "
    AND as_of_date <= '{}'",
            as_of_date_lte
        ));
    }
    if let Some(strip) = &query_filter.strip {
        query.push_str(&format!(
            "
    AND strip = '{}'",
            strip
        ));
    }
    if let Some(strip_like) = &query_filter.strip_like {
        query.push_str(&format!(
            "
    AND strip LIKE '{}'",
            strip_like
        ));
    }
    if let Some(strip_in) = &query_filter.strip_in {
        query.push_str(&format!(
            "
    AND strip IN ('{}')",
            strip_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(unit_conversion) = &query_filter.unit_conversion {
        query.push_str(&format!(
            "
    AND unit_conversion = '{}'",
            unit_conversion
        ));
    }
    if let Some(unit_conversion_like) = &query_filter.unit_conversion_like {
        query.push_str(&format!(
            "
    AND unit_conversion LIKE '{}'",
            unit_conversion_like
        ));
    }
    if let Some(unit_conversion_in) = &query_filter.unit_conversion_in {
        query.push_str(&format!(
            "
    AND unit_conversion IN ('{}')",
            unit_conversion_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(label) = &query_filter.label {
        query.push_str(&format!(
            "
    AND label = '{}'",
            label
        ));
    }
    if let Some(label_like) = &query_filter.label_like {
        query.push_str(&format!(
            "
    AND label LIKE '{}'",
            label_like
        ));
    }
    if let Some(label_in) = &query_filter.label_in {
        query.push_str(&format!(
            "
    AND label IN ('{}')",
            label_in
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
        let user_id: String = row.get::<usize, String>(0)?;
        let view_name: String = row.get::<usize, String>(1)?;
        let row_id: u32 = row.get::<usize, u32>(2)?;
        let source: String = row.get::<usize, String>(3)?;
        let ice_category: Option<String> = row.get::<usize, Option<String>>(4)?;
        let ice_hub: Option<String> = row.get::<usize, Option<String>>(5)?;
        let ice_product: Option<String> = row.get::<usize, Option<String>>(6)?;
        let endur_curve_name: Option<String> = row.get::<usize, Option<String>>(7)?;
        let nodal_contract_name: Option<String> = row.get::<usize, Option<String>>(8)?;
        let _n9 = 719528 + row.get::<usize, i32>(9)?;
        let as_of_date = Date::ZERO + _n9.days();
        let strip: Option<String> = row.get::<usize, Option<String>>(10)?;
        let unit_conversion: Option<String> = row.get::<usize, Option<String>>(11)?;
        let label: Option<String> = row.get::<usize, Option<String>>(12)?;
        Ok(Record {
            user_id,
            view_name,
            row_id,
            source,
            ice_category,
            ice_hub,
            ice_product,
            endur_curve_name,
            nodal_contract_name,
            as_of_date,
            strip,
            unit_conversion,
            label,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub user_id: Option<String>,
    pub user_id_like: Option<String>,
    pub user_id_in: Option<Vec<String>>,
    pub view_name: Option<String>,
    pub view_name_like: Option<String>,
    pub view_name_in: Option<Vec<String>>,
    pub row_id: Option<u32>,
    pub row_id_in: Option<Vec<u32>>,
    pub row_id_gte: Option<u32>,
    pub row_id_lte: Option<u32>,
    pub source: Option<String>,
    pub source_like: Option<String>,
    pub source_in: Option<Vec<String>>,
    pub ice_category: Option<String>,
    pub ice_category_like: Option<String>,
    pub ice_category_in: Option<Vec<String>>,
    pub ice_hub: Option<String>,
    pub ice_hub_like: Option<String>,
    pub ice_hub_in: Option<Vec<String>>,
    pub ice_product: Option<String>,
    pub ice_product_like: Option<String>,
    pub ice_product_in: Option<Vec<String>>,
    pub endur_curve_name: Option<String>,
    pub endur_curve_name_like: Option<String>,
    pub endur_curve_name_in: Option<Vec<String>>,
    pub nodal_contract_name: Option<String>,
    pub nodal_contract_name_like: Option<String>,
    pub nodal_contract_name_in: Option<Vec<String>>,
    pub as_of_date: Option<Date>,
    pub as_of_date_in: Option<Vec<Date>>,
    pub as_of_date_gte: Option<Date>,
    pub as_of_date_lte: Option<Date>,
    pub strip: Option<String>,
    pub strip_like: Option<String>,
    pub strip_in: Option<Vec<String>>,
    pub unit_conversion: Option<String>,
    pub unit_conversion_like: Option<String>,
    pub unit_conversion_in: Option<Vec<String>>,
    pub label: Option<String>,
    pub label_like: Option<String>,
    pub label_in: Option<Vec<String>>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.user_id {
            params.insert("user_id", value.to_string());
        }
        if let Some(value) = &self.user_id_like {
            params.insert("user_id_like", value.to_string());
        }
        if let Some(value) = &self.user_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("user_id_in", joined);
        }
        if let Some(value) = &self.view_name {
            params.insert("view_name", value.to_string());
        }
        if let Some(value) = &self.view_name_like {
            params.insert("view_name_like", value.to_string());
        }
        if let Some(value) = &self.view_name_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("view_name_in", joined);
        }
        if let Some(value) = &self.row_id {
            params.insert("row_id", value.to_string());
        }
        if let Some(value) = &self.row_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("row_id_in", joined);
        }
        if let Some(value) = &self.row_id_gte {
            params.insert("row_id_gte", value.to_string());
        }
        if let Some(value) = &self.row_id_lte {
            params.insert("row_id_lte", value.to_string());
        }
        if let Some(value) = &self.source {
            params.insert("source", value.to_string());
        }
        if let Some(value) = &self.source_like {
            params.insert("source_like", value.to_string());
        }
        if let Some(value) = &self.source_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("source_in", joined);
        }
        if let Some(value) = &self.ice_category {
            params.insert("ice_category", value.to_string());
        }
        if let Some(value) = &self.ice_category_like {
            params.insert("ice_category_like", value.to_string());
        }
        if let Some(value) = &self.ice_category_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("ice_category_in", joined);
        }
        if let Some(value) = &self.ice_hub {
            params.insert("ice_hub", value.to_string());
        }
        if let Some(value) = &self.ice_hub_like {
            params.insert("ice_hub_like", value.to_string());
        }
        if let Some(value) = &self.ice_hub_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("ice_hub_in", joined);
        }
        if let Some(value) = &self.ice_product {
            params.insert("ice_product", value.to_string());
        }
        if let Some(value) = &self.ice_product_like {
            params.insert("ice_product_like", value.to_string());
        }
        if let Some(value) = &self.ice_product_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("ice_product_in", joined);
        }
        if let Some(value) = &self.endur_curve_name {
            params.insert("endur_curve_name", value.to_string());
        }
        if let Some(value) = &self.endur_curve_name_like {
            params.insert("endur_curve_name_like", value.to_string());
        }
        if let Some(value) = &self.endur_curve_name_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("endur_curve_name_in", joined);
        }
        if let Some(value) = &self.nodal_contract_name {
            params.insert("nodal_contract_name", value.to_string());
        }
        if let Some(value) = &self.nodal_contract_name_like {
            params.insert("nodal_contract_name_like", value.to_string());
        }
        if let Some(value) = &self.nodal_contract_name_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("nodal_contract_name_in", joined);
        }
        if let Some(value) = &self.as_of_date {
            params.insert("as_of_date", value.to_string());
        }
        if let Some(value) = &self.as_of_date_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("as_of_date_in", joined);
        }
        if let Some(value) = &self.as_of_date_gte {
            params.insert("as_of_date_gte", value.to_string());
        }
        if let Some(value) = &self.as_of_date_lte {
            params.insert("as_of_date_lte", value.to_string());
        }
        if let Some(value) = &self.strip {
            params.insert("strip", value.to_string());
        }
        if let Some(value) = &self.strip_like {
            params.insert("strip_like", value.to_string());
        }
        if let Some(value) = &self.strip_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("strip_in", joined);
        }
        if let Some(value) = &self.unit_conversion {
            params.insert("unit_conversion", value.to_string());
        }
        if let Some(value) = &self.unit_conversion_like {
            params.insert("unit_conversion_like", value.to_string());
        }
        if let Some(value) = &self.unit_conversion_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("unit_conversion_in", joined);
        }
        if let Some(value) = &self.label {
            params.insert("label", value.to_string());
        }
        if let Some(value) = &self.label_like {
            params.insert("label_like", value.to_string());
        }
        if let Some(value) = &self.label_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("label_in", joined);
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

    pub fn user_id<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.user_id = Some(value.into());
        self
    }

    pub fn user_id_like(mut self, value_like: String) -> Self {
        self.inner.user_id_like = Some(value_like);
        self
    }

    pub fn user_id_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.user_id_in = Some(values_in);
        self
    }

    pub fn view_name<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.view_name = Some(value.into());
        self
    }

    pub fn view_name_like(mut self, value_like: String) -> Self {
        self.inner.view_name_like = Some(value_like);
        self
    }

    pub fn view_name_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.view_name_in = Some(values_in);
        self
    }

    pub fn row_id(mut self, value: u32) -> Self {
        self.inner.row_id = Some(value);
        self
    }

    pub fn row_id_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.row_id_in = Some(values_in);
        self
    }

    pub fn row_id_gte(mut self, value: u32) -> Self {
        self.inner.row_id_gte = Some(value);
        self
    }

    pub fn row_id_lte(mut self, value: u32) -> Self {
        self.inner.row_id_lte = Some(value);
        self
    }

    pub fn source<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.source = Some(value.into());
        self
    }

    pub fn source_like(mut self, value_like: String) -> Self {
        self.inner.source_like = Some(value_like);
        self
    }

    pub fn source_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.source_in = Some(values_in);
        self
    }

    pub fn ice_category<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.ice_category = Some(value.into());
        self
    }

    pub fn ice_category_like(mut self, value_like: String) -> Self {
        self.inner.ice_category_like = Some(value_like);
        self
    }

    pub fn ice_category_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.ice_category_in = Some(values_in);
        self
    }

    pub fn ice_hub<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.ice_hub = Some(value.into());
        self
    }

    pub fn ice_hub_like(mut self, value_like: String) -> Self {
        self.inner.ice_hub_like = Some(value_like);
        self
    }

    pub fn ice_hub_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.ice_hub_in = Some(values_in);
        self
    }

    pub fn ice_product<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.ice_product = Some(value.into());
        self
    }

    pub fn ice_product_like(mut self, value_like: String) -> Self {
        self.inner.ice_product_like = Some(value_like);
        self
    }

    pub fn ice_product_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.ice_product_in = Some(values_in);
        self
    }

    pub fn endur_curve_name<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.endur_curve_name = Some(value.into());
        self
    }

    pub fn endur_curve_name_like(mut self, value_like: String) -> Self {
        self.inner.endur_curve_name_like = Some(value_like);
        self
    }

    pub fn endur_curve_name_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.endur_curve_name_in = Some(values_in);
        self
    }

    pub fn nodal_contract_name<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.nodal_contract_name = Some(value.into());
        self
    }

    pub fn nodal_contract_name_like(mut self, value_like: String) -> Self {
        self.inner.nodal_contract_name_like = Some(value_like);
        self
    }

    pub fn nodal_contract_name_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.nodal_contract_name_in = Some(values_in);
        self
    }

    pub fn as_of_date(mut self, value: Date) -> Self {
        self.inner.as_of_date = Some(value);
        self
    }

    pub fn as_of_date_in(mut self, values_in: Vec<Date>) -> Self {
        self.inner.as_of_date_in = Some(values_in);
        self
    }

    pub fn as_of_date_gte(mut self, value: Date) -> Self {
        self.inner.as_of_date_gte = Some(value);
        self
    }

    pub fn as_of_date_lte(mut self, value: Date) -> Self {
        self.inner.as_of_date_lte = Some(value);
        self
    }

    pub fn strip<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.strip = Some(value.into());
        self
    }

    pub fn strip_like(mut self, value_like: String) -> Self {
        self.inner.strip_like = Some(value_like);
        self
    }

    pub fn strip_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.strip_in = Some(values_in);
        self
    }

    pub fn unit_conversion<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.unit_conversion = Some(value.into());
        self
    }

    pub fn unit_conversion_like(mut self, value_like: String) -> Self {
        self.inner.unit_conversion_like = Some(value_like);
        self
    }

    pub fn unit_conversion_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.unit_conversion_in = Some(values_in);
        self
    }

    pub fn label<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.label = Some(value.into());
        self
    }

    pub fn label_like(mut self, value_like: String) -> Self {
        self.inner.label_like = Some(value_like);
        self
    }

    pub fn label_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.label_in = Some(values_in);
        self
    }
}

pub fn users_views(conn: &Connection) -> Result<Vec<(String, String)>, Box<dyn std::error::Error>> {
    let query = String::from(
        r#"
SELECT DISTINCT user_id, view_name
FROM views_asof_date
ORDER BY user_id, view_name;
"#,
    );

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let user_id: String = row.get::<usize, String>(0)?;
        let view_name: String = row.get::<usize, String>(1)?;
        Ok((user_id, view_name))
    })?;
    let results: Vec<(String, String)> = rows.collect::<Result<_, _>>()?;
    Ok(results)
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
        let conn =
            Connection::open_with_flags(ProdDb::ui_eod_settlements_asof_date().duckdb_path, config)
                .unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }

    #[test]
    fn test_get_users_views() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::ui_eod_settlements_asof_date().duckdb_path, config)
                .unwrap();
        let xs: Vec<(String, String)> = users_views(&conn).unwrap();
        conn.close().unwrap();
        assert!(!xs.is_empty());
        let users: Vec<String> = xs.iter().map(|(user, _)| user.clone()).collect();
        assert!(users.contains(&"adrian".to_string()));
        Ok(())
    }
}
