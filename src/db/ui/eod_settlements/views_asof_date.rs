// Auto-generated Rust stub for DuckDB table: views_asof_date
// Created on 2026-04-15 with Dart package reduct

use std::collections::HashMap;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use jiff::{
    civil::Date,
    ToSpan,
};

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
}

/// Delete all existing records for `(user_id, view_name)` and insert the provided records.
pub fn write_records(
    conn: &Connection,
    user_id: &str,
    view_name: &str,
    records: &[Record],
) -> Result<(), Box<dyn std::error::Error>> {
    conn.execute(
        "DELETE FROM views_asof_date WHERE user_id = ? AND view_name = ?",
        duckdb::params![user_id, view_name],
    )?;

    if records.is_empty() {
        return Ok(());
    }

    let mut stmt = conn.prepare(
        "INSERT INTO views_asof_date \
         (user_id, view_name, row_id, source, ice_category, ice_hub, ice_product, \
          endur_curve_name, nodal_contract_name, as_of_date, strip, unit_conversion, label) \
         VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
    )?;

    for record in records {
        stmt.execute(duckdb::params![
            record.user_id,
            record.view_name,
            record.row_id,
            record.source,
            record.ice_category,
            record.ice_hub,
            record.ice_product,
            record.endur_curve_name,
            record.nodal_contract_name,
            record.as_of_date.to_string(),
            record.strip,
            record.unit_conversion,
            record.label,
        ])?;
    }

    Ok(())
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
    use jiff::civil::date;
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

    #[test]
    fn test_insert() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadWrite)?;
        let conn =
            Connection::open_with_flags(ProdDb::ui_eod_settlements_asof_date().duckdb_path, config)
                .unwrap();
        let record = Record {
            user_id: "joe".to_string(),
            view_name: "mass hub".to_string(),
            row_id: 0,
            source: "ice".to_string(),
            ice_category: Some("power".to_string()),
            ice_hub: Some("Nepool MH DA (Daily)".to_string()),
            ice_product: Some("Peak Futures".to_string()),
            endur_curve_name: None,
            nodal_contract_name: None,
            as_of_date: date(2026, 3, 20),
            strip: None,
            unit_conversion: None,
            label: None,
        };
        write_records(
            &conn,
            &record.user_id.clone(),
            &record.view_name.clone(),
            &[record],
        )
        .unwrap();
        conn.close().unwrap();
        Ok(())
    }
}
