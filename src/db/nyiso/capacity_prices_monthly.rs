// Auto-generated Rust stub for DuckDB table: capacity_prices_monthly
// Created on 2026-07-06 with Dart package reduct

use std::error::Error;
use std::{collections::HashMap, process::Command};

use duckdb::Connection;
use log::{error, info};
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use rust_decimal::Decimal;

use crate::interval::month::Month;

#[derive(Clone)]
pub struct NyisoCapacityPricesMonthlyArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl NyisoCapacityPricesMonthlyArchive {
    /// Return the file path of the csv file with data for one day
    pub fn filename(&self, month: &Month) -> String {
        self.base_dir.to_owned()
            + "/CSV/"
            + &month.strftime("%Y").to_string()
            + "/capacity_prices_"
            + &month.strftime("%Y-%m").to_string()
            + ".csv.gz"
    }

    /// Update duckdb with published data for the month.  No checks are made to see
    /// if there are missing files.  Does not delete any existing data.  So if data
    /// is wrong for some reason, it needs to be manually deleted first!
    ///
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!("inserting monthly capacity prices for {} ...", month);
        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS capacity_prices_monthly (
    capability_period VARCHAR NOT NULL,
    auction_month VARCHAR NOT NULL,
    forward_month VARCHAR NOT NULL,
    location VARCHAR NOT NULL,
    clearing_price DECIMAL(9,4) NOT NULL,
    awarded_mw DECIMAL(9,4) NOT NULL
);

CREATE TEMPORARY TABLE tmp
AS (
    SELECT * 
    FROM read_csv('{}/CSV/{}/capacity_prices_{}.csv.gz')
);

INSERT INTO capacity_prices_monthly
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM capacity_prices_monthly d
        WHERE
            d.capability_period = t.capability_period AND
            d.auction_month = t.auction_month AND
            d.forward_month = t.forward_month AND
            d.location = t.location
    )
);
        "#,
            self.base_dir,
            month.strftime("%Y"),
            month.strftime("%Y-%m"),
        );
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
            error!("Failed to update duckdb for {}: {}", month, stderr);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub capability_period: String,
    pub auction_month: String,
    pub forward_month: String,
    pub location: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub clearing_price: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub awarded_mw: Decimal,
}

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
    limit: Option<usize>,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    capability_period,
    auction_month,
    forward_month,
    location,
    clearing_price,
    awarded_mw
FROM capacity_prices_monthly WHERE 1=1"#,
    );
    if let Some(capability_period) = &query_filter.capability_period {
        query.push_str(&format!(
            "
    AND capability_period = '{}'",
            capability_period
        ));
    }
    if let Some(capability_period_like) = &query_filter.capability_period_like {
        query.push_str(&format!(
            "
    AND capability_period LIKE '{}'",
            capability_period_like
        ));
    }
    if let Some(capability_period_in) = &query_filter.capability_period_in {
        query.push_str(&format!(
            "
    AND capability_period IN ('{}')",
            capability_period_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(location) = &query_filter.location {
        query.push_str(&format!(
            "
    AND location = '{}'",
            location
        ));
    }
    if let Some(location_like) = &query_filter.location_like {
        query.push_str(&format!(
            "
    AND location LIKE '{}'",
            location_like
        ));
    }
    if let Some(location_in) = &query_filter.location_in {
        query.push_str(&format!(
            "
    AND location IN ('{}')",
            location_in
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
        let capability_period: String = row.get::<usize, String>(0)?;
        let auction_month: String = row.get::<usize, String>(1)?;
        let forward_month: String = row.get::<usize, String>(2)?;
        let location: String = row.get::<usize, String>(3)?;
        let clearing_price: Decimal = match row.get_ref_unwrap(4) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let awarded_mw: Decimal = match row.get_ref_unwrap(5) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Record {
            capability_period,
            auction_month,
            forward_month,
            location,
            clearing_price,
            awarded_mw,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub capability_period: Option<String>,
    pub capability_period_like: Option<String>,
    pub capability_period_in: Option<Vec<String>>,
    pub location: Option<String>,
    pub location_like: Option<String>,
    pub location_in: Option<Vec<String>>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.capability_period {
            params.insert("capability_period", value.to_string());
        }
        if let Some(value) = &self.capability_period_like {
            params.insert("capability_period_like", value.to_string());
        }
        if let Some(value) = &self.capability_period_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("capability_period_in", joined);
        }
        if let Some(value) = &self.location {
            params.insert("location", value.to_string());
        }
        if let Some(value) = &self.location_like {
            params.insert("location_like", value.to_string());
        }
        if let Some(value) = &self.location_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("location_in", joined);
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

    pub fn capability_period<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.capability_period = Some(value.into());
        self
    }

    pub fn capability_period_like(mut self, value_like: String) -> Self {
        self.inner.capability_period_like = Some(value_like);
        self
    }

    pub fn capability_period_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.capability_period_in = Some(values_in);
        self
    }

    pub fn location<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.location = Some(value.into());
        self
    }

    pub fn location_like(mut self, value_like: String) -> Self {
        self.inner.location_like = Some(value_like);
        self
    }

    pub fn location_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.location_in = Some(values_in);
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
        let conn = Connection::open_with_flags(
            ProdDb::nyiso_capacity_prices_monthly().duckdb_path,
            config,
        )
        .unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }
}
