//=========================================================
// Auto-generated Rust stub for DuckDB table: total_demand
// Created on 2026-02-11 with Dart package reduct

use std::collections::HashMap;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use jiff::Timestamp;
use jiff::{tz::TimeZone, Zoned};
use rust_decimal::Decimal;

use log::error;
use log::info;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;
use std::process::Command;

use crate::{
    api::isone::_api_isone_core::{deserialize_zoned_assume_ny, serialize_zoned_as_offset},
    interval::month::Month,
};


// 15-minute data for total electricity demand in Quebec from https://electricite-quebec.info/en#.
// This site allows downloading historical data!

pub struct HqTotalDemandArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    pub start_15min: Zoned,
    #[serde(with = "rust_decimal::serde::float")]
    pub value: Decimal,
}

impl HqTotalDemandArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, month: &Month) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &month.start_date().year().to_string()
            + "/demand_"
            + &month.strftime("%Y-%m").to_string()
            + ".json"
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.   
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting HQ total system demand files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS total_demand (
    start_15min TIMESTAMPTZ NOT NULL,
    value DECIMAL(9,2) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT 
       time::TIMESTAMPTZ AS start_15min,
       demand::DECIMAL(9,2) AS value
    FROM (
        SELECT time, demand
        FROM read_json('{}/Raw/{}/demand_{}.json.gz')
    )
    WHERE value IS NOT NULL
    ORDER BY start_15min
;

INSERT INTO total_demand
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM total_demand d
        WHERE
            d.start_15min = t.start_15min AND
            d.value = t.value
    )
) ORDER BY start_15min; 
            "#,
            self.base_dir,
            month.start_date().year(),
            month.strftime("%Y-%m")
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
            error!("Failed to update duckdb for month {}: {}", month, stderr);
        }

        Ok(())
    }

    /// Data is updated on the website every 15 min
    pub fn download_file(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let url = format!(
            "https://electricite-quebec.info/data?start_date={}&end_date={}",
            month.start_date(),
            month.end_date()
        );
        println!("downloading from url: {}", url);
        let resp = reqwest::blocking::get(url).expect("request failed");
        let body = resp.text().expect("body invalid");
        let path = &self.filename(month);
        let dir = Path::new(path).parent().unwrap();
        let _ = fs::create_dir_all(dir);
        let mut out = File::create(path).expect("failed to create file");
        io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");

        // gzip it
        Command::new("gzip")
            .args(["-f", path])
            .current_dir(dir)
            .spawn()
            .unwrap()
            .wait()
            .expect("gzip failed");

        Ok(())
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
    start_15min,
    value
FROM total_demand WHERE 1=1"#,
    );
    if let Some(start_15min) = &query_filter.start_15min {
        query.push_str(&format!(
            "
    AND start_15min = '{}'",
            start_15min.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(start_15min_gte) = &query_filter.start_15min_gte {
        query.push_str(&format!(
            "
    AND start_15min >= '{}'",
            start_15min_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(start_15min_lt) = &query_filter.start_15min_lt {
        query.push_str(&format!(
            "
    AND start_15min < '{}'",
            start_15min_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(value) = &query_filter.value {
        query.push_str(&format!(
            "
    AND value = {}",
            value
        ));
    }
    if let Some(value_in) = &query_filter.value_in {
        query.push_str(&format!(
            "
    AND value IN ({})",
            value_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(value_gte) = &query_filter.value_gte {
        query.push_str(&format!(
            "
    AND value >= {}",
            value_gte
        ));
    }
    if let Some(value_lte) = &query_filter.value_lte {
        query.push_str(&format!(
            "
    AND value <= {}",
            value_lte
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
        let _micros0: i64 = row.get::<usize, i64>(0)?;
        let start_15min = Zoned::new(
            Timestamp::from_microsecond(_micros0).unwrap(),
            TimeZone::get("America/New_York").unwrap(),
        );
        let value: Decimal = match row.get_ref_unwrap(1) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Record { start_15min, value })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub start_15min: Option<Zoned>,
    pub start_15min_gte: Option<Zoned>,
    pub start_15min_lt: Option<Zoned>,
    pub value: Option<Decimal>,
    pub value_in: Option<Vec<Decimal>>,
    pub value_gte: Option<Decimal>,
    pub value_lte: Option<Decimal>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.start_15min {
            params.insert("start_15min", value.to_string());
        }
        if let Some(value) = &self.start_15min_gte {
            params.insert("start_15min_gte", value.to_string());
        }
        if let Some(value) = &self.start_15min_lt {
            params.insert("start_15min_lt", value.to_string());
        }
        if let Some(value) = &self.value {
            params.insert("value", value.to_string());
        }
        if let Some(value) = &self.value_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("value_in", joined);
        }
        if let Some(value) = &self.value_gte {
            params.insert("value_gte", value.to_string());
        }
        if let Some(value) = &self.value_lte {
            params.insert("value_lte", value.to_string());
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

    pub fn start_15min(mut self, value: Zoned) -> Self {
        self.inner.start_15min = Some(value);
        self
    }

    pub fn start_15min_gte(mut self, value: Zoned) -> Self {
        self.inner.start_15min_gte = Some(value);
        self
    }

    pub fn start_15min_lt(mut self, value: Zoned) -> Self {
        self.inner.start_15min_lt = Some(value);
        self
    }

    pub fn value(mut self, value: Decimal) -> Self {
        self.inner.value = Some(value);
        self
    }

    pub fn value_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.value_in = Some(values_in);
        self
    }

    pub fn value_gte(mut self, value: Decimal) -> Self {
        self.inner.value_gte = Some(value);
        self
    }

    pub fn value_lte(mut self, value: Decimal) -> Self {
        self.inner.value_lte = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {

    use std::error::Error;

    use crate::{db::prod_db::ProdDb, interval::month::month};

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        let archive = ProdDb::hq_total_demand();
        let months = month(2024, 5).up_to(month(2026, 2))?;
        for m in months {
            // archive.download_file(&m)?;
            archive.update_duckdb(&m)?;
        }

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::hq_total_demand();
        let months = month(2024, 4).up_to(month(2026, 2))?;
        for m in months {
            archive.download_file(&m)?;
        }
        Ok(())
    }
}
