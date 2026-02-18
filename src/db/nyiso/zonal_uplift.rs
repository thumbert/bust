// Auto-generated Rust stub for DuckDB table: zonal_uplift
// Created on 2026-02-18 with Dart package reduct

use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use duckdb::Connection;
use jiff::civil::Date;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use jiff::ToSpan;
use log::{error, info};
use rust_decimal::Decimal;
use std::process::Command;

use crate::db::isone::lib_isoexpress::download_file;
use crate::interval::month::Month;

#[derive(Clone)]
pub struct NyisoZonalUpliftArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl NyisoZonalUpliftArchive {
    /// Return the file path of the csv file with data for one day
    pub fn filename(&self, month: &Month) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + month.year().to_string().as_str()
            + "/"
            + &month.strftime("%Y-%m").to_string()
            + "_zonal_uplift.csv"
    }

    /// Data is published on the 15th of every month for the previous month.
    /// See https://mis.nyiso.com/public/csv/ZonalUplift/20251215zonaluplift.csv
    pub fn download_file(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let url = format!(
            "https://mis.nyiso.com/public/csv/ZonalUplift/{}15zonaluplift.csv",
            month.start_date().strftime("%Y%m")
        );
        download_file(url, false, None, Path::new(&self.filename(month)), true)
    }

    /// Update duckdb with published data for the month.  No checks are made to see
    /// if there are missing files.  Does not delete any existing data.  So if data
    /// is wrong for some reason, it needs to be manually deleted first!
    ///
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!("inserting NYISO zonal uplift data for {} ...", month);
        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS zonal_uplift (
    day DATE NOT NULL,
    ptid VARCHAR NOT NULL,
    name VARCHAR NOT NULL,
    uplift_category VARCHAR NOT NULL,
    uplift_payment DECIMAL(18,2) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS (
    SELECT 
        strptime("Market Day", '%m/%d/%Y')::DATE AS day,
        PTID::VARCHAR AS ptid,
        "Name"::VARCHAR AS name,
        "Uplift Payment Category"::VARCHAR AS uplift_category,
        "Uplift Payment Amount"::DECIMAL(18,2) AS uplift_payment
    FROM read_csv('{}/Raw/{}/{}_zonal_uplift.csv.gz', 
        header = true)
    WHERE uplift_payment <> 0    
);

INSERT INTO zonal_uplift
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM zonal_uplift d
        WHERE
            d.day = t.day AND
            d.ptid = t.ptid AND
            d.name = t.name AND
            d.uplift_category = t.uplift_category AND
            d.uplift_payment = t.uplift_payment
    )
);
        "#,
            self.base_dir,
            month.start_date().year(),
            &month.start_date().strftime("%Y-%m"),
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
            error!("Failed to update duckdb for month {}: {}", month, stderr);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub day: Date,
    pub ptid: String,
    pub name: String,
    pub uplift_category: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub uplift_payment: Decimal,
}

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
    limit: Option<usize>,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    day,
    ptid,
    name,
    uplift_category,
    uplift_payment
FROM zonal_uplift WHERE 1=1"#,
    );
    if let Some(day) = &query_filter.day {
        query.push_str(&format!(
            "
    AND day = '{}'",
            day
        ));
    }
    if let Some(day_in) = &query_filter.day_in {
        query.push_str(&format!(
            "
    AND day IN ('{}')",
            day_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(day_gte) = &query_filter.day_gte {
        query.push_str(&format!(
            "
    AND day >= '{}'",
            day_gte
        ));
    }
    if let Some(day_lte) = &query_filter.day_lte {
        query.push_str(&format!(
            "
    AND day <= '{}'",
            day_lte
        ));
    }
    if let Some(ptid) = &query_filter.ptid {
        query.push_str(&format!(
            "
    AND ptid = '{}'",
            ptid
        ));
    }
    if let Some(ptid_like) = &query_filter.ptid_like {
        query.push_str(&format!(
            "
    AND ptid LIKE '{}'",
            ptid_like
        ));
    }
    if let Some(ptid_in) = &query_filter.ptid_in {
        query.push_str(&format!(
            "
    AND ptid IN ('{}')",
            ptid_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(name) = &query_filter.name {
        query.push_str(&format!(
            "
    AND name = '{}'",
            name
        ));
    }
    if let Some(name_like) = &query_filter.name_like {
        query.push_str(&format!(
            "
    AND name LIKE '{}'",
            name_like
        ));
    }
    if let Some(name_in) = &query_filter.name_in {
        query.push_str(&format!(
            "
    AND name IN ('{}')",
            name_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(uplift_category) = &query_filter.uplift_category {
        query.push_str(&format!(
            "
    AND uplift_category = '{}'",
            uplift_category
        ));
    }
    if let Some(uplift_category_like) = &query_filter.uplift_category_like {
        query.push_str(&format!(
            "
    AND uplift_category LIKE '{}'",
            uplift_category_like
        ));
    }
    if let Some(uplift_category_in) = &query_filter.uplift_category_in {
        query.push_str(&format!(
            "
    AND uplift_category IN ('{}')",
            uplift_category_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(uplift_payment) = &query_filter.uplift_payment {
        query.push_str(&format!(
            "
    AND uplift_payment = {}",
            uplift_payment
        ));
    }
    if let Some(uplift_payment_in) = &query_filter.uplift_payment_in {
        query.push_str(&format!(
            "
    AND uplift_payment IN ({})",
            uplift_payment_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(uplift_payment_gte) = &query_filter.uplift_payment_gte {
        query.push_str(&format!(
            "
    AND uplift_payment >= {}",
            uplift_payment_gte
        ));
    }
    if let Some(uplift_payment_lte) = &query_filter.uplift_payment_lte {
        query.push_str(&format!(
            "
    AND uplift_payment <= {}",
            uplift_payment_lte
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
        let day = Date::ZERO + _n0.days();
        let ptid: String = row.get::<usize, String>(1)?;
        let name: String = row.get::<usize, String>(2)?;
        let uplift_category: String = row.get::<usize, String>(3)?;
        let uplift_payment: Decimal = match row.get_ref_unwrap(4) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Record {
            day,
            ptid,
            name,
            uplift_category,
            uplift_payment,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub day: Option<Date>,
    pub day_in: Option<Vec<Date>>,
    pub day_gte: Option<Date>,
    pub day_lte: Option<Date>,
    pub ptid: Option<String>,
    pub ptid_like: Option<String>,
    pub ptid_in: Option<Vec<String>>,
    pub name: Option<String>,
    pub name_like: Option<String>,
    pub name_in: Option<Vec<String>>,
    pub uplift_category: Option<String>,
    pub uplift_category_like: Option<String>,
    pub uplift_category_in: Option<Vec<String>>,
    pub uplift_payment: Option<Decimal>,
    pub uplift_payment_in: Option<Vec<Decimal>>,
    pub uplift_payment_gte: Option<Decimal>,
    pub uplift_payment_lte: Option<Decimal>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.day {
            params.insert("day", value.to_string());
        }
        if let Some(value) = &self.day_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("day_in", joined);
        }
        if let Some(value) = &self.day_gte {
            params.insert("day_gte", value.to_string());
        }
        if let Some(value) = &self.day_lte {
            params.insert("day_lte", value.to_string());
        }
        if let Some(value) = &self.ptid {
            params.insert("ptid", value.to_string());
        }
        if let Some(value) = &self.ptid_like {
            params.insert("ptid_like", value.to_string());
        }
        if let Some(value) = &self.ptid_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("ptid_in", joined);
        }
        if let Some(value) = &self.name {
            params.insert("name", value.to_string());
        }
        if let Some(value) = &self.name_like {
            params.insert("name_like", value.to_string());
        }
        if let Some(value) = &self.name_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("name_in", joined);
        }
        if let Some(value) = &self.uplift_category {
            params.insert("uplift_category", value.to_string());
        }
        if let Some(value) = &self.uplift_category_like {
            params.insert("uplift_category_like", value.to_string());
        }
        if let Some(value) = &self.uplift_category_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("uplift_category_in", joined);
        }
        if let Some(value) = &self.uplift_payment {
            params.insert("uplift_payment", value.to_string());
        }
        if let Some(value) = &self.uplift_payment_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("uplift_payment_in", joined);
        }
        if let Some(value) = &self.uplift_payment_gte {
            params.insert("uplift_payment_gte", value.to_string());
        }
        if let Some(value) = &self.uplift_payment_lte {
            params.insert("uplift_payment_lte", value.to_string());
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

    pub fn day(mut self, value: Date) -> Self {
        self.inner.day = Some(value);
        self
    }

    pub fn day_in(mut self, values_in: Vec<Date>) -> Self {
        self.inner.day_in = Some(values_in);
        self
    }

    pub fn day_gte(mut self, value: Date) -> Self {
        self.inner.day_gte = Some(value);
        self
    }

    pub fn day_lte(mut self, value: Date) -> Self {
        self.inner.day_lte = Some(value);
        self
    }

    pub fn ptid<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.ptid = Some(value.into());
        self
    }

    pub fn ptid_like(mut self, value_like: String) -> Self {
        self.inner.ptid_like = Some(value_like);
        self
    }

    pub fn ptid_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.ptid_in = Some(values_in);
        self
    }

    pub fn name<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.name = Some(value.into());
        self
    }

    pub fn name_like(mut self, value_like: String) -> Self {
        self.inner.name_like = Some(value_like);
        self
    }

    pub fn name_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.name_in = Some(values_in);
        self
    }

    pub fn uplift_category<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.uplift_category = Some(value.into());
        self
    }

    pub fn uplift_category_like(mut self, value_like: String) -> Self {
        self.inner.uplift_category_like = Some(value_like);
        self
    }

    pub fn uplift_category_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.uplift_category_in = Some(values_in);
        self
    }

    pub fn uplift_payment(mut self, value: Decimal) -> Self {
        self.inner.uplift_payment = Some(value);
        self
    }

    pub fn uplift_payment_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.uplift_payment_in = Some(values_in);
        self
    }

    pub fn uplift_payment_gte(mut self, value: Decimal) -> Self {
        self.inner.uplift_payment_gte = Some(value);
        self
    }

    pub fn uplift_payment_lte(mut self, value: Decimal) -> Self {
        self.inner.uplift_payment_lte = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use duckdb::{AccessMode, Config, Connection};
    use jiff::civil::date;
    use rust_decimal_macros::dec;
    use std::error::Error;

    use crate::{db::prod_db::ProdDb, interval::month::month};

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::nyiso_zonal_uplift().duckdb_path, config).unwrap();
        let filter = QueryFilterBuilder::new()
            .day_gte(date(2026, 1, 1))
            .day_lte(date(2026, 1, 31))
            .uplift_payment_gte(dec!(1_000_000))
            .build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        let archive = ProdDb::nyiso_zonal_uplift();
        let months = month(2019, 1).up_to(month(2026, 1))?;
        for m in months {
            // archive.download_file(&m)?;
            archive.update_duckdb(&m)?;
        }

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nyiso_zonal_uplift();
        let months = month(2024, 1).up_to(month(2026, 1))?;
        for m in months {
            archive.download_file(&m)?;
        }
        Ok(())
    }
}
