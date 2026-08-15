// Auto-generated Rust stub for DuckDB table: ttc_limits
// Created on 2026-08-15 with Dart package reduct

use std::collections::HashMap;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use crate::utils::serde_helpers::*;
use convert_case::{Case, Casing};
use jiff::Timestamp;
use jiff::{tz::TimeZone, Zoned};
use std::str::FromStr;

use itertools::Itertools;
use jiff::civil::*;
use log::error;
use log::info;
use std::error::Error;
use std::fs;
use std::path::Path;
use std::process::Command;

use crate::interval::month::Month;
use scraper::{Html, Selector};

//
pub struct IsoneTtcArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IsoneTtcArchive {
    /// Path to the CSV file with the ISO report for a given day.
    /// ISO doesn't publish this data as part of their webservices API.
    /// https://webservices.iso-ne.com/api/v1.1/totaltransfercapability/day/20250101
    pub fn filename(&self, date: Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/ttc_"
            + &date.strftime("%Y%m%d").to_string()
            + ".csv"
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!("inserting ISONE TTC daily files for month {} ...", month);

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS ttc_limits (
    hour_beginning TIMESTAMPTZ NOT NULL,
    interface_name VARCHAR NOT NULL,
    flow_direction ENUM('import', 'export') NOT NULL,
    mw int64 NOT NULL,
    PRIMARY KEY (hour_beginning, interface_name, flow_direction)
);

CREATE TEMPORARY TABLE tmp AS
WITH source AS (
    SELECT
        *,
        row_number() OVER (PARTITION BY Day) - 1 AS hour_index
    FROM read_csv(
        '{}/Raw/{}/ttc_{}*.csv.gz',
        header = true,
        skip = 4,
        delim = ',',
        quote = '"',
        escape = '"',
        ignore_errors = true,
        all_varchar = true
    )
    WHERE H = 'D'
), long_limits AS (
    UNPIVOT source
    ON COLUMNS(* EXCLUDE (H, Day, "Hour Ending", hour_index))
    INTO NAME limit_name VALUE mw
)
SELECT
    (strptime(Day, '%m/%d/%Y') AT TIME ZONE 'America/New_York')
        + hour_index * INTERVAL '1 hour'
        AS hour_beginning,
    trim(regexp_replace(limit_name, '\s+(Import|Export) Limit MW\s*$', ''))
        AS interface_name,
    lower(regexp_extract(limit_name, '(Import|Export) Limit MW\s*$', 1))
        AS flow_direction,
    CAST(mw AS BIGINT) AS mw
FROM long_limits;

--- tmp values overwrite existing values in ttc_limits table (upsert)
INSERT INTO ttc_limits
SELECT t.*
FROM tmp t
ORDER BY hour_beginning, interface_name, flow_direction
ON CONFLICT (hour_beginning, interface_name, flow_direction)
DO UPDATE SET mw = EXCLUDED.mw;
"#,
            self.base_dir,
            month.start_date().year(),
            month.start_date().strftime("%Y%m")
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

    pub fn download_days(&self, days: Vec<Date>) -> Result<(), Box<dyn Error>> {
        if days.first().unwrap().year() != days.last().unwrap().year() {
            return Err("All days must be in the same year".into());
        }
        let dir = format!("{}/Raw/{}", self.base_dir, days.first().unwrap().year());
        let _ = fs::create_dir_all(&dir);

        let mut out = Command::new("python3")
            .args([
                &format!(
                    "{}/bin/python/isone_ttc_download.py",
                    std::env::var("DIR_ELEC_SERVER").unwrap()
                ),
                &format!(
                    "--days={}",
                    days.iter().map(|e| e.strftime("%Y%m%d")).join(",")
                ),
            ])
            .current_dir(&dir)
            .stdout(std::process::Stdio::inherit())
            .spawn()
            .expect("downloads failed");
        let _ = out.wait();
        Ok(())
    }

    /// Check if the files for some days are missing, and download them.
    pub fn download_missing_days(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let days = month.days();
        let mut missing_days: Vec<Date> = Vec::new();
        for day in days {
            let file = self.filename(day);
            if !Path::new(&file).exists() {
                missing_days.push(day);
            }
        }
        if !missing_days.is_empty() {
            self.download_days(missing_days)?;
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    pub hour_beginning: Zoned,
    pub interface_name: String,
    pub flow_direction: FlowDirection,
    pub mw: i64,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum FlowDirection {
    Export,
    Import,
}

impl std::str::FromStr for FlowDirection {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "EXPORT" => Ok(FlowDirection::Export),
            "IMPORT" => Ok(FlowDirection::Import),
            _ => Err(format!("Invalid value for FlowDirection: {}", s)),
        }
    }
}

impl std::fmt::Display for FlowDirection {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            FlowDirection::Export => write!(f, "export"),
            FlowDirection::Import => write!(f, "import"),
        }
    }
}

impl serde::Serialize for FlowDirection {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            FlowDirection::Export => "export",
            FlowDirection::Import => "import",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for FlowDirection {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        FlowDirection::from_str(&s).map_err(serde::de::Error::custom)
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
    hour_beginning,
    interface_name,
    flow_direction,
    mw
FROM ttc_limits WHERE 1=1"#,
    );
    if let Some(hour_beginning) = &query_filter.hour_beginning {
        query.push_str(&format!(
            "
    AND hour_beginning = '{}'",
            hour_beginning.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(hour_beginning_gte) = &query_filter.hour_beginning_gte {
        query.push_str(&format!(
            "
    AND hour_beginning >= '{}'",
            hour_beginning_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(hour_beginning_lt) = &query_filter.hour_beginning_lt {
        query.push_str(&format!(
            "
    AND hour_beginning < '{}'",
            hour_beginning_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(interface_name) = &query_filter.interface_name {
        query.push_str(&format!(
            "
    AND interface_name = '{}'",
            interface_name
        ));
    }
    if let Some(interface_name_like) = &query_filter.interface_name_like {
        query.push_str(&format!(
            "
    AND interface_name LIKE '{}'",
            interface_name_like
        ));
    }
    if let Some(interface_name_in) = &query_filter.interface_name_in {
        query.push_str(&format!(
            "
    AND interface_name IN ('{}')",
            interface_name_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(flow_direction) = &query_filter.flow_direction {
        query.push_str(&format!(
            "
    AND flow_direction = '{}'",
            flow_direction
        ));
    }
    if let Some(flow_direction_in) = &query_filter.flow_direction_in {
        query.push_str(&format!(
            "
    AND flow_direction IN ('{}')",
            flow_direction_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(mw) = &query_filter.mw {
        query.push_str(&format!(
            "
    AND mw = {}",
            mw
        ));
    }
    if let Some(mw_in) = &query_filter.mw_in {
        query.push_str(&format!(
            "
    AND mw IN ({})",
            mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(mw_gte) = &query_filter.mw_gte {
        query.push_str(&format!(
            "
    AND mw >= {}",
            mw_gte
        ));
    }
    if let Some(mw_lte) = &query_filter.mw_lte {
        query.push_str(&format!(
            "
    AND mw <= {}",
            mw_lte
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
        let hour_beginning = Zoned::new(
            Timestamp::from_microsecond(_micros0).unwrap(),
            TimeZone::get("America/New_York").unwrap(),
        );
        let interface_name: String = row.get::<usize, String>(1)?;
        let _n2 = match row.get_ref_unwrap(2).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum flow_direction"),
        };
        let flow_direction = FlowDirection::from_str(&_n2).unwrap();
        let mw: i64 = row.get::<usize, i64>(3)?;
        Ok(Record {
            hour_beginning,
            interface_name,
            flow_direction,
            mw,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub hour_beginning: Option<Zoned>,
    pub hour_beginning_gte: Option<Zoned>,
    pub hour_beginning_lt: Option<Zoned>,
    pub interface_name: Option<String>,
    pub interface_name_like: Option<String>,
    pub interface_name_in: Option<Vec<String>>,
    pub flow_direction: Option<FlowDirection>,
    pub flow_direction_in: Option<Vec<FlowDirection>>,
    pub mw: Option<i64>,
    pub mw_in: Option<Vec<i64>>,
    pub mw_gte: Option<i64>,
    pub mw_lte: Option<i64>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.hour_beginning {
            params.insert("hour_beginning", value.to_string());
        }
        if let Some(value) = &self.hour_beginning_gte {
            params.insert("hour_beginning_gte", value.to_string());
        }
        if let Some(value) = &self.hour_beginning_lt {
            params.insert("hour_beginning_lt", value.to_string());
        }
        if let Some(value) = &self.interface_name {
            params.insert("interface_name", value.to_string());
        }
        if let Some(value) = &self.interface_name_like {
            params.insert("interface_name_like", value.to_string());
        }
        if let Some(value) = &self.interface_name_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("interface_name_in", joined);
        }
        if let Some(value) = &self.flow_direction {
            params.insert("flow_direction", value.to_string());
        }
        if let Some(value) = &self.flow_direction_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("flow_direction_in", joined);
        }
        if let Some(value) = &self.mw {
            params.insert("mw", value.to_string());
        }
        if let Some(value) = &self.mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("mw_in", joined);
        }
        if let Some(value) = &self.mw_gte {
            params.insert("mw_gte", value.to_string());
        }
        if let Some(value) = &self.mw_lte {
            params.insert("mw_lte", value.to_string());
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

    pub fn hour_beginning(mut self, value: Zoned) -> Self {
        self.inner.hour_beginning = Some(value);
        self
    }

    pub fn hour_beginning_gte(mut self, value: Zoned) -> Self {
        self.inner.hour_beginning_gte = Some(value);
        self
    }

    pub fn hour_beginning_lt(mut self, value: Zoned) -> Self {
        self.inner.hour_beginning_lt = Some(value);
        self
    }

    pub fn interface_name<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.interface_name = Some(value.into());
        self
    }

    pub fn interface_name_like(mut self, value_like: String) -> Self {
        self.inner.interface_name_like = Some(value_like);
        self
    }

    pub fn interface_name_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.interface_name_in = Some(values_in);
        self
    }

    pub fn flow_direction(mut self, value: FlowDirection) -> Self {
        self.inner.flow_direction = Some(value);
        self
    }

    pub fn flow_direction_in(mut self, values_in: Vec<FlowDirection>) -> Self {
        self.inner.flow_direction_in = Some(values_in);
        self
    }

    pub fn mw(mut self, value: i64) -> Self {
        self.inner.mw = Some(value);
        self
    }

    pub fn mw_in(mut self, values_in: Vec<i64>) -> Self {
        self.inner.mw_in = Some(values_in);
        self
    }

    pub fn mw_gte(mut self, value: i64) -> Self {
        self.inner.mw_gte = Some(value);
        self
    }

    pub fn mw_lte(mut self, value: i64) -> Self {
        self.inner.mw_lte = Some(value);
        self
    }
}

/// Fetch the (report_date, published_at) tuples from the files on the website.
#[allow(clippy::type_complexity)]
pub fn get_ttc_reports_info() -> Result<Vec<(Date, Zoned)>, Box<dyn Error>> {
    let url = "https://www.iso-ne.com/isoexpress/web/reports/operations/-/tree/ttc-tables";
    let body = reqwest::blocking::get(url)?.text()?;
    parse_ttc_table_entries(&body)
}

#[allow(clippy::type_complexity)]
fn parse_ttc_table_entries(html: &str) -> Result<Vec<(Date, Zoned)>, Box<dyn Error>> {
    let document = Html::parse_document(html);
    let col2_sel = Selector::parse("td.rpt-col-2").expect("valid selector");
    let col3_sel = Selector::parse("td.rpt-col-3").expect("valid selector");

    let report_dates: Vec<String> = document
        .select(&col2_sel)
        .map(|el| el.text().collect::<String>().trim().to_string())
        .collect();
    let timestamps: Vec<String> = document
        .select(&col3_sel)
        .map(|el| el.text().collect::<String>().trim().to_string())
        .collect();

    let tz = TimeZone::get("America/New_York")?;
    let mut results = Vec::new();

    for (date_str, ts_str) in report_dates.iter().zip(timestamps.iter()) {
        // "August 22, 2026"
        let report_date = Date::strptime("%B %d, %Y", date_str.trim())?;

        // "08/15/2026 05:10 AM EDT" — strip the trailing timezone abbreviation
        let ts_parts: Vec<&str> = ts_str.trim().splitn(4, ' ').collect();
        if ts_parts.len() < 3 {
            continue;
        }
        let naive_str = format!("{} {} {}", ts_parts[0], ts_parts[1], ts_parts[2]);
        let naive_dt = DateTime::strptime("%m/%d/%Y %I:%M %p", &naive_str)?;
        let published_at = naive_dt.to_zoned(tz.clone())?;

        results.push((report_date, published_at));
    }

    Ok(results)
}

#[cfg(test)]
mod tests {
    use jiff::civil::date;
    use std::error::Error;
    use std::path::Path;

    use crate::{
        db::prod_db::ProdDb,
        interval::{interval_base::DateExt, month::month},
    };
    use duckdb::{AccessMode, Config, Connection};

    use super::*;

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_ttc().duckdb_path, config).unwrap();
        let filter = QueryFilterBuilder::new().build();
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
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::isone_ttc();
        let months = month(2026, 8).up_to(month(2026, 8))?;
        for month in &months {
            println!("Updating DuckDB for month {}", month);
            archive.download_missing_days(month)?;
            archive.update_duckdb(month)?;
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn test_get_ttc_table_entries() -> Result<(), Box<dyn Error>> {
        let entries = get_ttc_reports_info()?;
        eprintln!("found {} entries", entries.len());
        for (date, published_at) in &entries {
            eprintln!("for: {}, published at: {}", date, published_at);
        }
        assert!(!entries.is_empty());
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_days() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::isone_ttc();
        archive.download_days(date(2026, 1, 1).up_to(date(2026, 8, 14)))?;
        Ok(())
    }
}
