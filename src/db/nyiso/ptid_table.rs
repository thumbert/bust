// Auto-generated Rust stub for DuckDB table: ptid_table
// Created on 2026-06-17 with Dart package reduct

use std::collections::{HashMap, HashSet};
use std::error::Error;

use duckdb::Connection;
use jiff::civil::Date;
use jiff::{ToSpan, Zoned};
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use convert_case::{Case, Casing};
use log::{error, info};
use std::fs::File;
use std::io::Read;
use std::process::Command;
use std::str::FromStr;

#[derive(Clone)]
pub struct NyisoPtidTableArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl NyisoPtidTableArchive {
    /// Return the file path of the csv file with data for one day
    pub fn filename(&self, day: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/generator_"
            + &day.strftime("%Y-%m-%d").to_string()
            + ".csv"
    }

    /// Data is published around 10:30 every day
    /// See https://mis.nyiso.com/public/csv/DAMLimitingConstraints/20260101DAMLimitingConstraints_csv.zip
    /// Take the monthly zip file, extract it and compress each individual day as a gz file.
    pub fn download_file(&self) -> Result<(), Box<dyn Error>> {
        let binding = self.filename(&Zoned::now().date());

        let url = "http://mis.nyiso.com/public/csv/generator/generator.csv";
        let mut resp = reqwest::blocking::get(url)?;
        let mut out = File::create(&binding)?;
        std::io::copy(&mut resp, &mut out)?;
        info!("downloaded file: {}", binding);

        // Gzip the csv file
        let mut csv_file = File::open(&binding)?;
        let mut csv_data = Vec::new();
        csv_file.read_to_end(&mut csv_data)?;
        let gz_path = format!("{}.gz", binding);
        let mut gz_file = File::create(&gz_path)?;
        let mut encoder = flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
        use std::io::Write;
        encoder.write_all(&csv_data)?;
        let compressed_data = encoder.finish()?;
        gz_file.write_all(&compressed_data)?;
        info!(" -- gzipped file to {}", gz_path);

        // Remove the original csv file
        std::fs::remove_file(&binding)?;

        Ok(())
    }

    /// Update duckdb with published data for the month.  No checks are made to see
    /// if there are missing files.  Does not delete any existing data.  So if data
    /// is wrong for some reason, it needs to be manually deleted first!
    ///
    pub fn update_duckdb(&self, day: Date) -> Result<(), Box<dyn Error>> {
        info!("inserting ptid table for {} ...", day);
        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS ptid_table (
    node_type ENUM('gen', 'zone') NOT NULL,
    ptid INTEGER NOT NULL,
    name VARCHAR NOT NULL,
    aggregation_ptid INTEGER,
    subzone VARCHAR,
    zone VARCHAR NOT NULL,
    latitude DOUBLE,
    longitude DOUBLE,
    active BOOLEAN NOT NULL,
    "asof" DATE NOT NULL
);
CREATE TEMPORARY TABLE tmp
AS (
    SELECT 
        'gen' AS node_type,
        CAST("Generator PTID" AS INTEGER) AS ptid,
        "Generator Name" AS name,
        CAST("Aggregation PTID" AS INTEGER) AS aggregation_ptid,
        "Subzone" AS subzone,
        "Zone" AS zone,
        CAST("Latitude" AS DOUBLE) AS latitude,
        CAST("Longitude" AS DOUBLE) AS longitude,
        case "Active"
            when 'Y' then true
            when 'N' then false
            else NULL
        end AS active,
        CAST('{}' AS DATE) AS asof
    FROM read_csv('{}/Raw/generator_{}.csv.gz', 
        header = true)
);
INSERT INTO ptid_table
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM ptid_table d
        WHERE
            d.ptid = t.ptid
    )
);
--- update the active status of existing ptids (if it has changed)
UPDATE ptid_table p
SET active = t.active,
    "asof" = t.asof
FROM tmp t
WHERE p.ptid = t.ptid
  AND p.active IS DISTINCT FROM t.active;
        "#,
            day.strftime("%Y-%m-%d"),
            self.base_dir,
            day.strftime("%Y-%m-%d"),
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
            error!("Failed to update duckdb for {}: {}", day, stderr);
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub node_type: NodeType,
    pub ptid: i32,
    pub name: String,
    pub aggregation_ptid: Option<i32>,
    pub subzone: Option<String>,
    pub zone: String,
    pub latitude: Option<f64>,
    pub longitude: Option<f64>,
    pub active: bool,
    pub asof: Date,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NodeType {
    Gen,
    Zone,
}

impl std::str::FromStr for NodeType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "GEN" => Ok(NodeType::Gen),
            "ZONE" => Ok(NodeType::Zone),
            _ => Err(format!("Invalid value for NodeType: {}", s)),
        }
    }
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NodeType::Gen => write!(f, "gen"),
            NodeType::Zone => write!(f, "zone"),
        }
    }
}

impl serde::Serialize for NodeType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            NodeType::Gen => "gen",
            NodeType::Zone => "zone",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for NodeType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NodeType::from_str(&s).map_err(serde::de::Error::custom)
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
    node_type,
    ptid,
    name,
    aggregation_ptid,
    subzone,
    zone,
    latitude,
    longitude,
    active,
    "asof"
FROM ptid_table WHERE 1=1"#,
    );
    if let Some(node_type) = &query_filter.node_type {
        query.push_str(&format!(
            "
    AND node_type = '{}'",
            node_type
        ));
    }
    if let Some(node_type_in) = &query_filter.node_type_in {
        query.push_str(&format!(
            "
    AND node_type IN ('{}')",
            node_type_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(zone) = &query_filter.zone {
        query.push_str(&format!(
            "
    AND zone = '{}'",
            zone
        ));
    }
    if let Some(zone_like) = &query_filter.zone_like {
        query.push_str(&format!(
            "
    AND zone LIKE '{}'",
            zone_like
        ));
    }
    if let Some(zone_in) = &query_filter.zone_in {
        query.push_str(&format!(
            "
    AND zone IN ('{}')",
            zone_in
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
        let _n0 = match row.get_ref_unwrap(0).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum node_type"),
        };
        let node_type = NodeType::from_str(&_n0).unwrap();
        let ptid: i32 = row.get::<usize, i32>(1)?;
        let name: String = row.get::<usize, String>(2)?;
        let aggregation_ptid: Option<i32> = row.get::<usize, Option<i32>>(3)?;
        let subzone: Option<String> = row.get::<usize, Option<String>>(4)?;
        let zone: String = row.get::<usize, String>(5)?;
        let latitude: Option<f64> = row.get::<usize, Option<f64>>(6)?;
        let longitude: Option<f64> = row.get::<usize, Option<f64>>(7)?;
        let active: bool = row.get::<usize, bool>(8)?;
        let _n9 = 719528 + row.get::<usize, i32>(9)?;
        let asof = Date::ZERO + _n9.days();
        Ok(Record {
            node_type,
            ptid,
            name,
            aggregation_ptid,
            subzone,
            zone,
            latitude,
            longitude,
            active,
            asof,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub node_type: Option<NodeType>,
    pub node_type_in: Option<Vec<NodeType>>,
    pub zone: Option<String>,
    pub zone_like: Option<String>,
    pub zone_in: Option<Vec<String>>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.node_type {
            params.insert("node_type", value.to_string());
        }
        if let Some(value) = &self.node_type_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("node_type_in", joined);
        }
        if let Some(value) = &self.zone {
            params.insert("zone", value.to_string());
        }
        if let Some(value) = &self.zone_like {
            params.insert("zone_like", value.to_string());
        }
        if let Some(value) = &self.zone_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("zone_in", joined);
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

    pub fn node_type(mut self, value: NodeType) -> Self {
        self.inner.node_type = Some(value);
        self
    }

    pub fn node_type_in(mut self, values_in: Vec<NodeType>) -> Self {
        self.inner.node_type_in = Some(values_in);
        self
    }

    pub fn zone<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.zone = Some(value.into());
        self
    }

    pub fn zone_like(mut self, value_like: String) -> Self {
        self.inner.zone_like = Some(value_like);
        self
    }

    pub fn zone_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.zone_in = Some(values_in);
        self
    }
}

/// Get new nodes that were added between day1 and day2.  Returns an error if day2 is not after day1.
pub fn get_new_nodes(
    conn: &Connection,
    day1: Date,
    day2: Date,
) -> Result<Vec<Record>, Box<dyn Error>> {
    if day2 <= day1 {
        return Err(format!("day2 ({}) must be after day1 ({})", day2, day1).into());
    }
    let rows = get_data(conn, &QueryFilterBuilder::new().build(), None)?;
    // get the rows that were there on day 1
    let rows1: HashSet<i32> = rows
        .iter()
        .filter(|r| r.asof >= day1)
        .map(|r| r.ptid)
        .collect::<HashSet<_>>();
    let rows2: HashSet<i32> = rows
        .iter()
        .filter(|r| r.asof >= day2)
        .map(|r| r.ptid)
        .collect::<HashSet<_>>();
    // get the rows that are in day2 but not in day1 using set difference
    let new_rows = rows2
        .difference(&rows1)
        .map(|ptid| rows.iter().find(|r| r.ptid == *ptid).unwrap().clone())
        .collect::<Vec<_>>();
    Ok(new_rows)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::prod_db::ProdDb;
    use duckdb::{AccessMode, Config, Connection};
    use jiff::civil::date;
    use std::{error::Error, path::Path};

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::nyiso_ptid_table().duckdb_path, config).unwrap();
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
        let archive = ProdDb::nyiso_ptid_table();
        let today = Zoned::now().date();
        archive.update_duckdb(today)?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();

        let archive = ProdDb::nyiso_ptid_table();
        archive.download_file()?;
        Ok(())
    }

    #[test]
    fn new_nodes() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::nyiso_ptid_table().duckdb_path, config).unwrap();
        let rows = get_new_nodes(&conn, date(2026, 6, 6), date(2026, 6, 21))?;
        assert_eq!(rows.len(), 0);
        Ok(())
    }
}
