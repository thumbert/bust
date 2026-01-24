// Auto-generated Rust stub for DuckDB table: binding_constraints_da
// Created on 2026-01-24 with Dart package reduct

use std::collections::HashMap;
use std::error::Error;
use std::path::Path;

use duckdb::Connection;
use jiff::civil::Date;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use convert_case::{Case, Casing};
use jiff::Timestamp;
use jiff::{tz::TimeZone, Zoned};
use rust_decimal::Decimal;
use std::fs::{self, File};
use std::io::Read;
use std::process::Command;
use log::{error, info};
use std::str::FromStr;

use crate::interval::month::Month;

#[derive(Clone)]
pub struct NyisoBindingConstraintsDaArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl NyisoBindingConstraintsDaArchive {
    /// Return the full file path of the zip file with data for the entire month  
    pub fn filename_zip(&self, month: &Month) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &month.start_date().strftime("%Y%m%d").to_string()
            + "damlbmp_zone_csv.zip"
    }

    /// Return the file path of the csv file with data for one day
    pub fn filename(&self, day: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + day.year().to_string().as_str()
            + "/"
            + &day.strftime("%Y%m%d").to_string()
            + "damlbmp_zone.csv"
    }

    /// Data is published around 10:30 every day
    /// See https://mis.nyiso.com/public/csv/damlbmp/20250501damlbmp_gen_csv.zip
    /// Take the monthly zip file, extract it and compress each individual day as a gz file.
    pub fn download_file(&self, month: Month) -> Result<(), Box<dyn Error>> {
        let binding = self.filename_zip(&month);
        let zip_path = Path::new(&binding);

        let url = format!(
            "https://mis.nyiso.com/public/csv/damlbmp/{}",
            zip_path.file_name().unwrap().to_str().unwrap()
        );
        let mut resp = reqwest::blocking::get(url)?;
        let mut out = File::create(&binding)?;
        std::io::copy(&mut resp, &mut out)?;
        info!("downloaded file: {}", binding);

        // Unzip the file
        info!("Unzipping file {:?}", zip_path);
        let mut zip_file = File::open(zip_path)?;
        let mut zip_data = Vec::new();
        zip_file.read_to_end(&mut zip_data)?;
        let reader = std::io::Cursor::new(zip_data);
        let mut zip = zip::ZipArchive::new(reader)?;
        use std::fs::File as StdFile;
        use std::io::copy as std_copy;

        for i in 0..zip.len() {
            let mut file = zip.by_index(i)?;
            let out_path = match file.enclosed_name() {
                Some(path) => path.to_owned(),
                None => continue,
            };
            let day: Date = out_path.file_name().unwrap().to_str().unwrap()[0..8]
                .parse()
                .map_err(|_| format!("Invalid date in filename: {:?}", out_path))?;
            let out_path = self.base_dir.to_owned()
                + "/Raw/"
                + &day.year().to_string()
                + "/"
                + out_path.file_name().unwrap().to_str().unwrap();
            let dir = Path::new(&out_path).parent().unwrap();
            fs::create_dir_all(dir)?;

            // Use blocking std::fs::File and std::io::copy for extraction
            let mut outfile = StdFile::create(&out_path)?;
            std_copy(&mut file, &mut outfile)?;
            info!(" -- extracted file to {}", out_path);

            // Gzip the csv file
            let mut csv_file = File::open(&out_path)?;
            let mut csv_data = Vec::new();
            csv_file.read_to_end(&mut csv_data)?;
            let gz_path = format!("{}.gz", out_path);
            let mut gz_file = File::create(&gz_path)?;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            use std::io::Write;
            encoder.write_all(&csv_data)?;
            let compressed_data = encoder.finish()?;
            gz_file.write_all(&compressed_data)?;
            info!(" -- gzipped file to {}", gz_path);

            // Remove the original csv file
            std::fs::remove_file(&out_path)?;
        }

        // Remove the zip file
        std::fs::remove_file(zip_path)?;
        info!("removed zip file {:?}", zip_path);

        Ok(())
    }

    /// Update duckdb with published data for the month.  No checks are made to see
    /// if there are missing files.  Does not delete any existing data.  So if data
    /// is wrong for some reason, it needs to be manually deleted first!
    ///
    pub fn update_duckdb(&self, month: Month) -> Result<(), Box<dyn Error>> {
        info!("inserting zone + gen files for the month {} ...", month);
        let sql = format!(
            r#"
        LOAD zipfs;
        CREATE TEMPORARY TABLE tmp1 AS SELECT * FROM '{}/Raw/{}/{}*damlbmp_zone.csv.gz';
        CREATE TEMPORARY TABLE tmp2 AS SELECT * FROM '{}/Raw/{}/{}*damlbmp_gen.csv.gz';

        CREATE TEMPORARY TABLE tmp AS
        (SELECT
            strptime("Time Stamp" || ' America/New_York' , '%m/%d/%Y %H:%M %Z')::TIMESTAMPTZ AS "hour_beginning",
            ptid::INTEGER AS ptid,
            "LBMP ($/MWHr)"::DECIMAL(9,2) AS "lmp",
            "Marginal Cost Losses ($/MWHr)"::DECIMAL(9,2) AS "mlc",
            "Marginal Cost Congestion ($/MWHr)"::DECIMAL(9,2) AS "mcc"
        FROM tmp1
        )
        UNION
        (SELECT
            strptime("Time Stamp" || ' America/New_York' , '%m/%d/%Y %H:%M %Z')::TIMESTAMPTZ AS "hour_beginning",
            ptid::INTEGER AS ptid,
            "LBMP ($/MWHr)"::DECIMAL(9,2) AS "lmp",
            "Marginal Cost Losses ($/MWHr)"::DECIMAL(9,2) AS "mlc",
            "Marginal Cost Congestion ($/MWHr)"::DECIMAL(9,2) AS "mcc"
        FROM tmp2
        )
        ORDER BY hour_beginning, ptid;

        INSERT INTO dalmp
        (SELECT hour_beginning, ptid, lmp, mlc, mcc FROM tmp
        WHERE NOT EXISTS (
            SELECT * FROM dalmp d
            WHERE d.hour_beginning = tmp.hour_beginning
            AND d.ptid = tmp.ptid
        ))
        ORDER BY hour_beginning, ptid;
        "#,
            self.base_dir,
            month.start_date().year(),
            &month.start_date().strftime("%Y%m"),
            self.base_dir,
            month.start_date().year(),
            &month.start_date().strftime("%Y%m"),
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
    pub market: Market,
    pub hour_beginning: Zoned,
    pub limiting_facility: String,
    pub facility_ptid: i64,
    pub contingency: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub constraint_cost: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Market {
    Da,
    Rt,
}

impl std::str::FromStr for Market {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "DA" => Ok(Market::Da),
            "RT" => Ok(Market::Rt),
            _ => Err(format!("Invalid value for Market: {}", s)),
        }
    }
}

impl std::fmt::Display for Market {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Market::Da => write!(f, "DA"),
            Market::Rt => write!(f, "RT"),
        }
    }
}

impl serde::Serialize for Market {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            Market::Da => "DA",
            Market::Rt => "RT",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for Market {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Market::from_str(&s).map_err(serde::de::Error::custom)
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
    market,
    hour_beginning,
    limiting_facility,
    facility_ptid,
    contingency,
    constraint_cost
FROM binding_constraints_da WHERE 1=1"#,
    );
    if let Some(market) = &query_filter.market {
        query.push_str(&format!(
            "
    AND market = '{}'",
            market
        ));
    }
    if let Some(market_in) = &query_filter.market_in {
        query.push_str(&format!(
            "
    AND market IN ('{}')",
            market_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
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
    if let Some(limiting_facility) = &query_filter.limiting_facility {
        query.push_str(&format!(
            "
    AND limiting_facility = '{}'",
            limiting_facility
        ));
    }
    if let Some(limiting_facility_like) = &query_filter.limiting_facility_like {
        query.push_str(&format!(
            "
    AND limiting_facility LIKE '{}'",
            limiting_facility_like
        ));
    }
    if let Some(limiting_facility_in) = &query_filter.limiting_facility_in {
        query.push_str(&format!(
            "
    AND limiting_facility IN ('{}')",
            limiting_facility_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(facility_ptid) = &query_filter.facility_ptid {
        query.push_str(&format!(
            "
    AND facility_ptid = {}",
            facility_ptid
        ));
    }
    if let Some(facility_ptid_in) = &query_filter.facility_ptid_in {
        query.push_str(&format!(
            "
    AND facility_ptid IN ({})",
            facility_ptid_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(facility_ptid_gte) = &query_filter.facility_ptid_gte {
        query.push_str(&format!(
            "
    AND facility_ptid >= {}",
            facility_ptid_gte
        ));
    }
    if let Some(facility_ptid_lte) = &query_filter.facility_ptid_lte {
        query.push_str(&format!(
            "
    AND facility_ptid <= {}",
            facility_ptid_lte
        ));
    }
    if let Some(contingency) = &query_filter.contingency {
        query.push_str(&format!(
            "
    AND contingency = '{}'",
            contingency
        ));
    }
    if let Some(contingency_like) = &query_filter.contingency_like {
        query.push_str(&format!(
            "
    AND contingency LIKE '{}'",
            contingency_like
        ));
    }
    if let Some(contingency_in) = &query_filter.contingency_in {
        query.push_str(&format!(
            "
    AND contingency IN ('{}')",
            contingency_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(constraint_cost) = &query_filter.constraint_cost {
        query.push_str(&format!(
            "
    AND constraint_cost = {}",
            constraint_cost
        ));
    }
    if let Some(constraint_cost_in) = &query_filter.constraint_cost_in {
        query.push_str(&format!(
            "
    AND constraint_cost IN ({})",
            constraint_cost_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(constraint_cost_gte) = &query_filter.constraint_cost_gte {
        query.push_str(&format!(
            "
    AND constraint_cost >= {}",
            constraint_cost_gte
        ));
    }
    if let Some(constraint_cost_lte) = &query_filter.constraint_cost_lte {
        query.push_str(&format!(
            "
    AND constraint_cost <= {}",
            constraint_cost_lte
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
            v => panic!("Unexpected value type {v:?} for enum market"),
        };
        let market = Market::from_str(&_n0).unwrap();
        let _micros1: i64 = row.get::<usize, i64>(1)?;
        let hour_beginning = Zoned::new(
            Timestamp::from_microsecond(_micros1).unwrap(),
            TimeZone::get("America/New_York").unwrap(),
        );
        let limiting_facility: String = row.get::<usize, String>(2)?;
        let facility_ptid: i64 = row.get::<usize, i64>(3)?;
        let contingency: String = row.get::<usize, String>(4)?;
        let constraint_cost: Decimal = match row.get_ref_unwrap(5) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Record {
            market,
            hour_beginning,
            limiting_facility,
            facility_ptid,
            contingency,
            constraint_cost,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub market: Option<Market>,
    pub market_in: Option<Vec<Market>>,
    pub hour_beginning: Option<Zoned>,
    pub hour_beginning_gte: Option<Zoned>,
    pub hour_beginning_lt: Option<Zoned>,
    pub limiting_facility: Option<String>,
    pub limiting_facility_like: Option<String>,
    pub limiting_facility_in: Option<Vec<String>>,
    pub facility_ptid: Option<i64>,
    pub facility_ptid_in: Option<Vec<i64>>,
    pub facility_ptid_gte: Option<i64>,
    pub facility_ptid_lte: Option<i64>,
    pub contingency: Option<String>,
    pub contingency_like: Option<String>,
    pub contingency_in: Option<Vec<String>>,
    pub constraint_cost: Option<Decimal>,
    pub constraint_cost_in: Option<Vec<Decimal>>,
    pub constraint_cost_gte: Option<Decimal>,
    pub constraint_cost_lte: Option<Decimal>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.market {
            params.insert("market", value.to_string());
        }
        if let Some(value) = &self.market_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("market_in", joined);
        }
        if let Some(value) = &self.hour_beginning {
            params.insert("hour_beginning", value.to_string());
        }
        if let Some(value) = &self.hour_beginning_gte {
            params.insert("hour_beginning_gte", value.to_string());
        }
        if let Some(value) = &self.hour_beginning_lt {
            params.insert("hour_beginning_lt", value.to_string());
        }
        if let Some(value) = &self.limiting_facility {
            params.insert("limiting_facility", value.to_string());
        }
        if let Some(value) = &self.limiting_facility_like {
            params.insert("limiting_facility_like", value.to_string());
        }
        if let Some(value) = &self.limiting_facility_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("limiting_facility_in", joined);
        }
        if let Some(value) = &self.facility_ptid {
            params.insert("facility_ptid", value.to_string());
        }
        if let Some(value) = &self.facility_ptid_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("facility_ptid_in", joined);
        }
        if let Some(value) = &self.facility_ptid_gte {
            params.insert("facility_ptid_gte", value.to_string());
        }
        if let Some(value) = &self.facility_ptid_lte {
            params.insert("facility_ptid_lte", value.to_string());
        }
        if let Some(value) = &self.contingency {
            params.insert("contingency", value.to_string());
        }
        if let Some(value) = &self.contingency_like {
            params.insert("contingency_like", value.to_string());
        }
        if let Some(value) = &self.contingency_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("contingency_in", joined);
        }
        if let Some(value) = &self.constraint_cost {
            params.insert("constraint_cost", value.to_string());
        }
        if let Some(value) = &self.constraint_cost_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("constraint_cost_in", joined);
        }
        if let Some(value) = &self.constraint_cost_gte {
            params.insert("constraint_cost_gte", value.to_string());
        }
        if let Some(value) = &self.constraint_cost_lte {
            params.insert("constraint_cost_lte", value.to_string());
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

    pub fn market(mut self, value: Market) -> Self {
        self.inner.market = Some(value);
        self
    }

    pub fn market_in(mut self, values_in: Vec<Market>) -> Self {
        self.inner.market_in = Some(values_in);
        self
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

    pub fn limiting_facility<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.limiting_facility = Some(value.into());
        self
    }

    pub fn limiting_facility_like(mut self, value_like: String) -> Self {
        self.inner.limiting_facility_like = Some(value_like);
        self
    }

    pub fn limiting_facility_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.limiting_facility_in = Some(values_in);
        self
    }

    pub fn facility_ptid(mut self, value: i64) -> Self {
        self.inner.facility_ptid = Some(value);
        self
    }

    pub fn facility_ptid_in(mut self, values_in: Vec<i64>) -> Self {
        self.inner.facility_ptid_in = Some(values_in);
        self
    }

    pub fn facility_ptid_gte(mut self, value: i64) -> Self {
        self.inner.facility_ptid_gte = Some(value);
        self
    }

    pub fn facility_ptid_lte(mut self, value: i64) -> Self {
        self.inner.facility_ptid_lte = Some(value);
        self
    }

    pub fn contingency<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.contingency = Some(value.into());
        self
    }

    pub fn contingency_like(mut self, value_like: String) -> Self {
        self.inner.contingency_like = Some(value_like);
        self
    }

    pub fn contingency_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.contingency_in = Some(values_in);
        self
    }

    pub fn constraint_cost(mut self, value: Decimal) -> Self {
        self.inner.constraint_cost = Some(value);
        self
    }

    pub fn constraint_cost_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.constraint_cost_in = Some(values_in);
        self
    }

    pub fn constraint_cost_gte(mut self, value: Decimal) -> Self {
        self.inner.constraint_cost_gte = Some(value);
        self
    }

    pub fn constraint_cost_lte(mut self, value: Decimal) -> Self {
        self.inner.constraint_cost_lte = Some(value);
        self
    }
}

// #[cfg(test)]
// mod tests {
//     use std::error::Error;
//     use duckdb::{AccessMode, Config, Connection};
//     use crate::db::prod_db::ProdDb;
//     use super::*;

//     #[test]
//     fn test_get_data() -> Result<(), Box<dyn Error>> {
//         let config = Config::default().access_mode(AccessMode::ReadOnly)?;
//         let conn = Connection::open_with_flags(ProdDb::scratch().duckdb_path, config).unwrap();
//         let filter = QueryFilterBuilder::new().build();
//         let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
//         conn.close().unwrap();
//         assert_eq!(xs.len(), 5);
//         Ok(())
//     }
// }

#[cfg(test)]
mod tests {

    use std::{error::Error, path::Path, vec};

    use rust_decimal_macros::dec;

    use crate::{
        db::{nyiso::dalmp::*, prod_db::ProdDb},
        interval::month::month,
    };

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::nyiso_dalmp();
        archive.setup()?;

        let months = month(2026, 1).up_to(month(2026, 1))?;
        for month in months {
            println!("Processing month {}", month);
            archive.update_duckdb(month)?;
        }
        Ok(())
    }

    // #[test]
    // #[should_panic]
    // fn get_data_test() {
    //     dotenvy::from_path(Path::new(".env/test.env")).unwrap();
    //     let archive = ProdDb::nyiso_dalmp();
    //     let conn = duckdb::Connection::open(archive.duckdb_path.clone()).unwrap();
    //     // test a zone location at DST
    //     let rows = archive
    //         .get_data(
    //             &conn,
    //             date(2024, 11, 3),
    //             date(2024, 11, 3),
    //             LmpComponent::Lmp,
    //             Some(vec![61752]),
    //         )
    //         .unwrap();
    //     assert_eq!(rows.len(), 25);
    //     let values = rows[0..=2].iter().map(|r| r.value).collect::<Vec<_>>();
    //     // the assertion below fails.  DuckDB has issues importing the DST hour from NYISO file.
    //     assert_eq!(values, vec![dec!(29.27), dec!(27.32), dec!(27.14)]);
    //     assert_eq!(
    //         rows[2].hour_beginning,
    //         "2024-11-03T01:00:00-05:00[America/New_York]"
    //             .parse()
    //             .unwrap()
    //     );
    //     assert_eq!(rows[2].value, dec!(27.14));

    //     // test a gen location
    //     let rows = archive
    //         .get_data(
    //             &conn,
    //             date(2025, 6, 27),
    //             date(2025, 6, 27),
    //             LmpComponent::Lmp,
    //             Some(vec![23575]),
    //         )
    //         .unwrap();
    //     assert_eq!(rows.len(), 24);
    //     assert_eq!(
    //         rows[0].hour_beginning,
    //         "2025-06-27T00:00:00-04:00[America/New_York]"
    //             .parse()
    //             .unwrap()
    //     );
    //     assert_eq!(rows[0].value, dec!(37.59));
    // }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();

        let archive = ProdDb::nyiso_dalmp();
        let months = month(2026, 1).up_to(month(2026, 1))?;
        for month in months {
            archive.download_file(month, NodeType::Gen)?;
            archive.download_file(month, NodeType::Zone)?;
        }
        Ok(())
    }
}
