// Auto-generated Rust stub for DuckDB table: public_bids_da
// Created on 2025-12-23 with elec_server/utils/lib_duckdb_builder.dart

use convert_case::{Case, Casing};
use duckdb::Connection;
use futures::StreamExt;
use jiff::civil::Date;
use jiff::Timestamp;
use jiff::{tz::TimeZone, ToSpan, Zoned};
use log::{error, info};
use reqwest::get;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;
use tokio::fs::{self, File};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio_util::io::StreamReader;

use crate::interval::month::Month;

#[derive(Clone)]
pub struct CaisoPublicBidsArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl CaisoPublicBidsArchive {
    /// Return the csv filename for one component for the day.  Does not check if the file exists.  
    /// For example:
    ///  - 20250101_20250101_PUB_BID_DAM_v3.csv
    pub fn filename(&self, date: &Date) -> String {
        let yyyymmdd = date.strftime("%Y%m%d");
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + format!("/{}_{}_PUB_BID_DAM_v3.csv", yyyymmdd, yyyymmdd,).as_str()
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!("inserting Caiso public bid files for month {} ...", month);

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS public_bids_da (
    hour_beginning TIMESTAMPTZ NOT NULL,
    resource_type ENUM('GENERATOR','INTERTIE', 'LOAD') NOT NULL,
    scheduling_coordinator_seq UINTEGER NOT NULL,
    resource_bid_seq UINTEGER NOT NULL,
    time_interval_start TIMESTAMPTZ,
    time_interval_end TIMESTAMPTZ,
    product_bid_desc VARCHAR,
    product_bid_mrid VARCHAR,
    market_product_desc VARCHAR,
    market_product_type VARCHAR,
    self_sched_mw DECIMAL(9,4),
    sch_bid_time_interval_start TIMESTAMPTZ,
    sch_bid_time_interval_end TIMESTAMPTZ,
    sch_bid_xaxis_data DECIMAL(9,4),
    sch_bid_y1axis_data DECIMAL(9,4),
    sch_bid_y2axis_data DECIMAL(9,4),
    sch_bid_curve_type ENUM('BIDPRICE'),
    min_eoh_state_of_charge DECIMAL(9,4),
    max_eoh_state_of_charge DECIMAL(9,4),
);

LOAD icu; SET TimeZone = 'America/Los_Angeles';
CREATE TEMPORARY TABLE tmp 
AS 
    SELECT 
        "STARTTIME" AS hour_beginning,
        "RESOURCE_TYPE"::ENUM('GENERATOR','INTERTIE', 'LOAD') AS resource_type,
        "SCHEDULINGCOORDINATOR_SEQ"::UINTEGER AS scheduling_coordinator_seq,
        "RESOURCEBID_SEQ"::UINTEGER AS resource_bid_seq,
        "TIMEINTERVALSTART" AS time_interval_start,
        "TIMEINTERVALEND" AS time_interval_end,
        "PRODUCTBID_DESC" AS product_bid_desc,
        "PRODUCTBID_MRID" AS product_bid_mrid,
        "MARKETPRODUCT_DESC" AS market_product_desc,
        "MARKETPRODUCTTYPE" AS market_product_type,
        "SELFSCHEDMW"::DECIMAL(9,4) AS self_sched_mw,
        "SCH_BID_TIMEINTERVALSTART" AS sch_bid_time_interval_start,
        "SCH_BID_TIMEINTERVALSTOP" AS sch_bid_time_interval_end,
        "SCH_BID_XAXISDATA"::DECIMAL(9,4) AS sch_bid_xaxis_data,
        "SCH_BID_Y1AXISDATA"::DECIMAL(9,4) AS sch_bid_y1axis_data,
        "SCH_BID_Y2AXISDATA"::DECIMAL(9,4) AS sch_bid_y2axis_data,
        "SCH_BID_CURVETYPE"::ENUM('BIDPRICE') AS sch_bid_curve_type,
        "MINEOHSTATEOFCHARGE"::DECIMAL(9,4) AS min_eoh_state_of_charge,
        "MAXEOHSTATEOFCHARGE"::DECIMAL(9,4) AS max_eoh_state_of_charge
    FROM read_csv(
        '{}/Raw/{}/{}*_{}*_PUB_BID_DAM_v3.csv.gz',
        header = true,
        types = {{'SCH_BID_Y2AXISDATA': 'DECIMAL(9,4)', 'SCH_BID_Y1AXISDATA': 'DECIMAL(9,4)', 'SCH_BID_XAXISDATA': 'DECIMAL(9,4)', 'SELFSCHEDMW': 'DECIMAL(9,4)', 'RESOURCEBID_SEQ': 'UINTEGER', 'SCHEDULINGCOORDINATOR_SEQ': 'UINTEGER'}},
        timestampformat = 'YYYY-MM-DD HH:MM:SS.000'
    )
    ORDER BY hour_beginning, resource_bid_seq 
;

INSERT INTO public_bids_da
(
    SELECT * FROM tmp
    WHERE NOT EXISTS (
        SELECT 1 FROM public_bids_da AS pb
        WHERE pb.hour_beginning = tmp.hour_beginning
          AND pb.resource_bid_seq = tmp.resource_bid_seq
          AND pb.scheduling_coordinator_seq = tmp.scheduling_coordinator_seq
    )
);
"#,
            self.base_dir,
            month.start_date().year(),
            month.start_date().strftime("%Y%m"),
            month.start_date().strftime("%Y%m"),
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

    /// Data is usually published before XX:XX every day, with 4 month delay.
    /// Download the zip file for the date which contains 1 file.
    /// https://oasis.caiso.com/oasisapi/GroupZip?resultformat=6&version=3&groupid=PUB_DAM_GRP&startdatetime=20250101T08:00-0000
    /// https://oasis.caiso.com/oasisapi/GroupZip?resultformat=6&version=3&groupid=PUB_DAM_GRP&startdatetime=20250601T07:00-0000
    ///
    pub async fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        let start = date.at(0, 0, 0, 0).in_tz("America/Los_Angeles")?;
        let start_z = start.in_tz("UTC")?.strftime("%Y%m%dT%H:%M-0000");
        let url = format!("https://oasis.caiso.com/oasisapi/GroupZip?resultformat=6&version=3&groupid=PUB_DAM_GRP&startdatetime={}", start_z);
        // info!("Downloading from URL: {}", url);

        let resp = get(&url).await?;
        let stream = resp.bytes_stream();
        let mut reader = StreamReader::new(stream.map(|r| r.map_err(std::io::Error::other)));

        let zip_path = self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + format!("/{}_{}_PUB_BID_DAM_GRP_N_N_v3_csv.zip", yyyymmdd, yyyymmdd).as_str();
        let dir = Path::new(&zip_path).parent().unwrap();
        fs::create_dir_all(dir).await?;

        let mut out = File::create(&zip_path).await?;
        let out = tokio::io::copy(&mut reader, &mut out).await?;
        info!("downloaded {} bytes", out);

        // Unzip the file
        info!("Unzipping file {}", zip_path);
        let mut zip_file = File::open(&zip_path).await?;
        let mut zip_data = Vec::new();
        zip_file.read_to_end(&mut zip_data).await?;
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
            let out_path = self.base_dir.to_owned()
                + "/Raw/"
                + &date.year().to_string()
                + "/"
                + out_path.file_name().unwrap().to_str().unwrap();

            // Use blocking std::fs::File and std::io::copy for extraction
            let mut outfile = StdFile::create(&out_path)?;
            std_copy(&mut file, &mut outfile)?;
            info!("extracted file to {}", out_path);

            // Gzip the csv file
            let mut csv_file = File::open(&out_path).await?;
            let mut csv_data = Vec::new();
            csv_file.read_to_end(&mut csv_data).await?;
            let gz_path = format!("{}.gz", out_path);
            let mut gz_file = File::create(&gz_path).await?;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            use std::io::Write;
            encoder.write_all(&csv_data)?;
            let compressed_data = encoder.finish()?;
            gz_file.write_all(&compressed_data).await?;
            info!("gzipped file to {}", gz_path);

            // Remove the original csv file
            tokio::fs::remove_file(&out_path).await?;
        }

        // Remove the original zip file
        tokio::fs::remove_file(&zip_path).await?;
        info!("removed zip file {}", zip_path);

        Ok(())
    }

    /// Get the last date for which data is available.
    pub fn get_last_day(&self) -> Date {
        let mut last = Zoned::now().date();
        last = last.saturating_sub(3.months());
        last
    }

    /// Look for missing days
    pub async fn download_missing_days(&self, month: Month) -> Result<(), Box<dyn Error>> {
        let last = self.get_last_day();
        for day in month.days() {
            if day > last {
                continue;
            }
            let fname = format!("{}.gz", self.filename(&day));
            if !Path::new(&fname).exists() {
                info!("Working on {}", day);
                self.download_file(day).await?;
                info!("  downloaded file for {}", day);
                // wait a bit to avoid being blocked
                tokio::time::sleep(tokio::time::Duration::from_secs(11)).await;
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub hour_beginning: Zoned,
    pub resource_type: ResourceType,
    pub scheduling_coordinator_seq: u32,
    pub resource_bid_seq: u32,
    pub time_interval_start: Option<Zoned>,
    pub time_interval_end: Option<Zoned>,
    pub product_bid_desc: Option<String>,
    pub product_bid_mrid: Option<String>,
    pub market_product_desc: Option<String>,
    pub market_product_type: Option<String>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub self_sched_mw: Option<Decimal>,
    pub sch_bid_time_interval_start: Option<Zoned>,
    pub sch_bid_time_interval_end: Option<Zoned>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub sch_bid_xaxis_data: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub sch_bid_y1axis_data: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub sch_bid_y2axis_data: Option<Decimal>,
    pub sch_bid_curve_type: Option<SchBidCurveType>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub min_eoh_state_of_charge: Option<Decimal>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub max_eoh_state_of_charge: Option<Decimal>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResourceType {
    Generator,
    Intertie,
    Load,
}

impl std::str::FromStr for ResourceType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "GENERATOR" => Ok(ResourceType::Generator),
            "INTERTIE" => Ok(ResourceType::Intertie),
            "LOAD" => Ok(ResourceType::Load),
            _ => Err(format!("Invalid value for ResourceType: {}", s)),
        }
    }
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ResourceType::Generator => write!(f, "GENERATOR"),
            ResourceType::Intertie => write!(f, "INTERTIE"),
            ResourceType::Load => write!(f, "LOAD"),
        }
    }
}

impl serde::Serialize for ResourceType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            ResourceType::Generator => "GENERATOR",
            ResourceType::Intertie => "INTERTIE",
            ResourceType::Load => "LOAD",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for ResourceType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ResourceType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum SchBidCurveType {
    Bidprice,
}

impl std::str::FromStr for SchBidCurveType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "BIDPRICE" => Ok(SchBidCurveType::Bidprice),
            _ => Err(format!("Invalid value for SchBidCurveType: {}", s)),
        }
    }
}

impl std::fmt::Display for SchBidCurveType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            SchBidCurveType::Bidprice => write!(f, "BIDPRICE"),
        }
    }
}

impl serde::Serialize for SchBidCurveType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            SchBidCurveType::Bidprice => "BIDPRICE",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for SchBidCurveType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        SchBidCurveType::from_str(&s).map_err(serde::de::Error::custom)
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
    resource_type,
    scheduling_coordinator_seq,
    resource_bid_seq,
    time_interval_start,
    time_interval_end,
    product_bid_desc,
    product_bid_mrid,
    market_product_desc,
    market_product_type,
    self_sched_mw,
    sch_bid_time_interval_start,
    sch_bid_time_interval_end,
    sch_bid_xaxis_data,
    sch_bid_y1axis_data,
    sch_bid_y2axis_data,
    sch_bid_curve_type,
    min_eoh_state_of_charge,
    max_eoh_state_of_charge
FROM public_bids_da WHERE 1=1"#,
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
    if let Some(resource_type) = &query_filter.resource_type {
        query.push_str(&format!(
            "
    AND resource_type = '{}'",
            resource_type
        ));
    }
    if let Some(resource_type_in) = &query_filter.resource_type_in {
        query.push_str(&format!(
            "
    AND resource_type IN ('{}')",
            resource_type_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(scheduling_coordinator_seq) = &query_filter.scheduling_coordinator_seq {
        query.push_str(&format!(
            "
    AND scheduling_coordinator_seq = {}",
            scheduling_coordinator_seq
        ));
    }
    if let Some(scheduling_coordinator_seq_in) = &query_filter.scheduling_coordinator_seq_in {
        query.push_str(&format!(
            "
    AND scheduling_coordinator_seq IN ({})",
            scheduling_coordinator_seq_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(scheduling_coordinator_seq_gte) = &query_filter.scheduling_coordinator_seq_gte {
        query.push_str(&format!(
            "
    AND scheduling_coordinator_seq >= {}",
            scheduling_coordinator_seq_gte
        ));
    }
    if let Some(scheduling_coordinator_seq_lte) = &query_filter.scheduling_coordinator_seq_lte {
        query.push_str(&format!(
            "
    AND scheduling_coordinator_seq <= {}",
            scheduling_coordinator_seq_lte
        ));
    }
    if let Some(resource_bid_seq) = &query_filter.resource_bid_seq {
        query.push_str(&format!(
            "
    AND resource_bid_seq = {}",
            resource_bid_seq
        ));
    }
    if let Some(resource_bid_seq_in) = &query_filter.resource_bid_seq_in {
        query.push_str(&format!(
            "
    AND resource_bid_seq IN ({})",
            resource_bid_seq_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(resource_bid_seq_gte) = &query_filter.resource_bid_seq_gte {
        query.push_str(&format!(
            "
    AND resource_bid_seq >= {}",
            resource_bid_seq_gte
        ));
    }
    if let Some(resource_bid_seq_lte) = &query_filter.resource_bid_seq_lte {
        query.push_str(&format!(
            "
    AND resource_bid_seq <= {}",
            resource_bid_seq_lte
        ));
    }
    if let Some(time_interval_start) = &query_filter.time_interval_start {
        query.push_str(&format!(
            "
    AND time_interval_start = '{}'",
            time_interval_start.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(time_interval_start_gte) = &query_filter.time_interval_start_gte {
        query.push_str(&format!(
            "
    AND time_interval_start >= '{}'",
            time_interval_start_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(time_interval_start_lt) = &query_filter.time_interval_start_lt {
        query.push_str(&format!(
            "
    AND time_interval_start < '{}'",
            time_interval_start_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(time_interval_end) = &query_filter.time_interval_end {
        query.push_str(&format!(
            "
    AND time_interval_end = '{}'",
            time_interval_end.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(time_interval_end_gte) = &query_filter.time_interval_end_gte {
        query.push_str(&format!(
            "
    AND time_interval_end >= '{}'",
            time_interval_end_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(time_interval_end_lt) = &query_filter.time_interval_end_lt {
        query.push_str(&format!(
            "
    AND time_interval_end < '{}'",
            time_interval_end_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(product_bid_desc) = &query_filter.product_bid_desc {
        query.push_str(&format!(
            "
    AND product_bid_desc = '{}'",
            product_bid_desc
        ));
    }
    if let Some(product_bid_desc_like) = &query_filter.product_bid_desc_like {
        query.push_str(&format!(
            "
    AND product_bid_desc LIKE '{}'",
            product_bid_desc_like
        ));
    }
    if let Some(product_bid_desc_in) = &query_filter.product_bid_desc_in {
        query.push_str(&format!(
            "
    AND product_bid_desc IN ('{}')",
            product_bid_desc_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(product_bid_mrid) = &query_filter.product_bid_mrid {
        query.push_str(&format!(
            "
    AND product_bid_mrid = '{}'",
            product_bid_mrid
        ));
    }
    if let Some(product_bid_mrid_like) = &query_filter.product_bid_mrid_like {
        query.push_str(&format!(
            "
    AND product_bid_mrid LIKE '{}'",
            product_bid_mrid_like
        ));
    }
    if let Some(product_bid_mrid_in) = &query_filter.product_bid_mrid_in {
        query.push_str(&format!(
            "
    AND product_bid_mrid IN ('{}')",
            product_bid_mrid_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(market_product_desc) = &query_filter.market_product_desc {
        query.push_str(&format!(
            "
    AND market_product_desc = '{}'",
            market_product_desc
        ));
    }
    if let Some(market_product_desc_like) = &query_filter.market_product_desc_like {
        query.push_str(&format!(
            "
    AND market_product_desc LIKE '{}'",
            market_product_desc_like
        ));
    }
    if let Some(market_product_desc_in) = &query_filter.market_product_desc_in {
        query.push_str(&format!(
            "
    AND market_product_desc IN ('{}')",
            market_product_desc_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(market_product_type) = &query_filter.market_product_type {
        query.push_str(&format!(
            "
    AND market_product_type = '{}'",
            market_product_type
        ));
    }
    if let Some(market_product_type_like) = &query_filter.market_product_type_like {
        query.push_str(&format!(
            "
    AND market_product_type LIKE '{}'",
            market_product_type_like
        ));
    }
    if let Some(market_product_type_in) = &query_filter.market_product_type_in {
        query.push_str(&format!(
            "
    AND market_product_type IN ('{}')",
            market_product_type_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(self_sched_mw) = &query_filter.self_sched_mw {
        query.push_str(&format!(
            "
    AND self_sched_mw = {}",
            self_sched_mw
        ));
    }
    if let Some(self_sched_mw_in) = &query_filter.self_sched_mw_in {
        query.push_str(&format!(
            "
    AND self_sched_mw IN ({})",
            self_sched_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(self_sched_mw_gte) = &query_filter.self_sched_mw_gte {
        query.push_str(&format!(
            "
    AND self_sched_mw >= {}",
            self_sched_mw_gte
        ));
    }
    if let Some(self_sched_mw_lte) = &query_filter.self_sched_mw_lte {
        query.push_str(&format!(
            "
    AND self_sched_mw <= {}",
            self_sched_mw_lte
        ));
    }
    if let Some(sch_bid_time_interval_start) = &query_filter.sch_bid_time_interval_start {
        query.push_str(&format!(
            "
    AND sch_bid_time_interval_start = '{}'",
            sch_bid_time_interval_start.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(sch_bid_time_interval_start_gte) = &query_filter.sch_bid_time_interval_start_gte {
        query.push_str(&format!(
            "
    AND sch_bid_time_interval_start >= '{}'",
            sch_bid_time_interval_start_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(sch_bid_time_interval_start_lt) = &query_filter.sch_bid_time_interval_start_lt {
        query.push_str(&format!(
            "
    AND sch_bid_time_interval_start < '{}'",
            sch_bid_time_interval_start_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(sch_bid_time_interval_end) = &query_filter.sch_bid_time_interval_end {
        query.push_str(&format!(
            "
    AND sch_bid_time_interval_end = '{}'",
            sch_bid_time_interval_end.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(sch_bid_time_interval_end_gte) = &query_filter.sch_bid_time_interval_end_gte {
        query.push_str(&format!(
            "
    AND sch_bid_time_interval_end >= '{}'",
            sch_bid_time_interval_end_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(sch_bid_time_interval_end_lt) = &query_filter.sch_bid_time_interval_end_lt {
        query.push_str(&format!(
            "
    AND sch_bid_time_interval_end < '{}'",
            sch_bid_time_interval_end_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(sch_bid_xaxis_data) = &query_filter.sch_bid_xaxis_data {
        query.push_str(&format!(
            "
    AND sch_bid_xaxis_data = {}",
            sch_bid_xaxis_data
        ));
    }
    if let Some(sch_bid_xaxis_data_in) = &query_filter.sch_bid_xaxis_data_in {
        query.push_str(&format!(
            "
    AND sch_bid_xaxis_data IN ({})",
            sch_bid_xaxis_data_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(sch_bid_xaxis_data_gte) = &query_filter.sch_bid_xaxis_data_gte {
        query.push_str(&format!(
            "
    AND sch_bid_xaxis_data >= {}",
            sch_bid_xaxis_data_gte
        ));
    }
    if let Some(sch_bid_xaxis_data_lte) = &query_filter.sch_bid_xaxis_data_lte {
        query.push_str(&format!(
            "
    AND sch_bid_xaxis_data <= {}",
            sch_bid_xaxis_data_lte
        ));
    }
    if let Some(sch_bid_y1axis_data) = &query_filter.sch_bid_y1axis_data {
        query.push_str(&format!(
            "
    AND sch_bid_y1axis_data = {}",
            sch_bid_y1axis_data
        ));
    }
    if let Some(sch_bid_y1axis_data_in) = &query_filter.sch_bid_y1axis_data_in {
        query.push_str(&format!(
            "
    AND sch_bid_y1axis_data IN ({})",
            sch_bid_y1axis_data_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(sch_bid_y1axis_data_gte) = &query_filter.sch_bid_y1axis_data_gte {
        query.push_str(&format!(
            "
    AND sch_bid_y1axis_data >= {}",
            sch_bid_y1axis_data_gte
        ));
    }
    if let Some(sch_bid_y1axis_data_lte) = &query_filter.sch_bid_y1axis_data_lte {
        query.push_str(&format!(
            "
    AND sch_bid_y1axis_data <= {}",
            sch_bid_y1axis_data_lte
        ));
    }
    if let Some(sch_bid_y2axis_data) = &query_filter.sch_bid_y2axis_data {
        query.push_str(&format!(
            "
    AND sch_bid_y2axis_data = {}",
            sch_bid_y2axis_data
        ));
    }
    if let Some(sch_bid_y2axis_data_in) = &query_filter.sch_bid_y2axis_data_in {
        query.push_str(&format!(
            "
    AND sch_bid_y2axis_data IN ({})",
            sch_bid_y2axis_data_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(sch_bid_y2axis_data_gte) = &query_filter.sch_bid_y2axis_data_gte {
        query.push_str(&format!(
            "
    AND sch_bid_y2axis_data >= {}",
            sch_bid_y2axis_data_gte
        ));
    }
    if let Some(sch_bid_y2axis_data_lte) = &query_filter.sch_bid_y2axis_data_lte {
        query.push_str(&format!(
            "
    AND sch_bid_y2axis_data <= {}",
            sch_bid_y2axis_data_lte
        ));
    }
    if let Some(sch_bid_curve_type) = &query_filter.sch_bid_curve_type {
        query.push_str(&format!(
            "
    AND sch_bid_curve_type = '{}'",
            sch_bid_curve_type
        ));
    }
    if let Some(sch_bid_curve_type_in) = &query_filter.sch_bid_curve_type_in {
        query.push_str(&format!(
            "
    AND sch_bid_curve_type IN ('{}')",
            sch_bid_curve_type_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(min_eoh_state_of_charge) = &query_filter.min_eoh_state_of_charge {
        query.push_str(&format!(
            "
    AND min_eoh_state_of_charge = {}",
            min_eoh_state_of_charge
        ));
    }
    if let Some(min_eoh_state_of_charge_in) = &query_filter.min_eoh_state_of_charge_in {
        query.push_str(&format!(
            "
    AND min_eoh_state_of_charge IN ({})",
            min_eoh_state_of_charge_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(min_eoh_state_of_charge_gte) = &query_filter.min_eoh_state_of_charge_gte {
        query.push_str(&format!(
            "
    AND min_eoh_state_of_charge >= {}",
            min_eoh_state_of_charge_gte
        ));
    }
    if let Some(min_eoh_state_of_charge_lte) = &query_filter.min_eoh_state_of_charge_lte {
        query.push_str(&format!(
            "
    AND min_eoh_state_of_charge <= {}",
            min_eoh_state_of_charge_lte
        ));
    }
    if let Some(max_eoh_state_of_charge) = &query_filter.max_eoh_state_of_charge {
        query.push_str(&format!(
            "
    AND max_eoh_state_of_charge = {}",
            max_eoh_state_of_charge
        ));
    }
    if let Some(max_eoh_state_of_charge_in) = &query_filter.max_eoh_state_of_charge_in {
        query.push_str(&format!(
            "
    AND max_eoh_state_of_charge IN ({})",
            max_eoh_state_of_charge_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(max_eoh_state_of_charge_gte) = &query_filter.max_eoh_state_of_charge_gte {
        query.push_str(&format!(
            "
    AND max_eoh_state_of_charge >= {}",
            max_eoh_state_of_charge_gte
        ));
    }
    if let Some(max_eoh_state_of_charge_lte) = &query_filter.max_eoh_state_of_charge_lte {
        query.push_str(&format!(
            "
    AND max_eoh_state_of_charge <= {}",
            max_eoh_state_of_charge_lte
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
            TimeZone::get("America/Los_Angeles").unwrap(),
        );
        let _n1 = match row.get_ref_unwrap(1).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum resource_type"),
        };
        let resource_type = ResourceType::from_str(&_n1).unwrap();
        let scheduling_coordinator_seq: u32 = row.get::<usize, u32>(2)?;
        let resource_bid_seq: u32 = row.get::<usize, u32>(3)?;
        let _micros4: Option<i64> = row.get::<usize, Option<i64>>(4)?;
        let time_interval_start = _micros4.map(|micros| {
            Zoned::new(
                Timestamp::from_microsecond(micros).unwrap(),
                TimeZone::get("America/Los_Angeles").unwrap(),
            )
        });
        let _micros5: Option<i64> = row.get::<usize, Option<i64>>(5)?;
        let time_interval_end = _micros5.map(|micros| {
            Zoned::new(
                Timestamp::from_microsecond(micros).unwrap(),
                TimeZone::get("America/Los_Angeles").unwrap(),
            )
        });
        let product_bid_desc: Option<String> = row.get::<usize, Option<String>>(6)?;
        let product_bid_mrid: Option<String> = row.get::<usize, Option<String>>(7)?;
        let market_product_desc: Option<String> = row.get::<usize, Option<String>>(8)?;
        let market_product_type: Option<String> = row.get::<usize, Option<String>>(9)?;
        let self_sched_mw: Option<Decimal> = match row.get_ref_unwrap(10) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let _micros11: Option<i64> = row.get::<usize, Option<i64>>(11)?;
        let sch_bid_time_interval_start = _micros11.map(|micros| {
            Zoned::new(
                Timestamp::from_microsecond(micros).unwrap(),
                TimeZone::get("America/Los_Angeles").unwrap(),
            )
        });
        let _micros12: Option<i64> = row.get::<usize, Option<i64>>(12)?;
        let sch_bid_time_interval_end = _micros12.map(|micros| {
            Zoned::new(
                Timestamp::from_microsecond(micros).unwrap(),
                TimeZone::get("America/Los_Angeles").unwrap(),
            )
        });
        let sch_bid_xaxis_data: Option<Decimal> = match row.get_ref_unwrap(13) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let sch_bid_y1axis_data: Option<Decimal> = match row.get_ref_unwrap(14) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let sch_bid_y2axis_data: Option<Decimal> = match row.get_ref_unwrap(15) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let _n16 = match row.get_ref_unwrap(16).to_owned() {
            duckdb::types::Value::Enum(v) => Some(v),
            duckdb::types::Value::Null => None,
            v => panic!("Unexpected value type {v:?} for enum sch_bid_curve_type"),
        };
        let sch_bid_curve_type = _n16.map(|s| SchBidCurveType::from_str(&s).unwrap());
        let min_eoh_state_of_charge: Option<Decimal> = match row.get_ref_unwrap(17) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let max_eoh_state_of_charge: Option<Decimal> = match row.get_ref_unwrap(18) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        Ok(Record {
            hour_beginning,
            resource_type,
            scheduling_coordinator_seq,
            resource_bid_seq,
            time_interval_start,
            time_interval_end,
            product_bid_desc,
            product_bid_mrid,
            market_product_desc,
            market_product_type,
            self_sched_mw,
            sch_bid_time_interval_start,
            sch_bid_time_interval_end,
            sch_bid_xaxis_data,
            sch_bid_y1axis_data,
            sch_bid_y2axis_data,
            sch_bid_curve_type,
            min_eoh_state_of_charge,
            max_eoh_state_of_charge,
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
    pub resource_type: Option<ResourceType>,
    pub resource_type_in: Option<Vec<ResourceType>>,
    pub scheduling_coordinator_seq: Option<u32>,
    pub scheduling_coordinator_seq_in: Option<Vec<u32>>,
    pub scheduling_coordinator_seq_gte: Option<u32>,
    pub scheduling_coordinator_seq_lte: Option<u32>,
    pub resource_bid_seq: Option<u32>,
    pub resource_bid_seq_in: Option<Vec<u32>>,
    pub resource_bid_seq_gte: Option<u32>,
    pub resource_bid_seq_lte: Option<u32>,
    pub time_interval_start: Option<Zoned>,
    pub time_interval_start_gte: Option<Zoned>,
    pub time_interval_start_lt: Option<Zoned>,
    pub time_interval_end: Option<Zoned>,
    pub time_interval_end_gte: Option<Zoned>,
    pub time_interval_end_lt: Option<Zoned>,
    pub product_bid_desc: Option<String>,
    pub product_bid_desc_like: Option<String>,
    pub product_bid_desc_in: Option<Vec<String>>,
    pub product_bid_mrid: Option<String>,
    pub product_bid_mrid_like: Option<String>,
    pub product_bid_mrid_in: Option<Vec<String>>,
    pub market_product_desc: Option<String>,
    pub market_product_desc_like: Option<String>,
    pub market_product_desc_in: Option<Vec<String>>,
    pub market_product_type: Option<String>,
    pub market_product_type_like: Option<String>,
    pub market_product_type_in: Option<Vec<String>>,
    pub self_sched_mw: Option<Decimal>,
    pub self_sched_mw_in: Option<Vec<Decimal>>,
    pub self_sched_mw_gte: Option<Decimal>,
    pub self_sched_mw_lte: Option<Decimal>,
    pub sch_bid_time_interval_start: Option<Zoned>,
    pub sch_bid_time_interval_start_gte: Option<Zoned>,
    pub sch_bid_time_interval_start_lt: Option<Zoned>,
    pub sch_bid_time_interval_end: Option<Zoned>,
    pub sch_bid_time_interval_end_gte: Option<Zoned>,
    pub sch_bid_time_interval_end_lt: Option<Zoned>,
    pub sch_bid_xaxis_data: Option<Decimal>,
    pub sch_bid_xaxis_data_in: Option<Vec<Decimal>>,
    pub sch_bid_xaxis_data_gte: Option<Decimal>,
    pub sch_bid_xaxis_data_lte: Option<Decimal>,
    pub sch_bid_y1axis_data: Option<Decimal>,
    pub sch_bid_y1axis_data_in: Option<Vec<Decimal>>,
    pub sch_bid_y1axis_data_gte: Option<Decimal>,
    pub sch_bid_y1axis_data_lte: Option<Decimal>,
    pub sch_bid_y2axis_data: Option<Decimal>,
    pub sch_bid_y2axis_data_in: Option<Vec<Decimal>>,
    pub sch_bid_y2axis_data_gte: Option<Decimal>,
    pub sch_bid_y2axis_data_lte: Option<Decimal>,
    pub sch_bid_curve_type: Option<SchBidCurveType>,
    pub sch_bid_curve_type_in: Option<Vec<SchBidCurveType>>,
    pub min_eoh_state_of_charge: Option<Decimal>,
    pub min_eoh_state_of_charge_in: Option<Vec<Decimal>>,
    pub min_eoh_state_of_charge_gte: Option<Decimal>,
    pub min_eoh_state_of_charge_lte: Option<Decimal>,
    pub max_eoh_state_of_charge: Option<Decimal>,
    pub max_eoh_state_of_charge_in: Option<Vec<Decimal>>,
    pub max_eoh_state_of_charge_gte: Option<Decimal>,
    pub max_eoh_state_of_charge_lte: Option<Decimal>,
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
        if let Some(value) = &self.resource_type {
            params.insert("resource_type", value.to_string());
        }
        if let Some(value) = &self.resource_type_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("resource_type_in", joined);
        }
        if let Some(value) = &self.scheduling_coordinator_seq {
            params.insert("scheduling_coordinator_seq", value.to_string());
        }
        if let Some(value) = &self.scheduling_coordinator_seq_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("scheduling_coordinator_seq_in", joined);
        }
        if let Some(value) = &self.scheduling_coordinator_seq_gte {
            params.insert("scheduling_coordinator_seq_gte", value.to_string());
        }
        if let Some(value) = &self.scheduling_coordinator_seq_lte {
            params.insert("scheduling_coordinator_seq_lte", value.to_string());
        }
        if let Some(value) = &self.resource_bid_seq {
            params.insert("resource_bid_seq", value.to_string());
        }
        if let Some(value) = &self.resource_bid_seq_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("resource_bid_seq_in", joined);
        }
        if let Some(value) = &self.resource_bid_seq_gte {
            params.insert("resource_bid_seq_gte", value.to_string());
        }
        if let Some(value) = &self.resource_bid_seq_lte {
            params.insert("resource_bid_seq_lte", value.to_string());
        }
        if let Some(value) = &self.time_interval_start {
            params.insert("time_interval_start", value.to_string());
        }
        if let Some(value) = &self.time_interval_start_gte {
            params.insert("time_interval_start_gte", value.to_string());
        }
        if let Some(value) = &self.time_interval_start_lt {
            params.insert("time_interval_start_lt", value.to_string());
        }
        if let Some(value) = &self.time_interval_end {
            params.insert("time_interval_end", value.to_string());
        }
        if let Some(value) = &self.time_interval_end_gte {
            params.insert("time_interval_end_gte", value.to_string());
        }
        if let Some(value) = &self.time_interval_end_lt {
            params.insert("time_interval_end_lt", value.to_string());
        }
        if let Some(value) = &self.product_bid_desc {
            params.insert("product_bid_desc", value.to_string());
        }
        if let Some(value) = &self.product_bid_desc_like {
            params.insert("product_bid_desc_like", value.to_string());
        }
        if let Some(value) = &self.product_bid_desc_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("product_bid_desc_in", joined);
        }
        if let Some(value) = &self.product_bid_mrid {
            params.insert("product_bid_mrid", value.to_string());
        }
        if let Some(value) = &self.product_bid_mrid_like {
            params.insert("product_bid_mrid_like", value.to_string());
        }
        if let Some(value) = &self.product_bid_mrid_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("product_bid_mrid_in", joined);
        }
        if let Some(value) = &self.market_product_desc {
            params.insert("market_product_desc", value.to_string());
        }
        if let Some(value) = &self.market_product_desc_like {
            params.insert("market_product_desc_like", value.to_string());
        }
        if let Some(value) = &self.market_product_desc_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("market_product_desc_in", joined);
        }
        if let Some(value) = &self.market_product_type {
            params.insert("market_product_type", value.to_string());
        }
        if let Some(value) = &self.market_product_type_like {
            params.insert("market_product_type_like", value.to_string());
        }
        if let Some(value) = &self.market_product_type_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("market_product_type_in", joined);
        }
        if let Some(value) = &self.self_sched_mw {
            params.insert("self_sched_mw", value.to_string());
        }
        if let Some(value) = &self.self_sched_mw_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("self_sched_mw_in", joined);
        }
        if let Some(value) = &self.self_sched_mw_gte {
            params.insert("self_sched_mw_gte", value.to_string());
        }
        if let Some(value) = &self.self_sched_mw_lte {
            params.insert("self_sched_mw_lte", value.to_string());
        }
        if let Some(value) = &self.sch_bid_time_interval_start {
            params.insert("sch_bid_time_interval_start", value.to_string());
        }
        if let Some(value) = &self.sch_bid_time_interval_start_gte {
            params.insert("sch_bid_time_interval_start_gte", value.to_string());
        }
        if let Some(value) = &self.sch_bid_time_interval_start_lt {
            params.insert("sch_bid_time_interval_start_lt", value.to_string());
        }
        if let Some(value) = &self.sch_bid_time_interval_end {
            params.insert("sch_bid_time_interval_end", value.to_string());
        }
        if let Some(value) = &self.sch_bid_time_interval_end_gte {
            params.insert("sch_bid_time_interval_end_gte", value.to_string());
        }
        if let Some(value) = &self.sch_bid_time_interval_end_lt {
            params.insert("sch_bid_time_interval_end_lt", value.to_string());
        }
        if let Some(value) = &self.sch_bid_xaxis_data {
            params.insert("sch_bid_xaxis_data", value.to_string());
        }
        if let Some(value) = &self.sch_bid_xaxis_data_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("sch_bid_xaxis_data_in", joined);
        }
        if let Some(value) = &self.sch_bid_xaxis_data_gte {
            params.insert("sch_bid_xaxis_data_gte", value.to_string());
        }
        if let Some(value) = &self.sch_bid_xaxis_data_lte {
            params.insert("sch_bid_xaxis_data_lte", value.to_string());
        }
        if let Some(value) = &self.sch_bid_y1axis_data {
            params.insert("sch_bid_y1axis_data", value.to_string());
        }
        if let Some(value) = &self.sch_bid_y1axis_data_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("sch_bid_y1axis_data_in", joined);
        }
        if let Some(value) = &self.sch_bid_y1axis_data_gte {
            params.insert("sch_bid_y1axis_data_gte", value.to_string());
        }
        if let Some(value) = &self.sch_bid_y1axis_data_lte {
            params.insert("sch_bid_y1axis_data_lte", value.to_string());
        }
        if let Some(value) = &self.sch_bid_y2axis_data {
            params.insert("sch_bid_y2axis_data", value.to_string());
        }
        if let Some(value) = &self.sch_bid_y2axis_data_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("sch_bid_y2axis_data_in", joined);
        }
        if let Some(value) = &self.sch_bid_y2axis_data_gte {
            params.insert("sch_bid_y2axis_data_gte", value.to_string());
        }
        if let Some(value) = &self.sch_bid_y2axis_data_lte {
            params.insert("sch_bid_y2axis_data_lte", value.to_string());
        }
        if let Some(value) = &self.sch_bid_curve_type {
            params.insert("sch_bid_curve_type", value.to_string());
        }
        if let Some(value) = &self.sch_bid_curve_type_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("sch_bid_curve_type_in", joined);
        }
        if let Some(value) = &self.min_eoh_state_of_charge {
            params.insert("min_eoh_state_of_charge", value.to_string());
        }
        if let Some(value) = &self.min_eoh_state_of_charge_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("min_eoh_state_of_charge_in", joined);
        }
        if let Some(value) = &self.min_eoh_state_of_charge_gte {
            params.insert("min_eoh_state_of_charge_gte", value.to_string());
        }
        if let Some(value) = &self.min_eoh_state_of_charge_lte {
            params.insert("min_eoh_state_of_charge_lte", value.to_string());
        }
        if let Some(value) = &self.max_eoh_state_of_charge {
            params.insert("max_eoh_state_of_charge", value.to_string());
        }
        if let Some(value) = &self.max_eoh_state_of_charge_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("max_eoh_state_of_charge_in", joined);
        }
        if let Some(value) = &self.max_eoh_state_of_charge_gte {
            params.insert("max_eoh_state_of_charge_gte", value.to_string());
        }
        if let Some(value) = &self.max_eoh_state_of_charge_lte {
            params.insert("max_eoh_state_of_charge_lte", value.to_string());
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

    pub fn resource_type(mut self, value: ResourceType) -> Self {
        self.inner.resource_type = Some(value);
        self
    }

    pub fn resource_type_in(mut self, values_in: Vec<ResourceType>) -> Self {
        self.inner.resource_type_in = Some(values_in);
        self
    }

    pub fn scheduling_coordinator_seq(mut self, value: u32) -> Self {
        self.inner.scheduling_coordinator_seq = Some(value);
        self
    }

    pub fn scheduling_coordinator_seq_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.scheduling_coordinator_seq_in = Some(values_in);
        self
    }

    pub fn scheduling_coordinator_seq_gte(mut self, value: u32) -> Self {
        self.inner.scheduling_coordinator_seq_gte = Some(value);
        self
    }

    pub fn scheduling_coordinator_seq_lte(mut self, value: u32) -> Self {
        self.inner.scheduling_coordinator_seq_lte = Some(value);
        self
    }

    pub fn resource_bid_seq(mut self, value: u32) -> Self {
        self.inner.resource_bid_seq = Some(value);
        self
    }

    pub fn resource_bid_seq_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.resource_bid_seq_in = Some(values_in);
        self
    }

    pub fn resource_bid_seq_gte(mut self, value: u32) -> Self {
        self.inner.resource_bid_seq_gte = Some(value);
        self
    }

    pub fn resource_bid_seq_lte(mut self, value: u32) -> Self {
        self.inner.resource_bid_seq_lte = Some(value);
        self
    }

    pub fn time_interval_start(mut self, value: Zoned) -> Self {
        self.inner.time_interval_start = Some(value);
        self
    }

    pub fn time_interval_start_gte(mut self, value: Zoned) -> Self {
        self.inner.time_interval_start_gte = Some(value);
        self
    }

    pub fn time_interval_start_lt(mut self, value: Zoned) -> Self {
        self.inner.time_interval_start_lt = Some(value);
        self
    }

    pub fn time_interval_end(mut self, value: Zoned) -> Self {
        self.inner.time_interval_end = Some(value);
        self
    }

    pub fn time_interval_end_gte(mut self, value: Zoned) -> Self {
        self.inner.time_interval_end_gte = Some(value);
        self
    }

    pub fn time_interval_end_lt(mut self, value: Zoned) -> Self {
        self.inner.time_interval_end_lt = Some(value);
        self
    }

    pub fn product_bid_desc<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.product_bid_desc = Some(value.into());
        self
    }

    pub fn product_bid_desc_like(mut self, value_like: String) -> Self {
        self.inner.product_bid_desc_like = Some(value_like);
        self
    }

    pub fn product_bid_desc_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.product_bid_desc_in = Some(values_in);
        self
    }

    pub fn product_bid_mrid<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.product_bid_mrid = Some(value.into());
        self
    }

    pub fn product_bid_mrid_like(mut self, value_like: String) -> Self {
        self.inner.product_bid_mrid_like = Some(value_like);
        self
    }

    pub fn product_bid_mrid_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.product_bid_mrid_in = Some(values_in);
        self
    }

    pub fn market_product_desc<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.market_product_desc = Some(value.into());
        self
    }

    pub fn market_product_desc_like(mut self, value_like: String) -> Self {
        self.inner.market_product_desc_like = Some(value_like);
        self
    }

    pub fn market_product_desc_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.market_product_desc_in = Some(values_in);
        self
    }

    pub fn market_product_type<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.market_product_type = Some(value.into());
        self
    }

    pub fn market_product_type_like(mut self, value_like: String) -> Self {
        self.inner.market_product_type_like = Some(value_like);
        self
    }

    pub fn market_product_type_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.market_product_type_in = Some(values_in);
        self
    }

    pub fn self_sched_mw(mut self, value: Decimal) -> Self {
        self.inner.self_sched_mw = Some(value);
        self
    }

    pub fn self_sched_mw_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.self_sched_mw_in = Some(values_in);
        self
    }

    pub fn self_sched_mw_gte(mut self, value: Decimal) -> Self {
        self.inner.self_sched_mw_gte = Some(value);
        self
    }

    pub fn self_sched_mw_lte(mut self, value: Decimal) -> Self {
        self.inner.self_sched_mw_lte = Some(value);
        self
    }

    pub fn sch_bid_time_interval_start(mut self, value: Zoned) -> Self {
        self.inner.sch_bid_time_interval_start = Some(value);
        self
    }

    pub fn sch_bid_time_interval_start_gte(mut self, value: Zoned) -> Self {
        self.inner.sch_bid_time_interval_start_gte = Some(value);
        self
    }

    pub fn sch_bid_time_interval_start_lt(mut self, value: Zoned) -> Self {
        self.inner.sch_bid_time_interval_start_lt = Some(value);
        self
    }

    pub fn sch_bid_time_interval_end(mut self, value: Zoned) -> Self {
        self.inner.sch_bid_time_interval_end = Some(value);
        self
    }

    pub fn sch_bid_time_interval_end_gte(mut self, value: Zoned) -> Self {
        self.inner.sch_bid_time_interval_end_gte = Some(value);
        self
    }

    pub fn sch_bid_time_interval_end_lt(mut self, value: Zoned) -> Self {
        self.inner.sch_bid_time_interval_end_lt = Some(value);
        self
    }

    pub fn sch_bid_xaxis_data(mut self, value: Decimal) -> Self {
        self.inner.sch_bid_xaxis_data = Some(value);
        self
    }

    pub fn sch_bid_xaxis_data_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.sch_bid_xaxis_data_in = Some(values_in);
        self
    }

    pub fn sch_bid_xaxis_data_gte(mut self, value: Decimal) -> Self {
        self.inner.sch_bid_xaxis_data_gte = Some(value);
        self
    }

    pub fn sch_bid_xaxis_data_lte(mut self, value: Decimal) -> Self {
        self.inner.sch_bid_xaxis_data_lte = Some(value);
        self
    }

    pub fn sch_bid_y1axis_data(mut self, value: Decimal) -> Self {
        self.inner.sch_bid_y1axis_data = Some(value);
        self
    }

    pub fn sch_bid_y1axis_data_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.sch_bid_y1axis_data_in = Some(values_in);
        self
    }

    pub fn sch_bid_y1axis_data_gte(mut self, value: Decimal) -> Self {
        self.inner.sch_bid_y1axis_data_gte = Some(value);
        self
    }

    pub fn sch_bid_y1axis_data_lte(mut self, value: Decimal) -> Self {
        self.inner.sch_bid_y1axis_data_lte = Some(value);
        self
    }

    pub fn sch_bid_y2axis_data(mut self, value: Decimal) -> Self {
        self.inner.sch_bid_y2axis_data = Some(value);
        self
    }

    pub fn sch_bid_y2axis_data_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.sch_bid_y2axis_data_in = Some(values_in);
        self
    }

    pub fn sch_bid_y2axis_data_gte(mut self, value: Decimal) -> Self {
        self.inner.sch_bid_y2axis_data_gte = Some(value);
        self
    }

    pub fn sch_bid_y2axis_data_lte(mut self, value: Decimal) -> Self {
        self.inner.sch_bid_y2axis_data_lte = Some(value);
        self
    }

    pub fn sch_bid_curve_type(mut self, value: SchBidCurveType) -> Self {
        self.inner.sch_bid_curve_type = Some(value);
        self
    }

    pub fn sch_bid_curve_type_in(mut self, values_in: Vec<SchBidCurveType>) -> Self {
        self.inner.sch_bid_curve_type_in = Some(values_in);
        self
    }

    pub fn min_eoh_state_of_charge(mut self, value: Decimal) -> Self {
        self.inner.min_eoh_state_of_charge = Some(value);
        self
    }

    pub fn min_eoh_state_of_charge_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.min_eoh_state_of_charge_in = Some(values_in);
        self
    }

    pub fn min_eoh_state_of_charge_gte(mut self, value: Decimal) -> Self {
        self.inner.min_eoh_state_of_charge_gte = Some(value);
        self
    }

    pub fn min_eoh_state_of_charge_lte(mut self, value: Decimal) -> Self {
        self.inner.min_eoh_state_of_charge_lte = Some(value);
        self
    }

    pub fn max_eoh_state_of_charge(mut self, value: Decimal) -> Self {
        self.inner.max_eoh_state_of_charge = Some(value);
        self
    }

    pub fn max_eoh_state_of_charge_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.max_eoh_state_of_charge_in = Some(values_in);
        self
    }

    pub fn max_eoh_state_of_charge_gte(mut self, value: Decimal) -> Self {
        self.inner.max_eoh_state_of_charge_gte = Some(value);
        self
    }

    pub fn max_eoh_state_of_charge_lte(mut self, value: Decimal) -> Self {
        self.inner.max_eoh_state_of_charge_lte = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {

    use duckdb::{AccessMode, Config, Connection};
    use jiff::civil::date;
    use log::info;
    use std::{error::Error, path::Path};

    use super::*;
    use crate::{db::prod_db::ProdDb, interval::month::month};

    #[ignore]
    #[tokio::test]
    async fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::caiso_public_bids();

        let months = month(2024, 1).up_to(month(2024, 1));
        for month in months.unwrap() {
            info!("Working on month {}", month);
            archive.download_missing_days(month).await?;
            archive.update_duckdb(&month)?;
        }
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn download_file() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::caiso_public_bids();
        archive.download_file(date(2025, 1, 1)).await?;
        Ok(())
    }

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::caiso_public_bids().duckdb_path, config).unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }
}
