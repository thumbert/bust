// Auto-generated Rust stub for DuckDB table: lmp
// Created on 2025-12-15 with elec_server/utils/lib_duckdb_builder.dart

use duckdb::Connection;
use futures::StreamExt;
use itertools::Itertools;
use jiff::civil::Date;
use jiff::Timestamp;
use jiff::{tz::TimeZone, Zoned};
use log::{error, info};
use reqwest::get;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::Path;
use std::process::Command;
use tokio::fs::{self, File};
use tokio::io::AsyncReadExt;
use tokio::io::AsyncWriteExt;
use tokio_util::io::StreamReader;

use crate::db::nyiso::dalmp::LmpComponent;
use crate::interval::month::Month;

#[derive(Clone)]
pub struct CaisoDaLmpArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl CaisoDaLmpArchive {
    /// Return the csv filename for one component for the day.  Does not check if the file exists.  
    /// For example:
    ///  - 20251206_20251206_PRC_LMP_DAM_LMP_v12.csv
    ///  - 20251206_20251206_PRC_LMP_DAM_MCC_v12.csv
    ///  - 20251206_20251206_PRC_LMP_DAM_MCL_v12.csv
    pub fn filename(&self, date: &Date, component: LmpComponent) -> String {
        let yyyymmdd = date.strftime("%Y%m%d");
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + format!(
                "/{}_{}_PRC_LMP_DAM_{}_v12.csv",
                yyyymmdd,
                yyyymmdd,
                component.to_string().to_uppercase()
            )
            .as_str()
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting CAISO DALMP hourly prices for month {} ...",
            month
        );

        let sql = format!(
            r#"
LOAD icu;
SET TimeZone = 'America/Los_Angeles';

CREATE TABLE IF NOT EXISTS lmp (
    node_id VARCHAR NOT NULL,
    hour_beginning TIMESTAMPTZ NOT NULL,
    lmp DECIMAL(18,5) NOT NULL,
    mcc DECIMAL(18,5) NOT NULL,
    mcl DECIMAL(18,5) NOT NULL,
);
CREATE INDEX IF NOT EXISTS idx_lmp_node_id ON lmp(node_id);

CREATE TEMPORARY TABLE tmp_lmp 
AS 
    SELECT 
        "NODE_ID"::STRING AS node_id,
        "INTERVALSTARTTIME_GMT"::STRING AS hour_beginning,
        "MW"::DECIMAL(18,5) as lmp
    FROM read_csv(
            '{}/Raw/{}/{}*_{}*_PRC_LMP_DAM_LMP_v12.csv.gz',
            header = true
    )
    ORDER BY node_id, hour_beginning 
;

CREATE TEMPORARY TABLE tmp_mcc 
AS 
    SELECT 
        "NODE_ID"::STRING AS node_id,
        "INTERVALSTARTTIME_GMT"::STRING AS hour_beginning,
        "MW"::DECIMAL(18,5) as mcc
    FROM read_csv(
            '{}/Raw/{}/{}*_{}*_PRC_LMP_DAM_MCC_v12.csv.gz',
            header = true
    )
    ORDER BY node_id, hour_beginning 
;

CREATE TEMPORARY TABLE tmp_mcl 
AS 
    SELECT 
        "NODE_ID"::STRING AS node_id,
        "INTERVALSTARTTIME_GMT"::STRING AS hour_beginning,
        "MW"::DECIMAL(18,5) as mcl
    FROM read_csv(
            '{}/Raw/{}/{}*_{}*_PRC_LMP_DAM_MCL_v12.csv.gz',
            header = true
    )
    ORDER BY node_id, hour_beginning 
;

CREATE TEMPORARY TABLE tmp 
AS
    SELECT 
        l.node_id,
        l.hour_beginning,
        l.lmp,
        m.mcc,
        c.mcl
    FROM tmp_lmp l
    JOIN tmp_mcc m
        ON l.node_id = m.node_id
        AND l.hour_beginning = m.hour_beginning
    JOIN tmp_mcl c
        ON l.node_id = c.node_id
        AND l.hour_beginning = c.hour_beginning
    ORDER BY l.node_id, l.hour_beginning;


INSERT INTO lmp
(
    SELECT * FROM tmp
    WHERE NOT EXISTS 
    (
        SELECT 1 FROM lmp 
        WHERE lmp.node_id = tmp.node_id 
        AND lmp.hour_beginning = tmp.hour_beginning
    )
)
ORDER BY node_id, hour_beginning;
"#,
            self.base_dir,
            month.start_date().year(),
            month.start_date().strftime("%Y%m"),
            month.start_date().strftime("%Y%m"),
            self.base_dir,
            month.start_date().year(),
            month.start_date().strftime("%Y%m"),
            month.start_date().strftime("%Y%m"),
            self.base_dir,
            month.start_date().year(),
            month.start_date().strftime("%Y%m"),
            month.start_date().strftime("%Y%m"),
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

    /// Data is usually published every day before 18:00[America/New_York]
    /// It is a zip file which contains 4 csv files (one per component)
    /// https://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=12&startdatetime=20251206T08:00-0000&enddatetime=20251207T08:00-0000&market_run_id=DAM&grp_type=ALL
    pub async fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        let start = date.at(0, 0, 0, 0).in_tz("America/Los_Angeles")?;
        let start_z = start.in_tz("UTC")?.strftime("%Y%m%dT%H:%M-0000");
        let url = format!("https://oasis.caiso.com/oasisapi/SingleZip?resultformat=6&queryname=PRC_LMP&version=12&startdatetime={}&enddatetime={}&market_run_id=DAM&grp_type=ALL", start_z, start_z);
        let resp = get(&url).await?;
        let stream = resp.bytes_stream();
        let mut reader = StreamReader::new(stream.map(|r| r.map_err(std::io::Error::other)));
        let zip_path = self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + format!("/{}_{}_PRC_LMP_DAM_LMP_v12_csv.zip", yyyymmdd, yyyymmdd).as_str();
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

        // Remove the zip file
        tokio::fs::remove_file(&zip_path).await?;
        info!("removed zip file {}", zip_path);

        Ok(())
    }

    /// Look for missing days
    pub async fn download_missing_days(&self, month: Month) -> Result<(), Box<dyn Error>> {
        let mut last = Zoned::now().date();
        if Zoned::now().hour() > 18 {
            last = last.tomorrow()?;
        }
        for day in month.days() {
            if day > last {
                continue;
            }
            let fname = format!("{}.gz", self.filename(&day, LmpComponent::Lmp));
            if !Path::new(&fname).exists() {
                info!("Working on {}", day);
                self.download_file(day).await?;
                info!("  finished processing file for {}", day);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub node_id: String,
    pub hour_beginning: Zoned,
    #[serde(with = "rust_decimal::serde::float")]
    pub lmp: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub mcc: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub mcl: Decimal,
}

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    node_id,
    hour_beginning,
    lmp,
    mcc,
    mcl
FROM lmp WHERE 1=1"#,
    );
    if let Some(node_id) = &query_filter.node_id {
        query.push_str(&format!(
            "
    AND node_id = '{}'",
            node_id
        ));
    }
    if let Some(node_id_like) = &query_filter.node_id_like {
        query.push_str(&format!(
            "
    AND node_id LIKE '{}'",
            node_id_like
        ));
    }
    if let Some(node_id_in) = &query_filter.node_id_in {
        query.push_str(&format!(
            "
    AND node_id IN ('{}')",
            node_id_in.iter().join("','")
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
    if let Some(lmp) = &query_filter.lmp {
        query.push_str(&format!(
            "
    AND lmp = {}",
            lmp
        ));
    }
    if let Some(lmp_in) = &query_filter.lmp_in {
        query.push_str(&format!(
            "
    AND lmp IN ({})",
            lmp_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(lmp_gte) = &query_filter.lmp_gte {
        query.push_str(&format!(
            "
    AND lmp >= {}",
            lmp_gte
        ));
    }
    if let Some(lmp_lte) = &query_filter.lmp_lte {
        query.push_str(&format!(
            "
    AND lmp <= {}",
            lmp_lte
        ));
    }
    if let Some(mcc) = &query_filter.mcc {
        query.push_str(&format!(
            "
    AND mcc = {}",
            mcc
        ));
    }
    if let Some(mcc_in) = &query_filter.mcc_in {
        query.push_str(&format!(
            "
    AND mcc IN ({})",
            mcc_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(mcc_gte) = &query_filter.mcc_gte {
        query.push_str(&format!(
            "
    AND mcc >= {}",
            mcc_gte
        ));
    }
    if let Some(mcc_lte) = &query_filter.mcc_lte {
        query.push_str(&format!(
            "
    AND mcc <= {}",
            mcc_lte
        ));
    }
    if let Some(mcl) = &query_filter.mcl {
        query.push_str(&format!(
            "
    AND mcl = {}",
            mcl
        ));
    }
    if let Some(mcl_in) = &query_filter.mcl_in {
        query.push_str(&format!(
            "
    AND mcl IN ({})",
            mcl_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(mcl_gte) = &query_filter.mcl_gte {
        query.push_str(&format!(
            "
    AND mcl >= {}",
            mcl_gte
        ));
    }
    if let Some(mcl_lte) = &query_filter.mcl_lte {
        query.push_str(&format!(
            "
    AND mcl <= {}",
            mcl_lte
        ));
    }
    query.push(';');
    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let node_id: String = row.get::<usize, String>(0)?;
        let _micros1: i64 = row.get::<usize, i64>(1)?;
        let hour_beginning = Zoned::new(
            Timestamp::from_microsecond(_micros1).unwrap(),
            TimeZone::get("America/Los_Angeles").unwrap(),
        );
        let lmp: Decimal = match row.get_ref_unwrap(2) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let mcc: Decimal = match row.get_ref_unwrap(3) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let mcl: Decimal = match row.get_ref_unwrap(4) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Record {
            node_id,
            hour_beginning,
            lmp,
            mcc,
            mcl,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub node_id: Option<String>,
    pub node_id_like: Option<String>,
    pub node_id_in: Option<Vec<String>>,
    pub hour_beginning: Option<Zoned>,
    pub hour_beginning_gte: Option<Zoned>,
    pub hour_beginning_lt: Option<Zoned>,
    pub lmp: Option<Decimal>,
    pub lmp_in: Option<Vec<Decimal>>,
    pub lmp_gte: Option<Decimal>,
    pub lmp_lte: Option<Decimal>,
    pub mcc: Option<Decimal>,
    pub mcc_in: Option<Vec<Decimal>>,
    pub mcc_gte: Option<Decimal>,
    pub mcc_lte: Option<Decimal>,
    pub mcl: Option<Decimal>,
    pub mcl_in: Option<Vec<Decimal>>,
    pub mcl_gte: Option<Decimal>,
    pub mcl_lte: Option<Decimal>,
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

    pub fn node_id<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.node_id = Some(value.into());
        self
    }

    pub fn node_id_like(mut self, value_like: String) -> Self {
        self.inner.node_id_like = Some(value_like);
        self
    }

    pub fn node_id_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.node_id_in = Some(values_in);
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

    pub fn lmp(mut self, value: Decimal) -> Self {
        self.inner.lmp = Some(value);
        self
    }

    pub fn lmp_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.lmp_in = Some(values_in);
        self
    }

    pub fn lmp_gte(mut self, value: Decimal) -> Self {
        self.inner.lmp_gte = Some(value);
        self
    }

    pub fn lmp_lte(mut self, value: Decimal) -> Self {
        self.inner.lmp_lte = Some(value);
        self
    }

    pub fn mcc(mut self, value: Decimal) -> Self {
        self.inner.mcc = Some(value);
        self
    }

    pub fn mcc_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.mcc_in = Some(values_in);
        self
    }

    pub fn mcc_gte(mut self, value: Decimal) -> Self {
        self.inner.mcc_gte = Some(value);
        self
    }

    pub fn mcc_lte(mut self, value: Decimal) -> Self {
        self.inner.mcc_lte = Some(value);
        self
    }

    pub fn mcl(mut self, value: Decimal) -> Self {
        self.inner.mcl = Some(value);
        self
    }

    pub fn mcl_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.mcl_in = Some(values_in);
        self
    }

    pub fn mcl_gte(mut self, value: Decimal) -> Self {
        self.inner.mcl_gte = Some(value);
        self
    }

    pub fn mcl_lte(mut self, value: Decimal) -> Self {
        self.inner.mcl_lte = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use duckdb::{AccessMode, Config, Connection};
    use jiff::civil::date;
    use log::info;
    use rust_decimal_macros::dec;
    use std::{error::Error, path::Path};

    use super::*;
    use crate::{db::prod_db::ProdDb, interval::month::month};

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::caiso_dalmp().duckdb_path, config).unwrap();
        conn.execute("LOAD ICU;SET TimeZone = 'America/Los_Angeles';", [])?;
        let filter = QueryFilterBuilder::new()
            .node_id("TH_NP15_GEN_ONPEAK-APND")
            .hour_beginning_gte(
                date(2025, 12, 1)
                    .at(0, 0, 0, 0)
                    .in_tz("America/Los_Angeles")?,
            )
            .hour_beginning_lt(
                date(2025, 12, 2)
                    .at(0, 0, 0, 0)
                    .in_tz("America/Los_Angeles")?,
            )
            .build();
        let xs: Vec<Record> = get_data(&conn, &filter).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 16);
        assert_eq!(
            xs[0],
            Record {
                node_id: "TH_NP15_GEN_ONPEAK-APND".to_string(),
                hour_beginning: date(2025, 12, 1)
                    .at(6, 0, 0, 0)
                    .in_tz("America/Los_Angeles")?,
                lmp: dec!(65.50000),
                mcc: dec!(-0.36631),
                mcl: dec!(-1.05060),
            }
        );
        Ok(())
    }

    #[test]
    fn test_get_data2() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::caiso_dalmp().duckdb_path, config).unwrap();
        conn.execute("LOAD ICU;SET TimeZone = 'America/Los_Angeles';", [])?;
        let filter = QueryFilterBuilder::new()
            .node_id_in(vec![
                "TH_NP15_GEN-APND".to_string(),
                "TH_SP15_GEN-APND".to_string(),
            ])
            .hour_beginning_gte(
                date(2025, 12, 1)
                    .at(0, 0, 0, 0)
                    .in_tz("America/Los_Angeles")?,
            )
            .hour_beginning_lt(
                date(2025, 12, 2)
                    .at(0, 0, 0, 0)
                    .in_tz("America/Los_Angeles")?,
            )
            .build();
        let xs: Vec<Record> = get_data(&conn, &filter).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 48);
        let xs0 = xs
            .iter()
            .find(|r| {
                r.node_id == "TH_NP15_GEN-APND"
                    && r.hour_beginning
                        == date(2025, 12, 1)
                            .at(6, 0, 0, 0)
                            .in_tz("America/Los_Angeles")
                            .unwrap()
            })
            .unwrap();
        assert_eq!(
            *xs0,
            Record {
                node_id: "TH_NP15_GEN-APND".to_string(),
                hour_beginning: date(2025, 12, 1)
                    .at(6, 0, 0, 0)
                    .in_tz("America/Los_Angeles")?,
                lmp: dec!(65.50000),
                mcc: dec!(-0.36631),
                mcl: dec!(-1.05060),
            }
        );
        Ok(())
    }

    #[ignore]
    #[tokio::test]
    async fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::caiso_dalmp();

        let months = month(2026, 1).up_to(month(2026, 1));
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
        let archive = ProdDb::caiso_dalmp();
        archive.download_file(date(2025, 12, 3)).await?;
        Ok(())
    }
}
