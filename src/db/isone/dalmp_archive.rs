use duckdb::Connection;
use jiff::civil::Date;
use jiff::Timestamp;
use jiff::{Zoned, tz::TimeZone};
use log::{error, info};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::Path;
use std::process::Command;

use crate::interval::month::Month;

#[derive(Debug, PartialEq)]
pub struct Row {
    hour_beginning: Zoned,
    ptid: u32,
    lmp: Decimal,
    mcc: Decimal,
    mlc: Decimal,
}

#[derive(Clone)]
pub struct IsoneDaLmpArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IsoneDaLmpArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/WW_DALMP_ISO_"
            + &date.strftime("%Y%m%d").to_string()
            + ".json"
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting daily DALMP hourly price files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS da_lmp (
    hour_beginning TIMESTAMPTZ NOT NULL,
    ptid UINTEGER NOT NULL,
    lmp DECIMAL(9,4) NOT NULL,
    mcc DECIMAL(9,4) NOT NULL,
    mcl DECIMAL(9,4) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT 
        BeginDate::TIMESTAMPTZ AS hour_beginning,
        "@LocId"::UINTEGER AS ptid,
        LmpTotal::DECIMAL(9,4) AS "lmp",
        CongestionComponent::DECIMAL(9,4) AS "mcc",
        LossComponent::DECIMAL(9,4) AS "mcl" 
    FROM (
        SELECT DISTINCT BeginDate, "@LocId", LmpTotal, CongestionComponent, LossComponent FROM (
            SELECT unnest(HourlyLmps.HourlyLmp, recursive := true)
            FROM read_json('{}/Raw/{}/WW_DALMP_ISO_{}*.json.gz')
        )
    )
    ORDER BY hour_beginning, ptid
;

INSERT INTO da_lmp
(SELECT * FROM tmp 
WHERE NOT EXISTS (
    SELECT * FROM da_lmp d
    WHERE d.hour_beginning = tmp.hour_beginning
    AND d.ptid = tmp.ptid
    )
)
ORDER BY hour_beginning, ptid;
"#,
            self.base_dir,
            month.start_date().year(),
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

    /// Data is usually published before 13:30 every day
    pub fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        super::lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/hourlylmp/da/final/day/{}",
                yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(&date)),
            true,
        )
    }

    /// Look for missing days
    pub fn download_missing_days(&self, month: Month) -> Result<(), Box<dyn Error>> {
        let mut last = Zoned::now().date();
        if Zoned::now().hour() > 13 {
            last = last.tomorrow()?;
        }
        for day in month.days() {
            if day > last {
                continue;
            }
            let fname = format!("{}.gz", self.filename(&day));
            if !Path::new(&fname).exists() {
                info!("Working on {}", day);
                self.download_file(day)?;
                info!("  downloaded file for {}", day);
            }
        }
        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub hour_beginning: Zoned,
    pub ptid: u32,
    #[serde(with = "rust_decimal::serde::float")]
    pub lmp: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub mcc: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub mcl: Decimal,
}

pub fn get_data(conn: &Connection, query_filter: &QueryFilter) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
   let mut query = String::from(r#"
SELECT
    hour_beginning,
    ptid,
    lmp,
    mcc,
    mcl
FROM da_lmp WHERE 1=1"#);
    if let Some(hour_beginning) = &query_filter.hour_beginning {
        query.push_str(&format!("
    AND hour_beginning = '{}'", hour_beginning.strftime("%Y-%m-%d %H:%M:%S.000%:z")));
    }
    if let Some(hour_beginning_gte) = &query_filter.hour_beginning_gte {
        query.push_str(&format!("
    AND hour_beginning >= '{}'", hour_beginning_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")));
    }
    if let Some(hour_beginning_lt) = &query_filter.hour_beginning_lt {
        query.push_str(&format!("
    AND hour_beginning < '{}'", hour_beginning_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")));
    }
    if let Some(ptid) = query_filter.ptid {
        query.push_str(&format!("
    AND ptid = {}", ptid));
    }
    if let Some(ptid_in) = &query_filter.ptid_in {
        query.push_str(&format!("
    AND ptid IN ({})", ptid_in.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")));
    }
    if let Some(ptid_gte) = query_filter.ptid_gte {
        query.push_str(&format!("
    AND ptid >= {}", ptid_gte));
    }
    if let Some(ptid_lte) = query_filter.ptid_lte {
        query.push_str(&format!("
    AND ptid <= {}", ptid_lte));
    }
    if let Some(lmp) = &query_filter.lmp {
        query.push_str(&format!("
    AND lmp = {}", lmp));
    }
    if let Some(lmp_in) = &query_filter.lmp_in {
        query.push_str(&format!("
    AND lmp IN ({})", lmp_in.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")));
    }
    if let Some(lmp_gte) = &query_filter.lmp_gte {
        query.push_str(&format!("
    AND lmp >= {}", lmp_gte));
    }
    if let Some(lmp_lte) = &query_filter.lmp_lte {
        query.push_str(&format!("
    AND lmp <= {}", lmp_lte));
    }
    if let Some(mcc) = &query_filter.mcc {
        query.push_str(&format!("
    AND mcc = {}", mcc));
    }
    if let Some(mcc_in) = &query_filter.mcc_in {
        query.push_str(&format!("
    AND mcc IN ({})", mcc_in.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")));
    }
    if let Some(mcc_gte) = &query_filter.mcc_gte {
        query.push_str(&format!("
    AND mcc >= {}", mcc_gte));
    }
    if let Some(mcc_lte) = &query_filter.mcc_lte {
        query.push_str(&format!("
    AND mcc <= {}", mcc_lte));
    }
    if let Some(mcl) = &query_filter.mcl {
        query.push_str(&format!("
    AND mcl = {}", mcl));
    }
    if let Some(mcl_in) = &query_filter.mcl_in {
        query.push_str(&format!("
    AND mcl IN ({})", mcl_in.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",")));
    }
    if let Some(mcl_gte) = &query_filter.mcl_gte {
        query.push_str(&format!("
    AND mcl >= {}", mcl_gte));
    }
    if let Some(mcl_lte) = &query_filter.mcl_lte {
        query.push_str(&format!("
    AND mcl <= {}", mcl_lte));
    }
    query.push(';');
    println!("query: {}", query);   

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let _micros0: i64 = row.get::<usize, i64>(0)?;
        let hour_beginning = Zoned::new(
                 Timestamp::from_microsecond(_micros0).unwrap(),
                 TimeZone::get("America/Los_Angeles").unwrap()
        );
        let ptid: u32 = row.get::<usize, u32>(1)?;
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
            hour_beginning,
            ptid,
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
    pub hour_beginning: Option<Zoned>,
    pub hour_beginning_gte: Option<Zoned>,
    pub hour_beginning_lt: Option<Zoned>,
    pub ptid: Option<u32>,
    pub ptid_in: Option<Vec<u32>>,
    pub ptid_gte: Option<u32>,
    pub ptid_lte: Option<u32>,
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

    pub fn ptid(mut self, value: u32) -> Self {
        self.inner.ptid = Some(value);
        self
    }

    pub fn ptid_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.ptid_in = Some(values_in);
        self
    }

    pub fn ptid_gte(mut self, value: u32) -> Self {
        self.inner.ptid_gte = Some(value);
        self
    }

    pub fn ptid_lte(mut self, value: u32) -> Self {
        self.inner.ptid_lte = Some(value);
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

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_dalmp();

        let months = month(2022, 1).up_to(month(2022, 2));
        for month in months.unwrap() {
            info!("Working on month {}", month);
            archive.download_missing_days(month)?;
            archive.update_duckdb(&month)?;
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_dalmp();
        archive.download_file(date(2025, 7, 1))?;
        Ok(())
    }

    #[test]
    fn test_get_data2() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_dalmp().duckdb_path, config).unwrap();
        conn.execute("LOAD ICU;SET TimeZone = 'America/New_York';", [])?;
        let filter = QueryFilterBuilder::new()
            .ptid_in(vec![4000, 4001])
            .hour_beginning_gte(
                date(2025, 12, 1)
                    .at(0, 0, 0, 0)
                    .in_tz("America/New_York")?,
            )
            .hour_beginning_lt(
                date(2025, 12, 2)
                    .at(0, 0, 0, 0)
                    .in_tz("America/New_York")?,
            )
            .build();
        let xs: Vec<Record> = get_data(&conn, &filter).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 48);  // two ptids, 24 hours
        let xs0 = xs
            .iter()
            .find(|r| {
                r.ptid == 4000
                    && r.hour_beginning
                        == date(2025, 12, 1)
                            .at(6, 0, 0, 0)
                            .in_tz("America/New_York")
                            .unwrap()
            })
            .unwrap();
        assert_eq!(
            *xs0,
            Record {
                ptid: 4000,
                hour_beginning: date(2025, 12, 1)
                    .at(6, 0, 0, 0)
                    .in_tz("America/New_York")?,
                lmp: dec!(72.41),
                mcc: dec!(0.02),
                mcl: dec!(0.16),
            }
        );
        Ok(())
    }
}
