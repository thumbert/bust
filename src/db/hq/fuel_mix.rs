// 15-minute data for total electricity demand in Quebec.
// https://donnees.hydroquebec.com/explore/dataset/demande-electricite-quebec/information/

use jiff::civil::*;
use log::error;
use log::info;
use reqwest::header;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;
use std::process::Command;

use std::collections::HashMap;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use crate::utils::serde_helpers::*;
use jiff::Timestamp;
use jiff::{tz::TimeZone, Zoned};

use crate::interval::month::Month;

pub struct HqFuelMixArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl HqFuelMixArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/fuel_mix_"
            + &date.to_string()
            + ".json"
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.   
    pub fn update_duckdb(&self, month: Month) -> Result<(), Box<dyn Error>> {
        info!("inserting HQ fuel mix files for month {} ...", month);

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS fuel_mix (
    zoned TIMESTAMPTZ NOT NULL,
    total INT64 NOT NULL,
    hydro INT64 NOT NULL,
    wind INT64 NOT NULL,
    solar INT64 NOT NULL,
    other INT64 NOT NULL,
    thermal INT64 NOT NULL
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT
        make_timestamptz(epoch_us("time")) AS zoned,
        total::INT64 AS total,
        hydraulique::INT64 AS hydro,
        eolien::INT64 AS wind,
        solaire::INT64 AS solar,
        autres::INT64 AS other,
        thermique::INT64 AS thermal,
    FROM (
        SELECT unnest(data, recursive := true)
        FROM read_json('{}/Raw/{}/fuel_mix_{}-*.json.gz')
    )
    WHERE total != 0
    ORDER BY zoned
;
INSERT INTO fuel_mix
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM fuel_mix d
        WHERE
            d.zoned = t.zoned AND
            d.total = t.total AND
            d.hydro = t.hydro
    )
) ORDER BY zoned; 
"#,
            self.base_dir,
            month.start_date().year(),
            month.strftime("%Y-%m")
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

    // I switched to this url which has data from 2024-01-01.  Issues with data at DST, some missing days, etc. 
    pub fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        let url = format!(
            "https://electricite-quebec.info/api/generation?start_date={}&end_date={}",
            date, date,
        );
        info!(
            "Downloading HQ fuel mix data for {} from url: {}",
            date, url
        );
        let client = reqwest::blocking::Client::builder()
            .danger_accept_invalid_certs(false)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");
        let resp = client
            .get(&url)
            .header(header::ACCEPT, "*/*")
            .header(header::ACCEPT_LANGUAGE, "en-US,en;q=0.9")
            .header(header::CONNECTION, "keep-alive")
            .header(header::COOKIE, "app_session=1")
            .header(header::REFERER, "https://electricite-quebec.info/data-hub")
            .header("Sec-Fetch-Dest", "empty")
            .header("Sec-Fetch-Mode", "cors")
            .header("Sec-Fetch-Site", "same-origin")
            .header(header::USER_AGENT, "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3")
            .header("X-App-Request", "1")
            .header("sec-ch-ua", r#""Google Chrome";v="149", "Not A(Brand";v="24", "Chromium";v="149""#)
            .header("sec-ch-ua-mobile", "?0")
            .header("sec-ch-ua-platform", r#""Windows""#)
            .send()
            .expect("request failed");

        // let resp = reqwest::blocking::get(url).expect("request failed");
        let body = resp.text().expect("body invalid");
        let path = &self.filename(&date);
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

    /// Different url, only 48 hours of current data available
    pub fn download_file2(&self) -> Result<(), Box<dyn Error>> {
        // this url has only the most recent 48 records, so not good for backfilling.
        // let _ = "https://donnees.hydroquebec.com/api/explore/v2.1/catalog/datasets/production-electricite-quebec/records?limit=100&order_by=date";
        // I switched to this url which has data from 2024-01-01
        let url = "https://donnees.hydroquebec.com/api/explore/v2.1/catalog/datasets/production-electricite-quebec/records?limit=100&order_by=date";
        info!(
            "Downloading HQ fuel mix data from url: {}",
            url
        );
        let client = reqwest::blocking::Client::builder()
            .danger_accept_invalid_certs(false)
            .timeout(std::time::Duration::from_secs(30))
            .build()
            .expect("Failed to build HTTP client");
        let resp = client
            .get(url)
            .send()
            .expect("request failed");

        // let resp = reqwest::blocking::get(url).expect("request failed");
        let body = resp.text().expect("body invalid");
        let day = Zoned::now().date();
        let path =         self.base_dir.to_owned()
            + "/Raw2/"
            + &day.year().to_string()
            + "/fuel_mix_"
            + &day.to_string()
            + ".json"
;
        let dir = Path::new(&path).parent().unwrap();
        let _ = fs::create_dir_all(dir);
        let mut out = File::create(&path).expect("failed to create file");
        io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");

        // gzip it
        Command::new("gzip")
            .args(["-f", &path])
            .current_dir(dir)
            .spawn()
            .unwrap()
            .wait()
            .expect("gzip failed");

        Ok(())
    }


}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    pub zoned: Zoned,
    pub total: i64,
    pub hydro: i64,
    pub wind: i64,
    pub solar: i64,
    pub other: i64,
    pub thermal: i64,
}

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
    limit: Option<usize>,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    zoned,
    total,
    hydro,
    wind,
    solar,
    other,
    thermal
FROM fuel_mix WHERE 1=1"#,
    );
    if let Some(zoned) = &query_filter.zoned {
        query.push_str(&format!(
            "
    AND zoned = '{}'",
            zoned.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(zoned_gte) = &query_filter.zoned_gte {
        query.push_str(&format!(
            "
    AND zoned >= '{}'",
            zoned_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(zoned_lt) = &query_filter.zoned_lt {
        query.push_str(&format!(
            "
    AND zoned < '{}'",
            zoned_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")
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
        let zoned = Zoned::new(
            Timestamp::from_microsecond(_micros0).unwrap(),
            TimeZone::get("America/New_York").unwrap(),
        );
        let total: i64 = row.get::<usize, i64>(1)?;
        let hydro: i64 = row.get::<usize, i64>(2)?;
        let wind: i64 = row.get::<usize, i64>(3)?;
        let solar: i64 = row.get::<usize, i64>(4)?;
        let other: i64 = row.get::<usize, i64>(5)?;
        let thermal: i64 = row.get::<usize, i64>(6)?;
        Ok(Record {
            zoned,
            total,
            hydro,
            wind,
            solar,
            other,
            thermal,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub zoned: Option<Zoned>,
    pub zoned_gte: Option<Zoned>,
    pub zoned_lt: Option<Zoned>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.zoned {
            params.insert("zoned", value.to_string());
        }
        if let Some(value) = &self.zoned_gte {
            params.insert("zoned_gte", value.to_string());
        }
        if let Some(value) = &self.zoned_lt {
            params.insert("zoned_lt", value.to_string());
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

    pub fn zoned(mut self, value: Zoned) -> Self {
        self.inner.zoned = Some(value);
        self
    }

    pub fn zoned_gte(mut self, value: Zoned) -> Self {
        self.inner.zoned_gte = Some(value);
        self
    }

    pub fn zoned_lt(mut self, value: Zoned) -> Self {
        self.inner.zoned_lt = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {

    use std::{error::Error, path::Path};

    use super::*;
    use crate::{db::prod_db::ProdDb, interval::term::Term};
    use duckdb::{AccessMode, Config, Connection};

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::scratch().duckdb_path, config).unwrap();
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
        let archive = ProdDb::hq_fuel_mix();
        let term = "Jan25-Jan26".parse::<Term>().unwrap();
        for m in term.months() {
            archive.update_duckdb(m)?;
        }

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::hq_fuel_mix();
        let term = "1Jan26-23Jun26".parse::<Term>().unwrap();
        for day in term.days() {
            // wait for 3 seconds between downloads to avoid overwhelming the server
            std::thread::sleep(std::time::Duration::from_secs(3));
            archive.download_file(day)?;
        }
        Ok(())
    }
}
