use jiff::civil::Date;
use log::{error, info};
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use std::process::Command;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use crate::utils::serde_helpers::*;
use jiff::Timestamp;
use jiff::{tz::TimeZone, Zoned};
use rust_decimal::Decimal;

use crate::interval::month::Month;

#[derive(Clone)]
pub struct IsoneDaBindingConstraintsArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IsoneDaBindingConstraintsArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/da_binding_constraints_final_"
            + &date.strftime("%Y%m%d").to_string()
            + ".json"
    }

    /// Upload one month to DuckDB.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting DA binding constraints files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS constraints (
    hour_beginning TIMESTAMPTZ NOT NULL,
    constraint_name VARCHAR NOT NULL,
    contingency_name VARCHAR NOT NULL,
    marginal_value DECIMAL(9,2) NOT NULL
);

CREATE TEMPORARY TABLE tmp AS
    SELECT 
        make_timestamptz(epoch_us(BeginDate)) AS hour_beginning,
        ConstraintName::VARCHAR AS constraint_name,
        ContingencyName::VARCHAR AS contingency_name,
        MarginalValue::DECIMAL(9,2) AS marginal_value
    FROM (
        SELECT unnest(DayAheadConstraints.DayAheadConstraint, recursive := true)
        FROM read_json('{}/Raw/{}/da_binding_constraints_final_{}*.json.gz')
    )
ORDER BY hour_beginning, constraint_name;

INSERT INTO constraints (
    SELECT * FROM tmp 
    WHERE NOT EXISTS (
        SELECT * FROM constraints d
        WHERE d.hour_beginning = tmp.hour_beginning
        AND d.constraint_name = tmp.constraint_name
        )
)
ORDER BY hour_beginning, constraint_name;
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
                "https://webservices.iso-ne.com/api/v1.1/dayaheadconstraints/day/{}",
                yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(&date)),
            true,
        )
    }

    /// Look for missing days.  Does not download current day.
    pub fn download_missing_days(&self, month: Month) -> Result<(), Box<dyn Error>> {
        let last = Zoned::now().date();
        for day in month.days() {
            if day >= last {
                // incomplete day, don't download
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
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    pub hour_beginning: Zoned,
    pub constraint_name: String,
    pub contingency_name: String,
    #[serde(with = "rust_decimal::serde::float")]
    pub marginal_value: Decimal,
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
    constraint_name,
    contingency_name,
    marginal_value
FROM constraints WHERE 1=1"#,
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
    if let Some(constraint_name) = &query_filter.constraint_name {
        query.push_str(&format!(
            "
    AND constraint_name = '{}'",
            constraint_name
        ));
    }
    if let Some(constraint_name_like) = &query_filter.constraint_name_like {
        query.push_str(&format!(
            "
    AND constraint_name LIKE '{}'",
            constraint_name_like
        ));
    }
    if let Some(constraint_name_in) = &query_filter.constraint_name_in {
        query.push_str(&format!(
            "
    AND constraint_name IN ('{}')",
            constraint_name_in
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
        let _micros0: i64 = row.get::<usize, i64>(0)?;
        let hour_beginning = Zoned::new(
            Timestamp::from_microsecond(_micros0).unwrap(),
            TimeZone::get("America/New_York").unwrap(),
        );
        let constraint_name: String = row.get::<usize, String>(1)?;
        let contingency_name: String = row.get::<usize, String>(2)?;
        let marginal_value: Decimal = match row.get_ref_unwrap(3) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Record {
            hour_beginning,
            constraint_name,
            contingency_name,
            marginal_value,
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
    pub constraint_name: Option<String>,
    pub constraint_name_like: Option<String>,
    pub constraint_name_in: Option<Vec<String>>,
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
        if let Some(value) = &self.constraint_name {
            params.insert("constraint_name", value.to_string());
        }
        if let Some(value) = &self.constraint_name_like {
            params.insert("constraint_name_like", value.to_string());
        }
        if let Some(value) = &self.constraint_name_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("constraint_name_in", joined);
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

    pub fn constraint_name<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.constraint_name = Some(value.into());
        self
    }

    pub fn constraint_name_like(mut self, value_like: String) -> Self {
        self.inner.constraint_name_like = Some(value_like);
        self
    }

    pub fn constraint_name_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.constraint_name_in = Some(values_in);
        self
    }
}

pub fn get_new_constraints(
    conn: &Connection,
    asof: &Date,
) -> Result<Vec<String>, Box<dyn std::error::Error>> {
    conn.execute(
        &format!("LOAD icu; SET VARIABLE asof_date = DATE '{}'", asof),
        [],
    )?;
    let query = String::from(
        r#"
SELECT constraint_name 
FROM (
    SELECT 
        constraint_name, 
        MIN(hour_beginning)::DATE AS first_appearance
    FROM constraints
    GROUP BY constraint_name
    ORDER BY first_appearance
)
WHERE first_appearance = getvariable('asof_date');
"#,
    );

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let constraint_name: String = row.get::<usize, String>(0)?;
        Ok(constraint_name)
    })?;
    let results: Vec<String> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[cfg(test)]
mod tests {
    use duckdb::AccessMode;
    use jiff::civil::date;
    use std::{error::Error, path::Path, time::Duration};

    use super::*;
    use crate::{
        db::prod_db::ProdDb,
        interval::{month::month, term::Term},
        utils::lib_duckdb::open_with_retry,
    };

    #[test]
    fn test_new_constraints() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_da_binding_constraints();
        let conn = open_with_retry(
            &archive.duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        )?;

        let new_constraints = get_new_constraints(&conn, &date(2026, 6, 26))?;
        assert_eq!(new_constraints, vec!["WOBURN365ALN".to_string()]);
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
        let archive = ProdDb::isone_da_binding_constraints();

        let months = month(2025, 1).up_to(month(2025, 7)).unwrap();
        for month in months {
            archive.download_missing_days(month)?;
        }
        // archive.update_duckdb(&month)?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_da_binding_constraints();
        let term: Term = "Cal23".parse().unwrap();
        for day in term.days() {
            archive.download_file(day)?;
        }
        Ok(())
    }
}
