use std::{collections::HashMap, time::Duration};

use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{
    arrow::array::StringArray,
    types::{EnumType::UInt8, ValueRef},
    AccessMode, Connection, Result, Row,
};
use itertools::Itertools;
use jiff::{civil::Date, ToSpan};
use rust_decimal::prelude::ToPrimitive;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::utils::lib_duckdb::open_with_retry;

/// Provides the DuckDB path for a given state's EPA hourly emissions database.
/// Implement this trait to plug in different path configurations (prod, test, etc.)
/// from any crate that registers the API routes.
pub trait EpaEmissionsDbProvider: Send + Sync {
    fn duckdb_path(&self, state: &str) -> String;
}

#[get("/epa/hourly_emissions/state/{state}/all_facilities")]
async fn all_facilities(
    path: web::Path<(String,)>,
    provider: web::Data<dyn EpaEmissionsDbProvider>,
) -> impl Responder {
    let db_path = provider.duckdb_path(&path.0);
    let conn = open_with_retry(&db_path, 8, Duration::from_millis(25), AccessMode::ReadOnly);
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database at {}: {}",
            &db_path,
            conn.err().unwrap(),
        ));
    }
    let conn = conn.unwrap();
    let names = get_units(&conn);
    HttpResponse::Ok().json(names.unwrap())
}

#[get("/epa/hourly_emissions/state/{state}/all_columns")]
async fn all_columns(
    path: web::Path<(String,)>,
    provider: web::Data<dyn EpaEmissionsDbProvider>,
) -> impl Responder {
    let db_path = provider.duckdb_path(&path.0);
    let conn = open_with_retry(&db_path, 8, Duration::from_millis(25), AccessMode::ReadOnly);
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database at {}: {}",
            &db_path,
            conn.err().unwrap(),
        ));
    }
    let conn = conn.unwrap();
    let names = get_column_names(&conn);
    HttpResponse::Ok().json(names.unwrap())
}

/// Get generation data between a start/end date for some units as specified in the query
/// http://127.0.0.1:8111/epa/state/ma/start/2023-01-01/end/2023-01-01?names=Mystic&columns=facility_name|unit_id|date|hour|gross_load_mw
#[get("/epa/hourly_emissions/state/{state}/start/{start}/end/{end}")]
async fn api_data(
    path: web::Path<(String, Date, Date)>,
    query: web::Query<DataQuery>,
    provider: web::Data<dyn EpaEmissionsDbProvider>,
) -> impl Responder {
    let db_path = provider.duckdb_path(&path.0);
    let conn = open_with_retry(&db_path, 8, Duration::from_millis(25), AccessMode::ReadOnly);
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database at {}: {}",
            &db_path,
            conn.err().unwrap(),
        ));
    }
    let conn = conn.unwrap();

    let start_date = path.1;
    let end_date = path.2;
    let unit_names: Option<Vec<String>> = query
        .facility_names
        .as_ref()
        .map(|ids| ids.split('|').map(|e| e.to_string()).collect());
    let columns: Option<Vec<String>> = query
        .columns
        .as_ref()
        .map(|ids| ids.split('|').map(|e| e.to_string()).collect());
    let non_null_generation_only = query.non_null_generation_only.unwrap_or(true);
    let data = get_data(
        &conn,
        start_date,
        end_date,
        unit_names,
        columns,
        non_null_generation_only,
    )
    .unwrap();
    HttpResponse::Ok().json(data)
}

#[derive(Debug, Deserialize)]
struct DataQuery {
    /// One or more facility names, separated by '|'.  For example: ''
    /// If [None], return all of them.  Use carefully
    /// because it's a lot of data...
    facility_names: Option<String>,
    /// Which columns of the data to return, a list of columns
    /// separated by '|'.  If [None] return all columns.
    columns: Option<String>,
    /// Return only the rows where the generation output is not null.
    /// Defaults to true
    non_null_generation_only: Option<bool>,
}

/// Get emission data between a `start` and  `end` date.
///
/// Restrict the units by providing a list of unit names.  If `unit_names` is [None]
/// return all of them.
///
/// Restrict the columns returned by specifying a list of `columns`.
///
/// If `non_null_generation_only` is [true] return only the rows with non null generation.
///
pub fn get_data(
    conn: &Connection,
    start: Date,
    end: Date,
    unit_names: Option<Vec<String>>,
    columns: Option<Vec<String>>,
    not_null_generation_only: bool,
) -> Result<Vec<HashMap<String, Value>>> {
    let ids = match columns {
        Some(ids) => ids,
        None => get_column_names(conn).unwrap(),
    };
    let query = format!(
        r#"
SELECT
    "{}"
FROM emissions
WHERE Date >= '{}'
AND Date <= '{}'
{}
{}
ORDER BY "facility_name", "unit_id", "date", "hour";
    "#,
        ids.join("\", \""),
        start.strftime("%Y-%m-%d"),
        end.strftime("%Y-%m-%d"),
        match unit_names {
            Some(ids) => format!("AND facility_name in ('{}') ", ids.iter().join("', '")),
            None => "".to_string(),
        },
        match not_null_generation_only {
            true => "AND gross_load IS NOT NULL".to_owned(),
            false => "".to_owned(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let mut one: HashMap<String, Value> = HashMap::new();
        for (i, id) in ids.iter().enumerate() {
            let value = match id.as_str() {
                "facility_name"
                | "unit_id"
                | "state"
                | "associated_stacks"
                | "primary_fuel_type"
                | "secondary_fuel_type"
                | "so2_controls"
                | "nox_controls"
                | "pm_controls"
                | "hg_controls"
                | "program_code" => match row.get::<usize, String>(i) {
                    Ok(v) => json!(v),
                    Err(_) => json!(Value::Null),
                },
                "facility_id" => json!(row.get::<usize, usize>(i)?),
                "date" => json!(Date::ZERO
                    .checked_add((719528 + row.get::<usize, i32>(i).unwrap()).days())
                    .unwrap()
                    .to_string()),
                "hour" => json!(row.get::<usize, u8>(i).unwrap()),
                "gross_load" => match row.get::<usize, u16>(i) {
                    Ok(v) => json!(v),
                    Err(_) => json!(Value::Null),
                },
                "operating_time" | "steam_load" | "so2_mass" | "so2_rate" | "co2_mass"
                | "co2_rate" | "nox_mass" | "nox_rate" | "heat_input" => {
                    match row.get_ref_unwrap(i) {
                        ValueRef::Decimal(v) => json!(v.to_f64().unwrap()),
                        _ => json!(Value::Null),
                    }
                }
                "so2_mass_measure_indicator"
                | "so2_rate_measure_indicator"
                | "co2_mass_measure_indicator"
                | "co2_rate_measure_indicator"
                | "nox_mass_measure_indicator"
                | "nox_rate_measure_indicator"
                | "heat_input_measure_indicator"
                | "unit_type" => get_enum(row, i),
                _ => json!(format!("Wrong column name {}", id)),
            };

            one.insert(id.to_owned(), value);
        }
        Ok(one)
    })?;
    let data: Vec<HashMap<String, Value>> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(data)
}

fn get_enum(row: &Row, idx: usize) -> Value {
    let value = match row.get_ref_unwrap(idx) {
        ValueRef::Enum(e, idx) => match e {
            UInt8(v) => v
                .values()
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap()
                .value(v.key(idx).unwrap()),
            _ => panic!("Unknown enum value"),
        },
        _ => panic!("Oops, column should be an enum"),
    };
    json!(value)
}

/// Get all the names of the units in this state.  
pub fn get_units(conn: &Connection) -> Result<Vec<String>> {
    let query = r#"
SELECT DISTINCT facility_name
FROM emissions
ORDER BY facility_name;    
    "#;
    let mut stmt = conn.prepare(query).unwrap();
    let names_iter = stmt.query_map([], |row| row.get(0))?;
    let names: Vec<String> = names_iter.map(|e| e.unwrap()).collect();
    Ok(names)
}

/// Get all the column names of the table.  
pub fn get_column_names(conn: &Connection) -> Result<Vec<String>> {
    let query = r#"SHOW emissions;"#;
    let mut stmt = conn.prepare(query).unwrap();
    let names_iter = stmt.query_map([], |row| row.get(0))?;
    let names: Vec<String> = names_iter.map(|e| e.unwrap()).collect();
    Ok(names)
}

#[cfg(test)]
mod tests {
    use duckdb::{AccessMode, Result};
    use jiff::civil::date;
    use std::{collections::HashSet, env, error::Error, path::Path};

    use super::*;
    use crate::db::prod_db::ProdDb;

    #[test]
    fn test_get_units() -> Result<(), Box<dyn Error>> {
        let conn = open_with_retry(
            &ProdDb::epa_hourly_emissions("MA").duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        );
        let xs = get_units(&conn.unwrap())?;
        assert!(xs.iter().any(|e| e == "Mystic"));
        Ok(())
    }

    #[test]
    fn test_get_column_names() -> Result<(), Box<dyn Error>> {
        let conn = open_with_retry(
            &ProdDb::epa_hourly_emissions("MA").duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        );
        let xs = get_column_names(&conn.unwrap())?;
        assert!(xs.iter().any(|e| e == "gross_load"));
        Ok(())
    }

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let conn = open_with_retry(
            &ProdDb::epa_hourly_emissions("MA").duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        )
        .unwrap();
        //
        // Query some columns
        //
        let xs = get_data(
            &conn,
            date(2023, 1, 6),
            date(2023, 1, 6),
            Some(vec!["Mystic".to_string()]),
            Some(vec![
                "facility_name".to_string(),
                "unit_id".to_string(),
                "date".to_string(),
                "hour".to_string(),
                "gross_load".to_string(),
            ]),
            true,
        )?;
        // println!("{:?}", xs);
        assert_eq!(xs.len(), 91);
        let keys: HashSet<_> = xs.first().unwrap().keys().map(|e| e.to_string()).collect();
        assert_eq!(
            keys,
            HashSet::from([
                "facility_name".to_string(),
                "unit_id".to_string(),
                "date".to_string(),
                "hour".to_string(),
                "gross_load".to_string(),
            ])
        );
        //
        // Query all the columns
        //
        let xs = get_data(
            &conn,
            date(2023, 1, 6),
            date(2023, 1, 6),
            Some(vec!["Mystic".to_string()]),
            None,
            // Some(vec!["SO2 Mass (lbs)".to_string()]),
            true,
        )?;
        // println!("{:?}", xs);
        assert_eq!(xs.len(), 91);
        // println!("{:?}", xs.first());
        let n = xs.first().unwrap().keys().len();
        assert_eq!(n, 32);

        Ok(())
    }

    #[test]
    fn api_all_facilities() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/epa/hourly_emissions/state/ma/all_facilities",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Value = serde_json::from_str(&response).unwrap();
        if let Value::Array(xs) = &v {
            assert!(xs.len() > 30);
            assert!(xs.contains(&Value::String("Mystic".to_owned())));
        }
        Ok(())
    }

    #[test]
    fn api_all_columns() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/epa/hourly_emissions/state/ma/all_columns",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Value = serde_json::from_str(&response).unwrap();
        if let Value::Array(xs) = &v {
            assert_eq!(xs.len(), 32);
            assert!(xs.contains(&Value::String("heat_input".to_owned())));
        }
        // println!("{:?}", &v);
        Ok(())
    }

    #[test]
    fn api_data() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/epa/hourly_emissions/state/ma/start/2023-01-01/end/2023-01-06?facility_names=Mystic&columns=facility_name|unit_id|date|hour|gross_load&non_null_generation_only=true",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Value = serde_json::from_str(&response).unwrap();
        if let Value::Array(xs) = v {
            // println!("{:?}", xs);
            assert_eq!(xs.len(), 134);
        }
        Ok(())
    }
}
