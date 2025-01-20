use duckdb::{AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{civil::Date, ToSpan};

use crate::db::prod_db::ProdDb;
use actix_web::{get, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

#[get("/nrc/generator_status/unit_names")]
async fn api_get_names() -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let ids = get_names(&conn);
    match ids {
        Ok(vs) => HttpResponse::Ok().json(vs),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

/// Get the status for some/all facilities
/// http://127.0.0.1:8111/nrc/generator_status/start/2024-12-04/end/2024-12-08?unit_names=Calvert Cliffs 1,Byron 1
#[get("/nrc/generator_status/start/{start}/end/{end}")]
async fn api_status(
    path: web::Path<(Date, Date)>,
    query: web::Query<DataQuery>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let start_date = path.0;
    let end_date = path.1;
    let names: Option<Vec<String>> = query
        .unit_names
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.to_string()).collect());

    let res = get_status(&conn, start_date, end_date, names);
    match res {
        Ok(vs) => HttpResponse::Ok().json(vs),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct DataQuery {
    /// One or more facility ids, separated by ','.  For example: 'Calvert Cliffs 1,Byron 1'
    /// If [None], return all of them.  
    unit_names: Option<String>,
}

#[derive(Debug, PartialEq, Serialize)]
struct Row {
    name: String,
    date: Date,
    value: u8,
}

/// Get daily power status between a start and end date.
/// If `unit_names` is `None`, return all units.
fn get_status(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    ns: Option<Vec<String>>,
) -> Result<Vec<Row>> {
    let query = format!(
        r#"
SELECT ReportDt, Unit, Power
FROM Status 
WHERE ReportDt >= '{}'
AND ReportDt <= '{}'
{}
ORDER BY Unit, ReportDt;
    "#,
        start_date,
        end_date,
        match ns {
            Some(ids) => format!("AND Unit in ('{}')", ids.iter().join("','")),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(0).unwrap();
        Ok(Row {
            name: row.get::<usize, String>(1).unwrap(),
            date: Date::ZERO.checked_add(n.days()).unwrap(),
            value: row.get::<usize, u8>(2).unwrap(),
        })
    })?;
    let res: Vec<Row> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

fn get_names(conn: &Connection) -> Result<Vec<String>> {
    let query = "SELECT DISTINCT Unit FROM Status ORDER BY Unit;";
    let mut stmt = conn.prepare(query).unwrap();
    let res_iter = stmt.query_map([], |row| Ok(row.get::<usize, String>(0).unwrap()))?;
    let res: Vec<String> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

fn get_path() -> String {
    ProdDb::nrc_generator_status().duckdb_path.to_string()
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use serde_json::Value;

    use crate::api::nrc::generator_status::*;

    #[test]
    fn test_names() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        // get all facilities
        let names = get_names(&conn).unwrap();
        assert!(names.len() >= 110);
        Ok(())
    }

    #[test]
    fn test_status() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();

        // for all facilities
        let data = get_status(&conn, date(2024, 12, 4), date(2024, 12, 8), None).unwrap();
        assert!(data.iter().any(|e| e.name == "Byron 1"));
        assert!(data.len() == 470);

        // for one facility only
        let data = get_status(
            &conn,
            date(2024, 12, 4),
            date(2024, 12, 8),
            Some(vec!["Byron 1".to_owned()]),
        )
        .unwrap();
        assert!(data.len() == 5); // 5 days

        // for two facilities
        let data = get_status(
            &conn,
            date(2024, 12, 4),
            date(2024, 12, 8),
            Some(vec!["Byron 1".to_owned(), "Calvert Cliffs 1".to_owned()]),
        )
        .unwrap();
        assert!(data.len() == 10); // 5 days * 2 facilities

        Ok(())
    }

    #[test]
    fn api_status() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/hq/water_level/daily/start/2024-12-04/end/2024-12-08?station_ids=1-2951,1-9698",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Value = serde_json::from_str(&response).unwrap();
        if let Value::Array(xs) = v {
            assert_eq!(xs.len(), 10);
        }
        Ok(())
    }
}
