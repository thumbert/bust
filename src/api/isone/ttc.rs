use std::error::Error;

use csv::Writer;
use duckdb::{AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{civil::Date, Timestamp};

use crate::db::isone::total_transfer_capability_archive::TotalTransferCapabilityArchive;
use actix_web::{get, web, HttpResponse, Responder};
use serde::Deserialize;

/// http://127.0.0.1:8111/ttc/start/2024-01-01/end/2024-01-04?columns=hq_phase2_import,ny_north_import&format=csv
#[get("/ttc/start/{start}/end/{end}")]
async fn api_ttc_data(
    path: web::Path<(Date, Date)>,
    query: web::Query<DataQuery>,
    db: web::Data<TotalTransferCapabilityArchive>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(db.duckdb_path.clone(), config).unwrap();
    let start_date = path.0;
    let end_date = path.1;
    let names: Option<Vec<String>> = query
        .columns
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.trim().to_string()).collect());

    let res = get_ttc_data(&conn, start_date, end_date, names);
    match res {
        Ok(vs) => HttpResponse::Ok().body(vs),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
struct DataQuery {
    /// One or more column names, separated by ','.  For example: 'hq_phase2_import,ny_north_import'
    /// If [None], return all of them.  
    columns: Option<String>,
    // Optional format for the response, e.g., 'csv' or 'json'.  If [None], defaults to 'json'.
    // format: Option<String>,
}


/// Get TTC data between a start and end date.
/// If `columns` is `None`, return all columns.
fn get_ttc_data(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    columns: Option<Vec<String>>,
) -> Result<String, Box<dyn Error>> {
    let c_names = match &columns {
        Some(xs) => xs.to_vec(),
        None => TotalTransferCapabilityArchive::all_columns(),
    };

    let query = format!(
        r#"
SELECT hour_beginning, {}
FROM ttc_limits 
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'
ORDER BY hour_beginning;
    "#,
        c_names.iter().map(|s| s.as_str()).join(", "),
        start_date
            .in_tz("America/New_York")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S%:z"),
        end_date
            .tomorrow()
            .unwrap()
            .in_tz("America/New_York")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S%:z"),
    );
    // println!("{}", query);

    let mut wtr = Writer::from_writer(vec![]);
    wtr.write_record(std::iter::once("hour_beginning").chain(c_names.iter().map(|s| s.as_str())))?;

    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let mut row_vec: Vec<String> = vec![];
        let ts = row.get::<usize, i64>(0).unwrap();
        let hb = Timestamp::from_microsecond(ts)
            .unwrap()
            .in_tz("America/New_York")
            .unwrap();
        row_vec.push(hb.strftime("%Y-%m-%d %H:%M:%S%:z").to_string()); // hour_beginning
        for i in 1..=c_names.len() {
            row_vec.push(row.get::<usize, i64>(i).unwrap().to_string());
        }
        Ok(row_vec)
    })?;

    for res in res_iter {
        wtr.write_record(res?)?; // Handle any errors that may occur during iteration
    }

    Ok(String::from_utf8(wtr.into_inner()?)?)
}


#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;

    use crate::{api::isone::ttc::*, db::{prod_db::ProdDb}};

    #[test]
    fn test_data() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_ttc().duckdb_path.clone(), config).unwrap();
        let data = get_ttc_data(
            &conn,
            date(2024, 1, 1),
            date(2024, 1, 4),
            Some(vec![
                "hq_phase2_import".to_string(),
                "ny_north_import".to_string(),
            ]),
        )
        .unwrap();
        println!("{}", data);
        Ok(())
    }

    #[test]
    fn api_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/ttc/start/2024-01-01/end/2024-01-04?columns=hq_phase2_import,ny_north_import&format=csv",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        println!("{}", response);
        // let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
        // assert_eq!(vs.len(), 10);
        // println!("{:?}", vs);
        Ok(())
    }
}
