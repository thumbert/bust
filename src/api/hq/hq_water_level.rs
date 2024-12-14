use duckdb::{AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{civil::Date, ToSpan};

use actix_web::{get, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};
use crate::db::prod_db::ProdDb;

// #[get("/hq/water_level/mra/bids_offers/participant_ids")]
// async fn participant_ids() -> impl Responder {
//     let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
//     let conn = Connection::open_with_flags(get_path(), config).unwrap();
//     let ids = get_participant_ids(conn);
//     HttpResponse::Ok().json(ids)
// }

/// Get the water level for some/all facilities
/// http://127.0.0.1:8111/hq/water_level/daily/start/2024-12-04/end/2024-12-08?station_ids=1-2951,1-9698
#[get("/hq/water_level/daily/start/{start}/end/{end}")]
async fn api_daily_level(
    path: web::Path<(Date, Date)>,
    query: web::Query<DataQuery>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let start_date = path.0;
    let end_date = path.1;
    let station_ids: Option<Vec<String>> = query
        .station_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.to_string()).collect());

    let res = get_water_level(&conn, start_date, end_date, station_ids).unwrap();
    HttpResponse::Ok().json(res)
}

#[derive(Debug, Deserialize)]
struct DataQuery {
    /// One or more facility ids, separated by ','.  For example: '1-2951,1-9698'
    /// If [None], return all of them.  Use carefully
    /// because it's a lot of data...
    station_ids: Option<String>,
}

// #[get("/isone/capacity/mra/results/zone/start/{start}/end/{end}")]
// async fn results_zone(path: web::Path<(String, String)>) -> impl Responder {
//     let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
//     let conn = Connection::open_with_flags(get_path(), config).unwrap();
//     let start = match path.0.replace('-', "").parse::<u32>() {
//         Ok(v) => v,
//         Err(e) => return HttpResponse::BadRequest().body(format!("Invalid start month. {}", e)),
//     };
//     let end = match path.1.replace('-', "").parse::<u32>() {
//         Ok(v) => v,
//         Err(e) => return HttpResponse::BadRequest().body(format!("Invalid end month. {}", e)),
//     };
//     let res = get_results_zone(conn, start, end).unwrap();
//     HttpResponse::Ok().json(res)
// }

#[derive(Debug, PartialEq, Serialize)]
struct Row {
    station_id: String,
    date: Date,
    value: f64,
}

/// Get daily average water level between a start and end date.
/// If `station_ids` is `None`, return all stations.
fn get_water_level(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    station_ids: Option<Vec<String>>,
) -> Result<Vec<Row>> {
    let query = format!(
        r#"
SELECT station_id,
       hour_beginning::DATE AS date, 
       round(mean(value),2) AS value
FROM WaterLevel 
WHERE date >= '{}'
AND date <= '{}'
{}
GROUP BY station_id, date
ORDER BY date;
    "#,
        start_date,
        end_date,
        match station_ids {
            Some(ids) => format!("AND station_id in ('{}')", ids.iter().join("','")),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(1).unwrap();
        Ok(Row {
            station_id: row.get::<usize, String>(0).unwrap(),
            date: Date::ZERO.checked_add(n.days()).unwrap(),
            value: row.get::<usize, f64>(2).unwrap(),
        })
    })?;
    let res: Vec<Row> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

fn get_path() -> String {
    ProdDb::hq_hydro_data().duckdb_path.to_string()
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use serde_json::Value;

    use crate::api::hq::hq_water_level::*;

    #[test]
    fn test_avg_level() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();

        // for all facilities
        let data = get_water_level(&conn, date(2024, 12, 4), date(2024, 12, 8), None).unwrap();
        assert!(data.len() == 1470);
        assert!(data.iter().any(|e| e.station_id == "1-2951"));

        // for one facility only
        let data = get_water_level(
            &conn,
            date(2024, 12, 4),
            date(2024, 12, 8),
            Some(vec!["1-2951".to_owned()]),
        )
        .unwrap();
        assert!(data.len() == 5); // 5 days

        // for two facilities
        let data = get_water_level(
            &conn,
            date(2024, 12, 4),
            date(2024, 12, 8),
            Some(vec!["1-2951".to_owned(), "1-9698".to_owned()]),
        )
        .unwrap();
        assert!(data.len() == 10); // 5 days * 2 facilities
        // println!("{:?}", data);


        Ok(())
    }

    #[test]
    fn api_water_level() -> Result<(), reqwest::Error> {
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
