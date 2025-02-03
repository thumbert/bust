use std::error::Error;

use duckdb::{AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{civil::Date, tz::TimeZone, Timestamp, ToSpan, Zoned};

use crate::db::{isone::mis::sr_rsvcharge2::RowTab5, prod_db::ProdDb};
use actix_web::{get, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct DataQuery {
    /// Which version of the data to return.  If [None], return all versions.
    version: Option<u8>,
    /// Restrict the data to this account only.  If [None], return data from all accounts.
    account_id: Option<usize>,
}


/// Get the report for one tab between a start/end date (hourly data)
/// http://127.0.0.1:8111/isone/mis/sr_rsvcharge2/tab/5/start/2024-12-04/end/2024-12-08?version=99
#[get("/isone/mis/sr_rsvcharge2/tab/{tab}/start/{start}/end/{end}")]
async fn api_tab_data(
    path: web::Path<(u8, Date, Date)>,
    query: web::Query<DataQuery>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let tab = path.0;
    let start_date = path.1;
    let end_date = path.2;

    match tab {
        5 => match get_tab5_data(&conn, start_date, end_date, query.account_id) {
            Ok(rows) => HttpResponse::Ok().json(rows),
            Err(e) => HttpResponse::InternalServerError()
                .body(format!("Failed to get data from DuckDB. {}", e)),
        },
        _ => HttpResponse::BadRequest().body(format!(
            "Invalid tab {} value.  Only value 0 is supported.",
            tab
        )),
    }
}

#[derive(Debug, Deserialize)]
struct DataQuery2 {
    /// Restrict the data to these subaccounts only.  If [None], return data aggregated for 
    /// all subaccounts.  Subaccount names are separated by "|".
    subaccount_ids: Option<String>,
}

#[get("/isone/mis/sr_rsvcharge2/daily/charges/account_id/{account_id}/start/{start}/end/{end}/settlement/{settlement}")]
async fn api_daily_charges(
    path: web::Path<(usize, Date, Date, u8)>,
    query: web::Query<DataQuery2>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let account_id = path.0;
    let start_date = path.1;
    let end_date = path.2;
    let settlement = path.3;

    let subaccount_ids: Option<Vec<String>> = query
        .subaccount_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.to_string()).collect());


    match get_daily_charges(&conn, account_id, start_date, end_date, settlement, subaccount_ids) {
        Ok(rows) => HttpResponse::Ok().json(rows),
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("Failed to get data from DuckDB. {}", e)),
    }
}

fn get_daily_charges(
    conn: &Connection,
    account_id: usize,
    start_date: Date,
    end_date: Date,
    settlement: u8,
    subaccount_ids: Option<Vec<String>>,
) -> Result<Vec<RowTab5>, Box<dyn Error>> {
    let query = format!(
        r#"
SELECT *
FROM tab5 
WHERE report_date >= '{}'
AND report_date <= '{}'
AND account_id = {}
{}
ORDER BY subaccount_id, report_date, hour_beginning;
    "#,
        account_id,
        start_date,
        end_date,
        match subaccount_ids {
            Some(ids) => format!("AND subaccount_id in ('{}')", ids.iter().join("','")),
            None => "".to_string(),
        }
    );
    println!("{}", query);
    let time_zone = TimeZone::get("America/New_York").unwrap();
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(1).unwrap();
        let ts = Timestamp::from_microsecond(row.get::<usize, i64>(1).unwrap()).unwrap();
        Ok(RowTab5 {
            account_id: row.get::<usize, usize>(0).unwrap(),
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_second(row.get::<usize, i64>(2).unwrap() / 1_000_000).unwrap(),
            subaccount_id: row.get(3).unwrap(),
            subaccount_name: row.get(4).unwrap(),
            hour_beginning: Zoned::new(ts, time_zone.clone()),
            load_zone_id: row.get(6).unwrap(),
            load_zone_name: row.get(7).unwrap(),
            rt_load_obligation: row.get::<usize, f64>(8).unwrap(),
            ard_reserve_designation: row.get::<usize, f64>(9).unwrap(),
            external_sale_load_obligation_mw: row.get::<usize, f64>(10).unwrap(),
            reserve_charge_allocation_mw: row.get::<usize, f64>(11).unwrap(),
            total_rt_reserve_charge: row.get::<usize, f64>(12).unwrap(),
        })
    })?;
    let res: Vec<RowTab5> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

/// Get hourly data for a tab between a start and end date.
/// If `account_ids` is `None`, return all stations.
fn get_tab5_data(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    account_id: Option<usize>,
) -> Result<Vec<RowTab5>, Box<dyn Error>> {
    let query = format!(
        r#"
SELECT *
FROM tab5 
WHERE report_date >= '{}'
AND report_date <= '{}'
{}
ORDER BY subaccount_id, report_date, hour_beginning;
    "#,
        start_date,
        end_date,
        match account_id {
            Some(id) => format!("AND account_id = {}", id),
            None => "".to_string(),
        }
    );
    println!("{}", query);
    let time_zone = TimeZone::get("America/New_York").unwrap();
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(1).unwrap();
        let ts = Timestamp::from_microsecond(row.get::<usize, i64>(1).unwrap()).unwrap();
        Ok(RowTab5 {
            account_id: row.get::<usize, usize>(0).unwrap(),
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_second(row.get::<usize, i64>(2).unwrap() / 1_000_000).unwrap(),
            subaccount_id: row.get(3).unwrap(),
            subaccount_name: row.get(4).unwrap(),
            hour_beginning: Zoned::new(ts, time_zone.clone()),
            load_zone_id: row.get(6).unwrap(),
            load_zone_name: row.get(7).unwrap(),
            rt_load_obligation: row.get::<usize, f64>(8).unwrap(),
            ard_reserve_designation: row.get::<usize, f64>(9).unwrap(),
            external_sale_load_obligation_mw: row.get::<usize, f64>(10).unwrap(),
            reserve_charge_allocation_mw: row.get::<usize, f64>(11).unwrap(),
            total_rt_reserve_charge: row.get::<usize, f64>(12).unwrap(),
        })
    })?;
    let res: Vec<RowTab5> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

fn get_path() -> String {
    ProdDb::sr_rsvcharge2().duckdb_path.to_string()
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use serde_json::Value;

    use crate::api::isone::mis::sr_rsvcharge2::get_tab5_data;

    use super::get_path;

    #[test]
    fn test_avg_level() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();

        let data = get_tab5_data(&conn, date(2024, 11, 15), date(2024, 11, 15), None).unwrap();
        println!("{}", data.len());
        println!("{:?}", data);
        assert!(data.len() == 71);
        // assert!(data.iter().any(|e| e.station_id == "1-2951"));

        // // for one facility only
        // let data = get_water_level(
        //     &conn,
        //     date(2024, 12, 4),
        //     date(2024, 12, 8),
        //     Some(vec!["1-2951".to_owned()]),
        // )
        // .unwrap();
        // assert!(data.len() == 5); // 5 days

        // // for two facilities
        // let data = get_water_level(
        //     &conn,
        //     date(2024, 12, 4),
        //     date(2024, 12, 8),
        //     Some(vec!["1-2951".to_owned(), "1-9698".to_owned()]),
        // )
        // .unwrap();
        // assert!(data.len() == 10); // 5 days * 2 facilities
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
