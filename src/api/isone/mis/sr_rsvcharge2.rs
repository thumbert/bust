use std::error::Error;

use duckdb::{params, AccessMode, Config, Connection, Result};
use jiff::{civil::Date, tz::TimeZone, Timestamp, ToSpan, Zoned};

use crate::db::{isone::mis::sr_rsvcharge2::RowTab5, prod_db::ProdDb};
use actix_web::{get, web, HttpResponse, Responder};
use serde::{Deserialize, Serialize};

#[derive(Deserialize)]
struct DataQuery {
    /// Which version of the data to return.  If [None], return all versions.
    // version: Option<u8>,
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
    /// Restrict the data to this subaccount only.  If [None], return data aggregated for
    /// all subaccounts.
    subaccount_id: Option<String>,
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

    match get_daily_charges(
        &conn,
        account_id,
        start_date,
        end_date,
        settlement,
        query.subaccount_id.clone(),
    ) {
        Ok(rows) => HttpResponse::Ok().json(rows),
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("Failed to get data from DuckDB. {}", e)),
    }
}

#[derive(Debug, Serialize, Deserialize)]
pub struct DailyCharges {
    report_date: Date,
    load_zone_id: usize,
    version: Timestamp,
    total_rt_reserve_charge: f64,
}

pub fn get_daily_charges(
    conn: &Connection,
    account_id: usize,
    start_date: Date,
    end_date: Date,
    settlement: u8,
    subaccount_id: Option<String>,
) -> Result<Vec<DailyCharges>, Box<dyn Error>> {
    conn.execute("SET VARIABLE settlement = ?;", params![settlement])?;
    let query = format!(
        r#"
SELECT report_date, load_zone_id, 
    versions[LEAST(len(versions), getvariable('settlement') + 1)] as version,
    trc[LEAST(len(trc), getvariable('settlement') + 1)] as total_rt_reserve_charge
FROM (
    SELECT report_date, load_zone_id, 
      array_agg(version) as versions,
      array_agg(total_rt_reserve_charge) as trc
    FROM (
        SELECT report_date, version, load_zone_id, 
            sum(total_rt_reserve_charge) as total_rt_reserve_charge,
        FROM tab5
        WHERE report_date >= '{}'
        AND report_date <= '{}'
        AND account_id = {}
        {}
        GROUP BY report_date, load_zone_id, version
        ORDER BY report_date, load_zone_id, version
    )
    GROUP BY report_date, load_zone_id
)
ORDER BY report_date, load_zone_id;
    "#,
        start_date,
        end_date,
        account_id,
        match subaccount_id {
            Some(id) => format!("AND subaccount_id = '{}'", id),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(0).unwrap();
        Ok(DailyCharges {
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            load_zone_id: row.get(1).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(2).unwrap()).unwrap(),
            total_rt_reserve_charge: row.get::<usize, f64>(3).unwrap(),
        })
    })?;
    let res: Vec<DailyCharges> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

/// Get hourly data for a tab between a start and end date.
/// If `account_ids` is `None`, return all stations.
pub fn get_tab5_data(
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
    // println!("{}", query);
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

    use crate::api::isone::mis::sr_rsvcharge2::{get_tab5_data, DailyCharges};

    use super::get_path;

    #[test]
    fn test_tab5() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();

        let data = get_tab5_data(&conn, date(2024, 11, 15), date(2024, 11, 15), None).unwrap();
        // println!("{}", data.len());
        // println!("{:?}", data);
        assert!(data.len() == 71);

        Ok(())
    }

    #[test]
    fn api_daily_charges() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/mis/sr_rsvcharge2/daily/charges/account_id/2/start/2024-11-04/end/2024-12-08/settlement/0",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Vec<DailyCharges> = serde_json::from_str(&response).unwrap();
        assert_eq!(v.len(), 3);
        assert_eq!(v.first().unwrap().report_date, date(2024, 11, 15));

        // with a subaccount
        let url = format!(
            "{}/isone/mis/sr_rsvcharge2/daily/charges/account_id/2/start/2024-11-04/end/2024-12-08/settlement/0?subaccount_id=9001",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Vec<DailyCharges> = serde_json::from_str(&response).unwrap();
        assert_eq!(v.len(), 1);
        assert_eq!(v.first().unwrap().report_date, date(2024, 11, 15));

        Ok(())
    }
}
