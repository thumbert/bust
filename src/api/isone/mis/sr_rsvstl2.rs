use std::error::Error;

use duckdb::{params, AccessMode, Config, Connection, Result};
use jiff::{civil::Date, tz::TimeZone, Timestamp, ToSpan, Zoned};

use crate::db::{isone::mis::sr_rsvstl2::RowTab3, prod_db::ProdDb};
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
/// http://127.0.0.1:8111/isone/mis/sr_rsvstl2/tab/3/start/2024-12-04/end/2024-12-08?version=99
#[get("/isone/mis/sr_rsvstl2/tab/{tab}/start/{start}/end/{end}")]
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
        3 => match get_tab3_data(&conn, start_date, end_date, query.account_id) {
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

#[get("/isone/mis/sr_rsvstl2/daily/credits/account_id/{account_id}/start/{start}/end/{end}/settlement/{settlement}")]
async fn api_daily_credits(
    path: web::Path<(usize, Date, Date, u8)>,
    query: web::Query<DataQuery2>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let account_id = path.0;
    let start_date = path.1;
    let end_date = path.2;
    let settlement = path.3;

    match get_daily_credits(
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
pub struct DailyCredits {
    pub report_date: Date,
    pub asset_id: usize,
    pub version: Timestamp,
    pub rt_tmsr_credit: f64,
    pub rt_tmnsr_credit: f64,
    pub rt_tmor_credit: f64,
    pub rt_reserve_credit: f64,
}

fn get_daily_credits(
    conn: &Connection,
    account_id: usize,
    start_date: Date,
    end_date: Date,
    settlement: u8,
    subaccount_id: Option<String>,
) -> Result<Vec<DailyCredits>, Box<dyn Error>> {
    conn.execute("SET VARIABLE settlement = ?;", params![settlement])?;
    let query = format!(
        r#"
SELECT report_date, asset_id, 
    versions[LEAST(len(versions), getvariable('settlement') + 1)] as version,
    tmsr[LEAST(len(tmsr), getvariable('settlement') + 1)] as rt_tmsr_credit,
    tmnsr[LEAST(len(tmnsr), getvariable('settlement') + 1)] as rt_tmnsr_credit,
    tmor[LEAST(len(tmor), getvariable('settlement') + 1)] as rt_tmor_credit,
    total[LEAST(len(total), getvariable('settlement') + 1)] as rt_reserve_credit
FROM (
    SELECT report_date, asset_id, 
      array_agg(version) as versions,
      array_agg(tmsr_credit) as tmsr,
      array_agg(tmnsr_credit) as tmnsr,
      array_agg(tmor_credit) as tmor,
      array_agg(rt_reserve_credit) as total
    FROM (
        SELECT report_date, version, asset_id, 
            sum(rt_tmsr_credit) as tmsr_credit,
            sum(rt_tmnsr_credit) as tmnsr_credit,
            sum(rt_tmor_credit) as tmor_credit,
            sum(rt_reserve_credit) as rt_reserve_credit,
        FROM tab3
        WHERE report_date >= '{}'
        AND report_date <= '{}'
        AND account_id = {}
        {}
        GROUP BY report_date, asset_id, version
        ORDER BY report_date, asset_id, version
    )
    GROUP BY report_date, asset_id
)
ORDER BY report_date, asset_id;
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
        Ok(DailyCredits {
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            asset_id: row.get(1).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(2).unwrap()).unwrap(),
            rt_tmsr_credit: row.get::<usize, f64>(3).unwrap(),
            rt_tmnsr_credit: row.get::<usize, f64>(4).unwrap(),
            rt_tmor_credit: row.get::<usize, f64>(5).unwrap(),
            rt_reserve_credit: row.get::<usize, f64>(6).unwrap(),
        })
    })?;
    let res: Vec<DailyCredits> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

/// Get hourly data for a tab between a start and end date.
/// If `account_ids` is `None`, return all stations.
pub fn get_tab3_data(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    account_id: Option<usize>,
) -> Result<Vec<RowTab3>, Box<dyn Error>> {
    let query = format!(
        r#"
SELECT *
FROM tab3 
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
        let ts = Timestamp::from_microsecond(row.get::<usize, i64>(3).unwrap()).unwrap();
        Ok(RowTab3 {
            account_id: row.get::<usize, usize>(0).unwrap(),
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_second(row.get::<usize, i64>(2).unwrap() / 1_000_000).unwrap(),
            hour_beginning: Zoned::new(ts, time_zone.clone()),
            asset_id: row.get(4).unwrap(),
            asset_name: row.get(5).unwrap(),
            subaccount_id: row.get(6).unwrap(),
            subaccount_name: row.get(7).unwrap(),
            rt_tmsr_credit: row.get::<usize, f64>(8).unwrap(),
            rt_tmnsr_credit: row.get::<usize, f64>(9).unwrap(),
            rt_tmor_credit: row.get::<usize, f64>(10).unwrap(),
            rt_reserve_credit: row.get::<usize, f64>(11).unwrap(),
        })
    })?;
    let res: Vec<RowTab3> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

fn get_path() -> String {
    ProdDb::sr_rsvstl2().duckdb_path.to_string()
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;

    use crate::api::isone::mis::sr_rsvstl2::*;

    use super::get_path;

    #[test]
    fn test_tab3() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();

        let data = get_tab3_data(&conn, date(2024, 11, 15), date(2024, 11, 15), None).unwrap();
        // println!("{}", data.len());
        // println!("{:?}", data);
        assert!(data.len() == 168);

        Ok(())
    }

    #[test]
    fn api_daily_credits() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/mis/sr_rsvstl2/daily/credits/account_id/2/start/2024-11-04/end/2024-12-08/settlement/0",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Vec<DailyCredits> = serde_json::from_str(&response).unwrap();
        // println!("{:?}", v);
        assert_eq!(v.len(), 7);
        assert_eq!(v.first().unwrap().report_date, date(2024, 11, 15));

        // with a subaccount
        let url = format!(
            "{}/isone/mis/sr_rsvstl2/daily/credits/account_id/2/start/2024-11-04/end/2024-12-08/settlement/0?subaccount_id=9001",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Vec<DailyCredits> = serde_json::from_str(&response).unwrap();
        assert_eq!(v.len(), 7);
        assert_eq!(v.first().unwrap().report_date, date(2024, 11, 15));

        Ok(())
    }
}
