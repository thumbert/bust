use duckdb::{AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::civil::Date;

use crate::db::{
    isone::mis::sd_rtload::RowTab0,
    prod_db::ProdDb,
};
use actix_web::{get, web, HttpResponse, Responder};
use serde::Deserialize;


/// Get the report for one tab between a start/end date (hourly data)
/// http://127.0.0.1:8111/isone/mis/sd_rtload/tab/0/start/2024-12-04/end/2024-12-08?version=99
#[get("/isone/mis/sd_rtload/tab/{tab}/start/{start}/end/{end}")]
async fn api_tab_data(
    path: web::Path<(u8, Date, Date)>,
    query: web::Query<DataQuery>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let tab = path.0;
    let start_date = path.1;
    let end_date = path.2;
    let asset_ids: Option<Vec<String>> = query
        .asset_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.to_string()).collect());

    match tab {
        0 => {
            match get_tab0_data(&conn, start_date, end_date, asset_ids) {
                Ok(rows) => HttpResponse::Ok().json(rows),
                Err(e) => HttpResponse::InternalServerError().body(format!("Failed to get data from DuckDB. {}", e)),
            } 
        },
        _ => HttpResponse::BadRequest().body(format!("Invalid tab {} value.  Only value 0 is supported.", tab)),
    }
}

#[derive(Debug, Deserialize)]
struct DataQuery {
    /// Which version of the data to return.  If [None], return all versions.
    version: Option<u8>,
    /// One or more asset ids, separated by ','.  If [None], return all load asset.
    asset_ids: Option<String>,
    /// Restrict the assets returned to this account only.  If [None], return data from all accounts.
    account_id: Option<usize>,
}



/// Get hourly data for a tab between a start and end date.
/// If `account_ids` is `None`, return all stations.
fn get_tab0_data(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    asset_ids: Option<Vec<String>>,
) -> Result<Vec<RowTab0>> {
    let query = format!(
        r#"
SELECT *
FROM tab0 
WHERE date >= '{}'
AND date <= '{}'
{}
GROUP BY station_id, date
ORDER BY date;
    "#,
        start_date,
        end_date,
        match asset_ids {
            Some(ids) => format!("AND asset_id in ('{}')", ids.iter().join("','")),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    // let res_iter = stmt.query_map([], |row| {
    //     let n = 719528 + row.get::<usize, i32>(1).unwrap();
    //     Ok(RowTab0 {
    //         account_id: row.get::<usize, usize>(0).unwrap(),
    //         report_date: Date::ZERO.checked_add(n.days()).unwrap(),
    //         // value: row.get::<usize, f64>(2).unwrap(),
    //         version: todo!(),
    //         hour_beginning: todo!(),
    //         asset_id: todo!(),
    //         asset_name: todo!(),
    //         subaccount_id: todo!(),
    //         subaccount_name: todo!(),
    //         asset_type: todo!(),
    //         ownership_share: todo!(),
    //         product_type: todo!(),
    //         product_obligation: todo!(),
    //         product_clearing_price: todo!(),
    //         product_credit: todo!(),
    //         customer_share_of_product_credit: todo!(),
    //         strike_price: todo!(),
    //         hub_rt_lmp: todo!(),
    //         product_closeout_charge: todo!(),
    //         customer_share_of_product_closeout_charge: todo!(),
    //     })
    // })?;
    // let res: Vec<RowTab0> = res_iter.map(|e| e.unwrap()).collect();
    let res: Vec<RowTab0> = Vec::new();
    Ok(res)
}

fn get_path() -> String {
    ProdDb::sd_daasdt().duckdb_path.to_string()
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use serde_json::Value;

    use super::get_path;


    #[test]
    fn test_avg_level() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();

        // for all facilities
        // let data = get_water_level(&conn, date(2024, 12, 4), date(2024, 12, 8), None).unwrap();
        // assert!(data.len() == 1470);
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
