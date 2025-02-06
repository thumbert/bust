use std::{error::Error, str::FromStr};

use duckdb::{
    arrow::array::StringArray, params, types::EnumType::UInt8, types::ValueRef, AccessMode, Config,
    Connection, Result,
};
use itertools::Itertools;
use jiff::{civil::Date, tz::TimeZone, Timestamp, ToSpan, Zoned};

use crate::db::{
    isone::mis::sd_daasdt::{AssetType, ProductType, RowTab0, RowTab1, RowTab6, RowTab7},
    prod_db::ProdDb,
};
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
/// http://127.0.0.1:8111/isone/mis/sd_daasdt/tab/5/start/2025-03-01/end/2025-03-03?version=99
#[get("/isone/mis/sd_daasdt/tab/{tab}/start/{start}/end/{end}")]
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
        0 => match get_tab0_data(&conn, start_date, end_date, query.account_id) {
            Ok(rows) => HttpResponse::Ok().json(rows),
            Err(e) => HttpResponse::InternalServerError()
                .body(format!("Failed to get data from DuckDB. {}", e)),
        },
        1 => match get_tab1_data(&conn, start_date, end_date, query.account_id) {
            Ok(rows) => HttpResponse::Ok().json(rows),
            Err(e) => HttpResponse::InternalServerError()
                .body(format!("Failed to get data from DuckDB. {}", e)),
        },
        6 => match get_tab6_data(&conn, start_date, end_date, query.account_id) {
            Ok(rows) => HttpResponse::Ok().json(rows),
            Err(e) => HttpResponse::InternalServerError()
                .body(format!("Failed to get data from DuckDB. {}", e)),
        },
        7 => match get_tab7_data(&conn, start_date, end_date, query.account_id) {
            Ok(rows) => HttpResponse::Ok().json(rows),
            Err(e) => HttpResponse::InternalServerError()
                .body(format!("Failed to get data from DuckDB. {}", e)),
        },
        _ => HttpResponse::BadRequest().body(format!(
            "Invalid tab {} value.  Only values 0, 1, 6, 7 are supported.",
            tab
        )),
    }
}

#[derive(Debug, Deserialize)]
struct DataQuery2 {
    /// Restrict the data to this subaccount only.  If [None], return data for
    /// all subaccounts.
    subaccount_id: Option<String>,
    /// Get only the data for these asset_ids.  If [None], return all of them.  Only some
    /// asset ids can be returned by joining their values with a comma.
    asset_ids: Option<String>,
}

/// Daily charges to load
#[get("/isone/mis/sd_daasdt/daily/charges/account_id/{account_id}/start/{start}/end/{end}/settlement/{settlement}")]
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

/// Daily credit to the assets
#[get("/isone/mis/sd_daasdt/daily/credits/account_id/{account_id}/start/{start}/end/{end}/settlement/{settlement}")]
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
    let asset_ids: Option<Vec<u32>> = query
        .asset_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.parse::<u32>().unwrap()).collect());

    match get_daily_credit(
        &conn,
        account_id,
        start_date,
        end_date,
        settlement,
        query.subaccount_id.clone(),
        asset_ids,
    ) {
        Ok(rows) => HttpResponse::Ok().json(rows),
        Err(e) => HttpResponse::InternalServerError()
            .body(format!("Failed to get data from DuckDB. {}", e)),
    }
}

/// All customer credits by asset (FRS and EIR from tab0 & FER from tab1)
///
#[derive(Debug, Serialize, Deserialize)]
pub struct DailyCredit {
    pub report_date: Date,
    pub version: Timestamp,
    pub asset_id: u32,
    pub product: String,
    pub customer_share_of_product_credit: f64,
    pub customer_share_of_product_closeout_charge: f64,
}

pub fn get_daily_credit(
    conn: &Connection,
    account_id: usize,
    start_date: Date,
    end_date: Date,
    settlement: u8,
    subaccount_id: Option<String>,
    asset_ids: Option<Vec<u32>>,
) -> Result<Vec<DailyCredit>, Box<dyn Error>> {
    conn.execute("SET VARIABLE settlement = ?;", params![settlement])?;
    let query = format!(
        r#"
SELECT report_date, 
    versions[LEAST(len(versions), getvariable('settlement') + 1)] as version,
    asset_id, product_type,
    credit[LEAST(len(credit), getvariable('settlement') + 1)] as customer_share_of_product_credit,
    closeout_charge[LEAST(len(closeout_charge), getvariable('settlement') + 1)] as customer_share_of_product_closeout_charge,
FROM (
    SELECT report_date, asset_id, product_type,
    array_agg(version) as versions,
    array_agg(customer_share_of_product_credit) as credit,
    array_agg(customer_share_of_product_closeout_charge) as closeout_charge,
    FROM (
        SELECT report_date, version, asset_id, product_type,  
            sum(customer_share_of_product_credit) as customer_share_of_product_credit,
            sum(customer_share_of_product_closeout_charge) as customer_share_of_product_closeout_charge,
        FROM tab0
        WHERE report_date >= '{}'
        AND report_date <= '{}'
        AND account_id = {}{}{}
        GROUP BY report_date, version, asset_id, product_type
        ORDER BY report_date, version
    )
    GROUP BY report_date, asset_id, product_type
)
ORDER BY report_date;
        "#,
        start_date,
        end_date,
        account_id,
        match &subaccount_id {
            Some(id) => format!("\nAND subaccount_id = '{}'", id),
            None => "".to_string(),
        },
        match &asset_ids {
            Some(ids) => format!("\nAND asset_id in ('{}')", ids.iter().map(|e| e.to_string()).join("','")),
            None => "".to_string(),
        }
    );
    println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(0).unwrap();
        let product = match row.get_ref_unwrap(3) {
            ValueRef::Enum(e, idx) => match e {
                UInt8(v) => v
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(v.key(idx).unwrap()),
                _ => panic!("Unknown name"),
            },
            _ => panic!("Oops, column 3 should be an enum"),
        };

        Ok(DailyCredit {
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(1).unwrap()).unwrap(),
            asset_id: row.get::<usize, u32>(2).unwrap(),
            product: product.to_string(),
            customer_share_of_product_credit: row.get::<usize, f64>(4).unwrap(),
            customer_share_of_product_closeout_charge: row.get::<usize, f64>(5).unwrap(),
        })
    })?;
    let mut res: Vec<DailyCredit> = res_iter.map(|e| e.unwrap()).collect();

    // Get tab1 data (EIR)
    let query = format!(
        r#"
SELECT report_date, 
    versions[LEAST(len(versions), getvariable('settlement') + 1)] as version,
    asset_id, 
    credit[LEAST(len(credit), getvariable('settlement') + 1)] as customer_share_of_product_credit,
FROM (
    SELECT report_date, asset_id, 
    array_agg(version) as versions,
    array_agg(customer_share_of_product_credit) as credit,
    FROM (
        SELECT report_date, version, asset_id,  
            sum(customer_share_of_asset_fer_credit) as customer_share_of_product_credit,
        FROM tab1
        WHERE report_date >= '{}'
        AND report_date <= '{}'
        AND account_id = {}{}{}
        GROUP BY report_date, version, asset_id
        ORDER BY report_date, version
    )
    GROUP BY report_date, asset_id
)
ORDER BY report_date;
        "#,
        start_date,
        end_date,
        account_id,
        match &subaccount_id {
            Some(id) => format!("\nAND subaccount_id = '{}'", id),
            None => "".to_string(),
        },
        match &asset_ids {
            Some(ids) => format!("\nAND asset_id in ('{}')", ids.iter().map(|e| e.to_string()).join("','")),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(0).unwrap();
        Ok(DailyCredit {
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(1).unwrap()).unwrap(),
            asset_id: row.get::<usize, u32>(2).unwrap(),
            product: "FER".to_string(),
            customer_share_of_product_credit: row.get::<usize, f64>(4).unwrap(),
            customer_share_of_product_closeout_charge: 0.0,
        })
    })?;
    let mut fer: Vec<DailyCredit> = res_iter.map(|e| e.unwrap()).collect();

    // concatenate them
    res.append(&mut fer);
    Ok(res)
}

/// All customer charges (FRS and EIR)
#[derive(Debug, Serialize, Deserialize)]
pub struct DailyCharge {
    pub report_date: Date,
    pub version: Timestamp,
    pub name: String,
    pub value: f64,
}

pub fn get_daily_charges(
    conn: &Connection,
    account_id: usize,
    start_date: Date,
    end_date: Date,
    settlement: u8,
    subaccount_id: Option<String>,
) -> Result<Vec<DailyCharge>, Box<dyn Error>> {
    conn.execute("SET VARIABLE settlement = ?;", params![settlement])?;
    // Get tab6 data
    let query = format!(
        r#"
UNPIVOT (
    SELECT report_date, 
        versions[LEAST(len(versions), getvariable('settlement') + 1)] as version,
        tmsr[LEAST(len(tmsr), getvariable('settlement') + 1)] as da_tmsr_charge,
        tmnsr[LEAST(len(tmnsr), getvariable('settlement') + 1)] as da_tmnsr_charge,
        tmor[LEAST(len(tmor), getvariable('settlement') + 1)] as da_tmor_charge,
        tmsr_co[LEAST(len(tmsr_co), getvariable('settlement') + 1)] as da_tmsr_closeout_credit,
        tmnsr_co[LEAST(len(tmnsr_co), getvariable('settlement') + 1)] as da_tmnsr_closeout_credit,
        tmor_co[LEAST(len(tmor_co), getvariable('settlement') + 1)] as da_tmor_closeout_credit,
    FROM (
        SELECT report_date, 
        array_agg(version) as versions,
        array_agg(da_tmsr_charge) as tmsr,
        array_agg(da_tmnsr_charge) as tmnsr,
        array_agg(da_tmor_charge) as tmor,
        array_agg(da_tmsr_closeout_credit) as tmsr_co,
        array_agg(da_tmnsr_closeout_credit) as tmnsr_co,
        array_agg(da_tmor_closeout_credit) as tmor_co,
        FROM (
            SELECT report_date, version,  
                sum(da_tmsr_charge) as da_tmsr_charge,
                sum(da_tmnsr_charge) as da_tmnsr_charge,
                sum(da_tmor_charge) as da_tmor_charge,
                sum(da_tmsr_closeout_credit) as da_tmsr_closeout_credit,
                sum(da_tmnsr_closeout_credit) as da_tmnsr_closeout_credit,
                sum(da_tmor_closeout_credit) as da_tmor_closeout_credit,
            FROM tab6
            WHERE report_date >= '{}'
            AND report_date <= '{}'
            AND account_id = {}
            {}
            GROUP BY report_date, version
            ORDER BY report_date, version
        )
        GROUP BY report_date
    )
    ORDER BY report_date
)
    ON
        da_tmnsr_charge,
        da_tmor_charge,
        da_tmsr_charge,
        da_tmnsr_closeout_credit,
        da_tmor_closeout_credit,
        da_tmsr_closeout_credit
    INTO
        NAME name
        VALUE value;
        "#,
        start_date,
        end_date,
        account_id,
        match &subaccount_id {
            Some(id) => format!("AND subaccount_id = '{}'", id),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(0).unwrap();
        Ok(DailyCharge {
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(1).unwrap()).unwrap(),
            name: row.get(2).unwrap(),
            value: row.get::<usize, f64>(3).unwrap(),
        })
    })?;
    let mut res: Vec<DailyCharge> = res_iter.map(|e| e.unwrap()).collect();

    // Get tab7 data
    let query = format!(
        r#"
UNPIVOT (
    SELECT report_date, 
        versions[LEAST(len(versions), getvariable('settlement') + 1)] as version,
        eir[LEAST(len(eir), getvariable('settlement') + 1)] as fer_and_da_eir_charge,
        eir_co[LEAST(len(eir_co), getvariable('settlement') + 1)] as da_eir_closeout_credit,
    FROM (
        SELECT report_date, 
        array_agg(version) as versions,
        array_agg(fer_and_da_eir_charge) as eir,
        array_agg(da_eir_closeout_credit) as eir_co,
        FROM (
            SELECT report_date, version,  
                sum(fer_and_da_eir_charge) as fer_and_da_eir_charge,
                sum(da_eir_closeout_credit) as da_eir_closeout_credit,
            FROM tab7
            WHERE report_date >= '{}'
            AND report_date <= '{}'
            AND account_id = {}
            {}
            GROUP BY report_date, version
            ORDER BY report_date, version
        )
        GROUP BY report_date
    )
    ORDER BY report_date
)
    ON
        fer_and_da_eir_charge,
        da_eir_closeout_credit,
    INTO
        NAME name
        VALUE value;
    "#,
        start_date,
        end_date,
        account_id,
        match &subaccount_id {
            Some(id) => format!("AND subaccount_id = '{}'", id),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(0).unwrap();
        Ok(DailyCharge {
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(1).unwrap()).unwrap(),
            name: row.get(2).unwrap(),
            value: row.get::<usize, f64>(3).unwrap(),
        })
    })?;
    let mut eir: Vec<DailyCharge> = res_iter.map(|e| e.unwrap()).collect();

    // concatenate them
    res.append(&mut eir);
    Ok(res)
}

pub fn get_tab0_data(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    account_id: Option<usize>,
) -> Result<Vec<RowTab0>, Box<dyn Error>> {
    let query = format!(
        r#"
SELECT *
FROM tab0 
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
        let asset_type = match row.get_ref_unwrap(8) {
            ValueRef::Enum(e, idx) => match e {
                UInt8(v) => v
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(v.key(idx).unwrap()),
                _ => panic!("Unknown name"),
            },
            _ => panic!("Oops, column 8 should be an enum"),
        };
        let p_type = match row.get_ref_unwrap(10) {
            ValueRef::Enum(e, idx) => match e {
                UInt8(v) => v
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(v.key(idx).unwrap()),
                _ => panic!("Unknown name"),
            },
            _ => panic!("Oops, column 10 should be an enum"),
        };

        Ok(RowTab0 {
            account_id: row.get::<usize, usize>(0).unwrap(),
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(2).unwrap()).unwrap(),
            hour_beginning: Zoned::new(ts, time_zone.clone()),
            asset_id: row.get(4).unwrap(),
            asset_name: row.get(5).unwrap(),
            subaccount_id: row.get(6).unwrap(),
            subaccount_name: row.get(7).unwrap(),
            asset_type: AssetType::from_str(asset_type).unwrap(),
            ownership_share: row.get::<usize, f32>(9).unwrap(),
            product_type: ProductType::from_str(p_type).unwrap(),
            product_obligation: row.get::<usize, f64>(11).unwrap(),
            product_clearing_price: row.get::<usize, f64>(12).unwrap(),
            product_credit: row.get::<usize, f64>(13).unwrap(),
            customer_share_of_product_credit: row.get::<usize, f64>(14).unwrap(),
            strike_price: row.get::<usize, f64>(15).unwrap(),
            hub_rt_lmp: row.get::<usize, f64>(16).unwrap(),
            product_closeout_charge: row.get::<usize, f64>(17).unwrap(),
            customer_share_of_product_closeout_charge: row.get::<usize, f64>(18).unwrap(),
        })
    })?;
    let res: Vec<RowTab0> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

/// Get tab 1 data
pub fn get_tab1_data(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    account_id: Option<usize>,
) -> Result<Vec<RowTab1>, Box<dyn Error>> {
    let query = format!(
        r#"
SELECT *
FROM tab1 
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
        let asset_type = match row.get_ref_unwrap(8) {
            ValueRef::Enum(e, idx) => match e {
                UInt8(v) => v
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(v.key(idx).unwrap()),
                _ => panic!("Unknown name"),
            },
            _ => panic!("Oops, column 8 should be an enum"),
        };

        Ok(RowTab1 {
            account_id: row.get::<usize, usize>(0).unwrap(),
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(2).unwrap()).unwrap(),
            hour_beginning: Zoned::new(ts, time_zone.clone()),
            asset_id: row.get(4).unwrap(),
            asset_name: row.get(5).unwrap(),
            subaccount_id: row.get(6).unwrap(),
            subaccount_name: row.get(7).unwrap(),
            asset_type: AssetType::from_str(asset_type).unwrap(),
            ownership_share: row.get::<usize, f32>(9).unwrap(),
            da_cleared_energy: row.get::<usize, f64>(10).unwrap(),
            fer_price: row.get::<usize, f64>(11).unwrap(),
            asset_fer_credit: row.get::<usize, f64>(12).unwrap(),
            customer_share_of_asset_fer_credit: row.get::<usize, f64>(13).unwrap(),
        })
    })?;
    let res: Vec<RowTab1> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

/// Get hourly data for tab6 between a start and end date. If `account_id` is `None`, return all accounts.
pub fn get_tab6_data(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    account_id: Option<usize>,
) -> Result<Vec<RowTab6>, Box<dyn Error>> {
    let query = format!(
        r#"
SELECT *
FROM tab6 
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
        let ts = Timestamp::from_microsecond(row.get::<usize, i64>(5).unwrap()).unwrap();
        let rt_load_obligation = row.get::<usize, f64>(6).unwrap();
        let rt_external_node_load_obligation = row.get::<usize, f64>(7).unwrap();
        let rt_dard_load_obligation_reduction = row.get::<usize, f64>(8).unwrap();
        let rt_load_obligation_for_frs_charge_allocation = row.get::<usize, f64>(9).unwrap();
        let pool_rt_load_obligation_for_frs_charge_allocation = row.get::<usize, f64>(10).unwrap();
        let pool_da_tmsr_credit = row.get::<usize, f64>(11).unwrap();
        let da_tmsr_charge = row.get::<usize, f64>(12).unwrap();
        let pool_da_tmnsr_credit = row.get::<usize, f64>(13).unwrap();
        let da_tmnsr_charge = row.get::<usize, f64>(14).unwrap();
        let pool_da_tmor_credit = row.get::<usize, f64>(15).unwrap();
        let da_tmor_charge = row.get::<usize, f64>(16).unwrap();
        let pool_da_tmsr_closeout_charge = row.get::<usize, f64>(17).unwrap();
        let da_tmsr_closeout_credit = row.get::<usize, f64>(18).unwrap();
        let pool_da_tmnsr_closeout_charge = row.get::<usize, f64>(19).unwrap();
        let da_tmnsr_closeout_credit = row.get::<usize, f64>(20).unwrap();
        let pool_da_tmor_closeout_charge = row.get::<usize, f64>(21).unwrap();
        let da_tmor_closeout_credit = row.get::<usize, f64>(22).unwrap();
        Ok(RowTab6 {
            account_id: row.get::<usize, usize>(0).unwrap(),
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(2).unwrap()).unwrap(),
            subaccount_id: row.get(3).unwrap(),
            subaccount_name: row.get(4).unwrap(),
            hour_beginning: Zoned::new(ts, time_zone.clone()),
            rt_load_obligation,
            rt_external_node_load_obligation,
            rt_dard_load_obligation_reduction,
            rt_load_obligation_for_frs_charge_allocation,
            pool_rt_load_obligation_for_frs_charge_allocation,
            pool_da_tmsr_credit,
            da_tmsr_charge,
            pool_da_tmnsr_credit,
            da_tmnsr_charge,
            pool_da_tmor_credit,
            da_tmor_charge,
            pool_da_tmsr_closeout_charge,
            da_tmsr_closeout_credit,
            pool_da_tmnsr_closeout_charge,
            da_tmnsr_closeout_credit,
            pool_da_tmor_closeout_charge,
            da_tmor_closeout_credit,
        })
    })?;
    let res: Vec<RowTab6> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

pub fn get_tab7_data(
    conn: &Connection,
    start_date: Date,
    end_date: Date,
    account_id: Option<usize>,
) -> Result<Vec<RowTab7>, Box<dyn Error>> {
    let query = format!(
        r#"
SELECT *
FROM tab7 
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
        let ts = Timestamp::from_microsecond(row.get::<usize, i64>(5).unwrap()).unwrap();
        let rt_load_obligation = row.get::<usize, f64>(6).unwrap();
        let rt_external_node_load_obligation = row.get::<usize, f64>(7).unwrap();
        let rt_dard_load_obligation_reduction = row.get::<usize, f64>(8).unwrap();
        let rt_load_obligation_for_da_eir_charge_allocation = row.get::<usize, f64>(9).unwrap();
        let pool_rt_load_obligation_for_da_eir_charge_allocation =
            row.get::<usize, f64>(10).unwrap();

        let pool_da_eir_credit = row.get::<usize, f64>(11).unwrap();
        let pool_fer_credit = row.get::<usize, f64>(12).unwrap();
        let pool_export_fer_charge = row.get::<usize, f64>(13).unwrap();
        let pool_fer_and_da_eir_net_credits = row.get::<usize, f64>(14).unwrap();
        let fer_and_da_eir_charge = row.get::<usize, f64>(15).unwrap();
        let pool_da_eir_closeout_charge = row.get::<usize, f64>(16).unwrap();
        let da_eir_closeout_credit = row.get::<usize, f64>(17).unwrap();
        Ok(RowTab7 {
            account_id: row.get::<usize, usize>(0).unwrap(),
            report_date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_microsecond(row.get::<usize, i64>(2).unwrap()).unwrap(),
            subaccount_id: row.get(3).unwrap(),
            subaccount_name: row.get(4).unwrap(),
            hour_beginning: Zoned::new(ts, time_zone.clone()),
            rt_load_obligation,
            rt_external_node_load_obligation,
            rt_dard_load_obligation_reduction,
            rt_load_obligation_for_da_eir_charge_allocation,
            pool_rt_load_obligation_for_da_eir_charge_allocation,
            pool_da_eir_credit,
            pool_fer_credit,
            pool_export_fer_charge,
            pool_fer_and_da_eir_net_credits,
            fer_and_da_eir_charge,
            pool_da_eir_closeout_charge,
            da_eir_closeout_credit,
        })
    })?;
    let res: Vec<RowTab7> = res_iter.map(|e| e.unwrap()).collect();
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

    use super::*;

    #[test]
    fn test_tab0() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let data = get_tab0_data(&conn, date(2024, 11, 15), date(2024, 11, 15), Some(2)).unwrap();
        assert_eq!(data.len(), 44);
        Ok(())
    }

    #[test]
    fn test_tab1() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let data = get_tab1_data(&conn, date(2024, 11, 15), date(2024, 11, 15), Some(2)).unwrap();
        assert_eq!(data.len(), 24);
        Ok(())
    }

    #[test]
    fn test_tab6() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let data = get_tab6_data(&conn, date(2024, 11, 15), date(2024, 11, 15), Some(2)).unwrap();
        assert_eq!(data.len(), 24);
        Ok(())
    }

    #[test]
    fn test_tab7() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let data = get_tab7_data(&conn, date(2024, 11, 15), date(2024, 11, 15), Some(2)).unwrap();
        assert_eq!(data.len(), 24);
        Ok(())
    }

    #[test]
    fn api_daily_credits() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/mis/sd_daasdt/daily/credits/account_id/2/start/2024-11-15/end/2024-11-15/settlement/0",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Vec<DailyCredit> = serde_json::from_str(&response).unwrap();
        assert_eq!(v.len(), 4);
        assert_eq!(v.first().unwrap().report_date, date(2024, 11, 15));
        Ok(())
    }

    #[test]
    fn api_daily_charges() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/mis/sd_daasdt/daily/charges/account_id/2/start/2024-11-15/end/2024-11-15/settlement/0",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Vec<DailyCharge> = serde_json::from_str(&response).unwrap();
        assert_eq!(v.len(), 8);
        assert_eq!(v.first().unwrap().report_date, date(2024, 11, 15));
        Ok(())
    }
}
