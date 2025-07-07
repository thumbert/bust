use std::{fmt::{self}, str::FromStr};

use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{
    arrow::array::StringArray, types::EnumType::UInt8, types::ValueRef, AccessMode, Config,
    Connection, Result,
};
use itertools::Itertools;
use jiff::{civil::Date, Timestamp, ToSpan};
use serde::{Deserialize, Serialize};

use crate::db::prod_db::ProdDb;

#[derive(Debug, Deserialize)]
struct OffersQuery {
    /// One or more masked asset ids, separated by commas
    /// If asset_ids are not specified, return all of them.  Use carefully
    /// because it's a lot of data...
    masked_asset_ids: Option<String>,
}

/// Get DA or RT offers between a start/end date
#[get("/isone/energy_offers/{market}/start/{start}/end/{end}")]
async fn api_offers(
    path: web::Path<(Date, Date)>,
    query: web::Query<OffersQuery>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(ProdDb::isone_masked_daas_offers().duckdb_path, config).unwrap();

    let start_date = path.0;
    let end_date = path.1;  

    let asset_ids: Option<Vec<i32>> = query
        .masked_asset_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.parse::<i32>().unwrap()).collect());

    let offers = get_offers(&conn, start_date, end_date, asset_ids).unwrap();
    HttpResponse::Ok().json(offers)
}

#[derive(Debug, PartialEq, Serialize)]
pub struct DaasOffer {
    masked_asset_id: u32,
    timestamp_s: i64, // seconds since epoch of hour beginning
    segment: u8,
    quantity: f32,
    price: f32,
}




/// Get the energy offers between a [start, end] date for a list of units
/// (or all units)
pub fn get_offers(
    conn: &Connection,
    start: Date,
    end: Date,
    masked_unit_ids: Option<Vec<i32>>,
) -> Result<Vec<DaasOffer>> {
    let query = format!(
        r#"
SELECT 
    MaskedAssetId, 
    UnitStatus,
    HourBeginning,
    Segment,
    Quantity,
    Price,
FROM offers
WHERE HourBeginning >= '{}'
AND HourBeginning < '{}'
{}
ORDER BY "MaskedAssetId", "HourBeginning";    
    "#,
        start
            .in_tz("America/New_York")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        end.in_tz("America/New_York")
            .unwrap()
            .checked_add(1.day())
            .ok()
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        match masked_unit_ids {
            Some(ids) => format!("AND \"MaskedAssetId\" in ({}) ", ids.iter().join(", ")),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let unit_status = match row.get_ref_unwrap(1) {
            ValueRef::Enum(e, idx) => match e {
                UInt8(v) => v
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(v.key(idx).unwrap()),
                _ => panic!("Unknown unit status"),
            },
            _ => panic!("Oops, column should be an enum"),
        };
        let micro: i64 = row.get(2).unwrap();
        Ok(DaasOffer {
            masked_asset_id: row.get(0).unwrap(),
            timestamp_s: micro / 1_000_000,
            segment: row.get(3)?,
            quantity: row.get(4)?,
            price: row.get(5)?,
        })
    })?;
    let offers: Vec<DaasOffer> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

