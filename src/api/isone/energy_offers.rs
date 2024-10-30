use std::str::FromStr;

use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{
    arrow::array::StringArray, types::EnumType::UInt8, types::ValueRef, AccessMode, Config,
    Connection, Result,
};
use itertools::Itertools;
use jiff::{civil::Date, Timestamp, ToSpan};
use serde::{Deserialize, Serialize};

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
    path: web::Path<(String, String, String)>,
    query: web::Query<OffersQuery>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();

    let market: Market = match path.0.parse() {
        Ok(v) => v,
        Err(e) => return HttpResponse::BadRequest().body(e.to_string()),
    };

    let start_date: Date = match path.1.to_string().parse() {
        Ok(v) => v,
        Err(_) => {
            return HttpResponse::BadRequest().body(format!("Unable to parse {} as a date", path.1))
        }
    };

    let end_date: Date = match path.2.to_string().parse() {
        Ok(v) => v,
        Err(_) => {
            return HttpResponse::BadRequest().body(format!("Unable to parse {} as a date", path.1))
        }
    };

    let asset_ids: Option<Vec<i32>> = query
        .masked_asset_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.parse::<i32>().unwrap()).collect());

    let offers = get_energy_offers(&conn, market, start_date, end_date, asset_ids).unwrap();
    HttpResponse::Ok().json(offers)
}

/// Get DA or RT stack for a list of timestamps (seconds from epoch)
#[get("/isone/energy_offers/{market}/stack/timestamps/{timestamps}")]
async fn api_stack(path: web::Path<(String, String)>) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();

    let market: Market = match path.0.parse() {
        Ok(v) => v,
        Err(e) => return HttpResponse::BadRequest().body(e.to_string()),
    };

    let timestamps = match path
        .1
        .split(',')
        .map(|n| {
            n.trim()
                .parse::<i64>()
                .map_err(|_| format!("Failed to parse {} to an integer", n))
                .and_then(|e| Timestamp::from_second(e).map_err(|e| e.to_string()))
        })
        .collect::<Result<Vec<Timestamp>, _>>()
    {
        Ok(v) => v,
        Err(_) => {
            return HttpResponse::BadRequest().body(format!(
                "Unable to parse {} to a list of timestamps",
                path.1
            ))
        }
    };
    match get_stack(&conn, market, timestamps) {
        Ok(offers) => HttpResponse::Ok().json(offers),
        Err(_) => HttpResponse::BadRequest().body("Error executing the query"),
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub enum Market {
    DA,
    RT,
}

impl FromStr for Market {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "DA" => Ok(Market::DA),
            "RT" => Ok(Market::RT),
            _ => Err(format!("Can't parse market: {}", s)),
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub enum UnitStatus {
    Economic,
    Unavailable,
    MustRun,
}

impl FromStr for UnitStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ECONOMIC" => Ok(UnitStatus::Economic),
            "UNAVAILABLE" => Ok(UnitStatus::Unavailable),
            "MUST_RUN" => Ok(UnitStatus::MustRun),
            _ => Err(format!("Can't parse unit status: {}", s)),
        }
    }
}

#[derive(Debug, PartialEq, Serialize)]
pub struct EnergyOffer {
    masked_asset_id: u32,
    unit_status: UnitStatus,
    timestamp_s: i64, // seconds since epoch of hour beginning
    segment: u8,
    quantity: f32,
    price: f32,
}

/// Get the energy offers between a [start, end] date for a list of units
/// (or all units)
pub fn get_energy_offers(
    conn: &Connection,
    market: Market,
    start: Date,
    end: Date,
    masked_unit_ids: Option<Vec<i32>>,
) -> Result<Vec<EnergyOffer>> {
    let query = format!(
        r#"
SELECT 
    MaskedAssetId, 
    UnitStatus,
    HourBeginning,
    Segment,
    Quantity,
    Price,
FROM {:?}_offers
WHERE HourBeginning >= '{}'
AND HourBeginning < '{}'
{}
ORDER BY "MaskedAssetId", "HourBeginning";    
    "#,
        market,
        start
            .intz("America/New_York")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        end.intz("America/New_York")
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
        Ok(EnergyOffer {
            masked_asset_id: row.get(0).unwrap(),
            unit_status: unit_status.parse::<UnitStatus>().unwrap(),
            timestamp_s: micro / 1_000_000,
            segment: row.get(3)?,
            quantity: row.get(4)?,
            price: row.get(5)?,
        })
    })?;
    let offers: Vec<EnergyOffer> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

/// Get the energy offers for the units that are available, for one timestamp
/// (or more), sorted by timestamp and price
/// Don't return the Unavailable units.
///
pub fn get_stack(
    conn: &Connection,
    market: Market,
    timestamps: Vec<Timestamp>,
) -> Result<Vec<EnergyOffer>> {
    let query = format!(
        r#"
SELECT 
    MaskedAssetId, 
    UnitStatus,
    HourBeginning,
    Segment,
    Quantity,
    Price,
FROM {:?}_offers
WHERE UnitStatus <> 'UNAVAILABLE'
{}
ORDER BY HourBeginning, Price;    
    "#,
        market,
        match timestamps.len() {
            1 => format!(
                "AND HourBeginning == '{}' ",
                timestamps
                    .first()
                    .unwrap()
                    .intz("America/New_York")
                    .unwrap()
                    .strftime("%Y-%m-%d %H:%M:%S.000%:z")
            ),
            _ => format!(
                "AND HourBeginning in ('{}')",
                timestamps
                    .iter()
                    .map(|e| e
                        .intz("America/New_York")
                        .unwrap()
                        .strftime("%Y-%m-%d %H:%M:%S.000%:z"))
                    .join("', '")
            ),
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
        Ok(EnergyOffer {
            masked_asset_id: row.get(0).unwrap(),
            unit_status: unit_status.parse::<UnitStatus>().unwrap(),
            timestamp_s: micro / 1_000_000,
            segment: row.get(3)?,
            quantity: row.get(4)?,
            price: row.get(5)?,
        })
    })?;
    let offers: Vec<EnergyOffer> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

fn get_path() -> String {
    "/home/adrian/Downloads/Archive/IsoExpress/energy_offers.duckdb".to_string()
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use serde_json::Value;

    use crate::api::isone::energy_offers::*;

    #[test]
    fn test_get_offers() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let xs = get_energy_offers(
            &conn,
            Market::DA,
            date(2024, 3, 1),
            date(2024, 3, 1),
            Some(vec![77459, 86083, 31662]),
        )
        .unwrap();
        conn.close().unwrap();
        let x0 = xs.first().unwrap();
        // println!("{:?}", x0);
        assert_eq!(
            *x0,
            EnergyOffer {
                masked_asset_id: 31662,
                unit_status: UnitStatus::Economic,
                timestamp_s: 1709269200,
                segment: 0,
                quantity: 283.0,
                price: 37.61,
            }
        );
        Ok(())
    }

    #[test]
    fn test_get_stack() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let xs = get_stack(
            &conn,
            Market::DA,
            vec!["2024-03-01 00:00:00-05".parse().unwrap()],
        )
        .unwrap();
        conn.close().unwrap();
        let x0 = xs.first().unwrap();
        // println!("{:?}", x0);
        assert_eq!(
            *x0,
            EnergyOffer {
                masked_asset_id: 42103,
                unit_status: UnitStatus::MustRun,
                timestamp_s: 1709269200,
                segment: 0,
                quantity: 8.0,
                price: -150.0,
            }
        );
        assert_eq!(xs.len(), 780);
        Ok(())
    }

    #[test]
    fn test_get_stack2() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let xs = get_stack(
            &conn,
            Market::DA,
            vec![
                "2024-02-01 00:00:00-05".parse().unwrap(),
                "2024-03-01 00:00:00-05".parse().unwrap(),
            ],
        )
        .unwrap();
        conn.close().unwrap();
        let x0 = xs.first().unwrap();
        // println!("{:?}", x0);
        assert_eq!(
            *x0,
            EnergyOffer {
                masked_asset_id: 88805,
                unit_status: UnitStatus::MustRun,
                timestamp_s: 1706763600,
                segment: 0,
                quantity: 3.8,
                price: -150.0,
            }
        );
        assert_eq!(xs.len(), 1566);
        Ok(())
    }

    #[test]
    fn api_energy_offers() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/energy_offers/da/start/2024-01-01/end/2024-01-02?masked_asset_ids=77459",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Value = serde_json::from_str(&response).unwrap();
        if let Value::Array(xs) = v {
            assert_eq!(xs.len(), 192);
        }
        Ok(())
    }

    #[test]
    fn api_stack() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/energy_offers/da/stack/timestamps/1709269200",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Value = serde_json::from_str(&response).unwrap();
        if let Value::Array(xs) = v {
            assert_eq!(xs.len(), 780);
        }
        Ok(())
    }
}
