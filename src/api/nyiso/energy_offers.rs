use core::fmt;
use std::{fmt::Debug, str::FromStr, time::Duration};

use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{civil::Date, Timestamp, ToSpan};
use serde::{Deserialize, Serialize};

use crate::{db::nyiso::energy_offers::NyisoEnergyOffersArchive, utils::lib_duckdb::open_with_retry};

#[derive(Debug, Deserialize)]
struct OffersQuery {
    /// one or more masked asset ids, separated by commas
    masked_asset_ids: Option<String>,
}

#[get("/nyiso/energy_offers/{market}/start/{start}/end/{end}")]
async fn api_offers(
    path: web::Path<(String, String, String)>,
    query: web::Query<OffersQuery>,
    db: web::Data<NyisoEnergyOffersArchive>,
) -> impl Responder {
    let conn = open_with_retry(&db.duckdb_path, 8, Duration::from_millis(25), AccessMode::ReadOnly);
    if let Err(e) = conn {
        return HttpResponse::InternalServerError()
            .body(format!("Error opening DuckDB database: {}", e));
    }

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
    let asset_ids: Option<Vec<i32>> = query.masked_asset_ids.as_ref().map(|ids| {
        ids.split(',')
            .map(|e| e.trim().parse::<i32>().unwrap())
            .collect()
    });

    let offers = get_energy_offers(&conn.unwrap(), market, start_date, end_date, asset_ids).unwrap();
    HttpResponse::Ok().json(offers)
}

/// Get DAM or HAM stack for a list of timestamps (seconds from epoch)
#[get("/nyiso/energy_offers/{market}/stack/timestamps/{timestamps}")]
async fn api_stack(path: web::Path<(String, String)>, db: web::Data<NyisoEnergyOffersArchive>) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(db.duckdb_path.clone(), config).unwrap();

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
pub struct EnergyOffer {
    masked_asset_id: u32,
    timestamp_s: i64, // seconds since epoch of hour beginning
    segment: u8,
    price: f32,
    quantity: f32,
}

#[derive(Debug, PartialEq, Serialize)]
pub enum Market {
    Dam,
    Ham,
}

impl fmt::Display for Market {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{:?}", self)
    }
}

impl FromStr for Market {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "DAM" | "DA" => Ok(Market::Dam),
            "HAM" | "RT" | "RTM" => Ok(Market::Ham),
            _ => Err(format!("Can't parse market: {}", s)),
        }
    }
}

// Get the masked unit ids between a [start, end] date
pub fn get_unit_ids(conn: &Connection, start: Date, end: Date) -> Vec<u32> {
    let mut query = String::from("SELECT DISTINCT \"Masked Gen ID\" from offers ");
    query.push_str(&format!("WHERE \"Date Time\" >= '{}' ", start));
    query.push_str(&format!(
        "AND \"Date Time\" < '{}' ",
        end.checked_add(1.day()).ok().unwrap()
    ));
    query.push_str("ORDER BY \"Masked Gen ID\"");
    query.push(';');
    // println!("{}", query);

    let mut stmt = conn.prepare(&query).unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut ids: Vec<u32> = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        ids.push(row.get(0).unwrap());
    }
    ids
}

// Get the energy offers between a [start, end] date for a list of units and participant ids
pub fn get_energy_offers(
    conn: &Connection,
    market: Market,
    start: Date,
    end: Date,
    masked_unit_ids: Option<Vec<i32>>,
    // masked_participant_ids: Vec<i32>,
) -> Result<Vec<EnergyOffer>> {
    let query = format!(
        r#"
WITH unpivot_alias AS (
    UNPIVOT (
        SELECT "Masked Gen ID", "Date Time", 
            "Dispatch $/MW1",
            "Dispatch $/MW2",
            "Dispatch $/MW3",
            "Dispatch $/MW4",
            "Dispatch $/MW5",
            "Dispatch $/MW6",
            "Dispatch $/MW7",
            "Dispatch $/MW8",
            "Dispatch $/MW9",
            "Dispatch $/MW10",
            "Dispatch $/MW11",
            "Dispatch $/MW12",
            "Dispatch MW1" AS MW1, 
            ROUND("Dispatch MW2" - "Dispatch MW1", 1) AS MW2, 
            ROUND("Dispatch MW3" - "Dispatch MW2", 1) AS MW3, 
            ROUND("Dispatch MW4" - "Dispatch MW3", 1) AS MW4, 
            ROUND("Dispatch MW5" - "Dispatch MW4", 1) AS MW5, 
            ROUND("Dispatch MW6" - "Dispatch MW5", 1) AS MW6, 
            ROUND("Dispatch MW7" - "Dispatch MW6", 1) AS MW7, 
            ROUND("Dispatch MW8" - "Dispatch MW7", 1) AS MW8, 
            ROUND("Dispatch MW9" - "Dispatch MW8", 1) AS MW9, 
            ROUND("Dispatch MW10" - "Dispatch MW9", 1) AS MW10, 
            ROUND("Dispatch MW11" - "Dispatch MW10", 1) AS MW11, 
            ROUND("Dispatch MW12" - "Dispatch MW11", 1) AS MW12,  
        FROM offers
        WHERE "Date Time" >= '{}'
        AND "Date Time" < '{}'
        {}
        AND "Market" == '{}'
    )
    ON  ("MW1", "Dispatch $/MW1") AS "0", 
        ("MW2", "Dispatch $/MW2") AS "1", 
        ("MW3", "Dispatch $/MW3") AS "2",
        ("MW4", "Dispatch $/MW4") AS "3", 
        ("MW5", "Dispatch $/MW5") AS "4",
        ("MW6", "Dispatch $/MW6") AS "5", 
        ("MW7", "Dispatch $/MW7") AS "6",
        ("MW8", "Dispatch $/MW8") AS "7", 
        ("MW9", "Dispatch $/MW9") AS "8",
        ("MW10", "Dispatch $/MW10") AS "9", 
        ("MW11", "Dispatch $/MW11") AS "10",
        ("MW12", "Dispatch $/MW12") AS "11",
    INTO  
        NAME Segment
        VALUE MW, Price
)
SELECT "Masked Gen ID", 
    "Date Time", 
    CAST("Segment" as UTINYINT) AS Segment, 
    "MW", "Price", 
FROM unpivot_alias
ORDER BY "Masked Gen ID", "Date Time", "Price";    
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
            Some(ids) => format!("AND \"Masked Gen ID\" in ({}) ", ids.iter().join(", ")),
            None => "".to_string(),
        },
        market.to_string().to_uppercase(),
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let micro: i64 = row.get(1).unwrap();
        Ok(EnergyOffer {
            masked_asset_id: row.get(0).unwrap(),
            timestamp_s: micro / 1_000_000,
            segment: row.get(2)?,
            price: row.get(4)?,
            quantity: row.get(3)?,
        })
    })?;
    let offers: Vec<EnergyOffer> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

/// Construct the stack
pub fn get_stack(
    conn: &Connection,
    market: Market,
    timestamps: Vec<Timestamp>,
) -> Result<Vec<EnergyOffer>> {
    let query = format!(
        r#"
        WITH unpivot_alias AS (
            UNPIVOT (
                SELECT "Masked Gen ID", "Date Time", 
                    "Dispatch $/MW1",
                    "Dispatch $/MW2",
                    "Dispatch $/MW3",
                    "Dispatch $/MW4",
                    "Dispatch $/MW5",
                    "Dispatch $/MW6",
                    "Dispatch $/MW7",
                    "Dispatch $/MW8",
                    "Dispatch $/MW9",
                    "Dispatch $/MW10",
                    "Dispatch $/MW11",
                    "Dispatch $/MW12",
                    "Dispatch MW1" AS MW1, 
                    ROUND("Dispatch MW2" - "Dispatch MW1", 1) AS MW2, 
                    ROUND("Dispatch MW3" - "Dispatch MW2", 1) AS MW3, 
                    ROUND("Dispatch MW4" - "Dispatch MW3", 1) AS MW4, 
                    ROUND("Dispatch MW5" - "Dispatch MW4", 1) AS MW5, 
                    ROUND("Dispatch MW6" - "Dispatch MW5", 1) AS MW6, 
                    ROUND("Dispatch MW7" - "Dispatch MW6", 1) AS MW7, 
                    ROUND("Dispatch MW8" - "Dispatch MW7", 1) AS MW8, 
                    ROUND("Dispatch MW9" - "Dispatch MW8", 1) AS MW9, 
                    ROUND("Dispatch MW10" - "Dispatch MW9", 1) AS MW10, 
                    ROUND("Dispatch MW11" - "Dispatch MW10", 1) AS MW11, 
                    ROUND("Dispatch MW12" - "Dispatch MW11", 1) AS MW12,  
                FROM offers
                WHERE "Market" == '{}'
                {}
            )
            ON  ("MW1", "Dispatch $/MW1") AS "0", 
                ("MW2", "Dispatch $/MW2") AS "1", 
                ("MW3", "Dispatch $/MW3") AS "2",
                ("MW4", "Dispatch $/MW4") AS "3", 
                ("MW5", "Dispatch $/MW5") AS "4",
                ("MW6", "Dispatch $/MW6") AS "5", 
                ("MW7", "Dispatch $/MW7") AS "6",
                ("MW8", "Dispatch $/MW8") AS "7", 
                ("MW9", "Dispatch $/MW9") AS "8",
                ("MW10", "Dispatch $/MW10") AS "9", 
                ("MW11", "Dispatch $/MW11") AS "10",
                ("MW12", "Dispatch $/MW12") AS "11",
            INTO  
                NAME Segment
                VALUE MW, Price
        )
        SELECT "Masked Gen ID", 
            "Date Time", 
            CAST("Segment" as UTINYINT) AS Segment, 
            "MW", "Price", 
        FROM unpivot_alias
        WHERE MW > 0
        ORDER BY "Masked Gen ID", "Date Time", "Price";    
    "#,
        market.to_string().to_uppercase(),
        match timestamps.len() {
            1 => format!(
                r#"AND "Date Time" == '{}' "#,
                timestamps
                    .first()
                    .unwrap()
                    .in_tz("America/New_York")
                    .unwrap()
                    .strftime("%Y-%m-%d %H:%M:%S.000%:z")
            ),
            _ => format!(
                r#"AND "Date Time" in ('{}')"#,
                timestamps
                    .iter()
                    .map(|e| e
                        .in_tz("America/New_York")
                        .unwrap()
                        .strftime("%Y-%m-%d %H:%M:%S.000%:z"))
                    .join("', '")
            ),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let micro: i64 = row.get(1).unwrap();
        Ok(EnergyOffer {
            masked_asset_id: row.get(0).unwrap(),
            timestamp_s: micro / 1_000_000,
            segment: row.get(2)?,
            price: row.get(4)?,
            quantity: row.get(3)?,
        })
    })?;
    let offers: Vec<EnergyOffer> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::{date, Date};
    use serde_json::Value;

    use crate::{api::nyiso::energy_offers::*, db::prod_db::ProdDb};

    #[test]
    fn test_market() {
        assert_eq!("DAM".parse::<Market>(), Ok(Market::Dam));
        assert_eq!("DA".parse::<Market>(), Ok(Market::Dam));
        assert_eq!("dam".parse::<Market>(), Ok(Market::Dam));
    }

    #[test]
    fn test_get_masked_unit_ids() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::nyiso_energy_offers().duckdb_path, config).unwrap();
        let start: Date = date(2023, 1, 1);
        let end: Date = date(2023, 1, 31);
        let ids = get_unit_ids(&conn, start, end);
        assert_eq!(ids.len(), 316);
        conn.close().unwrap();
        Ok(())
    }

    #[test]
    fn test_get_offers() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::nyiso_energy_offers().duckdb_path, config).unwrap();
        let xs = get_energy_offers(
            &conn,
            Market::Dam,
            date(2024, 3, 1),
            date(2024, 3, 1),
            Some(vec![35537750, 55537750, 67537750, 75537750]),
        )
        .unwrap();
        conn.close().unwrap();
        let x0 = xs.first().unwrap();
        assert_eq!(
            *x0,
            EnergyOffer {
                masked_asset_id: 35537750,
                timestamp_s: 1709269200,
                segment: 0,
                price: 15.6,
                quantity: 150.0
            }
        );
        assert_eq!(xs.len(), 672);
        Ok(())
    }

    #[test]
    fn test_get_stack() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::nyiso_energy_offers().duckdb_path, config).unwrap();
        let xs = get_stack(
            &conn,
            Market::Dam,
            vec!["2024-03-01 00:00:00-05".parse().unwrap()],
        )
        .unwrap();
        conn.close().unwrap();
        let x0 = xs.iter().find(|&x| x.masked_asset_id == 37796180).unwrap();
        // println!("{:?}", x0);
        assert_eq!(
            *x0,
            EnergyOffer {
                masked_asset_id: 37796180,
                timestamp_s: 1709269200,
                segment: 0,
                quantity: 50.0,
                price: -999.0,
            }
        );
        assert_eq!(xs.len(), 751);
        Ok(())
    }

    #[test]
    fn api_energy_offers() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/nyiso/energy_offers/dam/start/2024-01-01/end/2024-01-02?masked_asset_ids=35537750",
            env::var("RUST_SERVER").unwrap(),
        );
        println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Value = serde_json::from_str(&response).unwrap();
        if let Value::Array(xs) = v {
            assert_eq!(xs.len(), 336);
        }
        Ok(())
    }

    #[test]
    fn api_stack() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/nyiso/energy_offers/dam/stack/timestamps/1709269200",
            env::var("RUST_SERVER").unwrap(),
        );
        // println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let v: Value = serde_json::from_str(&response).unwrap();
        if let Value::Array(xs) = v {
            assert_eq!(xs.len(), 751);
        }
        Ok(())
    }
}
