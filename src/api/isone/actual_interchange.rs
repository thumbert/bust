use std::{fmt, str::FromStr};

use actix_web::{get, web, HttpResponse, Responder};

use crate::api::isone::masked_daas_offers::{deserialize_zoned_assume_ny, serialize_zoned_as_offset};
use duckdb::{types::ValueRef, AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{civil::Date, tz::TimeZone, Timestamp, ToSpan, Zoned};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::db::prod_db::ProdDb;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Copy)]
pub enum FlowComponent {
    Net,
    Purchase,
    Sale,
}

impl fmt::Display for FlowComponent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            FlowComponent::Net => write!(f, "net"),
            FlowComponent::Purchase => write!(f, "purchase"),
            FlowComponent::Sale => write!(f, "sale"),
        }
    }
}

fn parse_component(s: &str) -> Result<FlowComponent, String> {
    match s.to_lowercase().as_str() {
        "net" => Ok(FlowComponent::Net),
        "purchase" => Ok(FlowComponent::Purchase),
        "sale" => Ok(FlowComponent::Sale),
        _ => Err(format!("Unknown flow component: {}", s)),
    }
}

impl FromStr for FlowComponent {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parse_component(s) {
            Ok(month) => Ok(month),
            Err(_) => Err(format!("Failed parsing {} as a Flow component", s)),
        }
    }
}

#[derive(Debug, Deserialize)]
struct Query {
    /// One or more ptids, separated by commas.
    /// If not specified, return all ptids.  Use carefully
    /// because it's a lot of data...
    ptids: Option<String>,

    /// One or more flow components, separated by commas.
    /// Valid values are: actual, purchase, sale.
    /// If not specified, return all of three.
    components: Option<String>,
}

#[get("/isone/actual_interchange/hourly/start/{start}/end/{end}")]
async fn api_actual_flows(
    path: web::Path<(Date, Date)>,
    query: web::Query<Query>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(ProdDb::isone_actual_interchange().duckdb_path, config)
        .unwrap();

    let start_date = path.0;
    let end_date = path.1;

    let ptids: Option<Vec<u32>> = query
        .ptids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.parse::<u32>().unwrap()).collect());

    let components: Option<Vec<FlowComponent>> = query.components.as_ref().map(|ids| {
        ids.split(',')
            .map(|e| e.parse::<FlowComponent>().unwrap())
            .collect()
    });

    let offers = get_flows(&conn, start_date, end_date, ptids, components).unwrap();
    HttpResponse::Ok().json(offers)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    hour_beginning: Zoned,
    ptid: u32,
    component: FlowComponent,
    #[serde(with = "rust_decimal::serde::float")]
    mw: Decimal,
}

/// Get hourly prices between a [start, end] date for a list of ptids
///
pub fn get_flows(
    conn: &Connection,
    start: Date,
    end: Date,
    ptids: Option<Vec<u32>>,
    components: Option<Vec<FlowComponent>>,
) -> Result<Vec<Row>> {
    let query = format!(
        r#"
WITH unpivot_alias AS (
    UNPIVOT flows
    ON {}
    INTO
        NAME component
        VALUE "value"
)
SELECT 
    hour_beginning, 
    ptid,
    component,
    "value"
FROM unpivot_alias
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}
ORDER BY component, ptid, hour_beginning; 
    "#,
        match components {
            Some(cs) => cs.iter().join(", ").to_string(),
            None => "net, purchase, sale".to_string(),
        },
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
        match ptids {
            Some(ids) => format!("\nAND ptid in ({}) ", ids.iter().join(", ")),
            None => "".to_string(),
        },
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let micro: i64 = row.get(0).unwrap();
        Ok(Row {
            hour_beginning: Zoned::new(
                Timestamp::from_microsecond(micro).unwrap(),
                TimeZone::get("America/New_York").unwrap(),
            ),
            ptid: row.get(1).unwrap(),
            component: row.get::<usize, String>(2).unwrap().parse().unwrap(),
            mw: match row.get_ref_unwrap(3) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
        })
    })?;
    let offers: Vec<Row> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error, path::Path, vec};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use rust_decimal_macros::dec;

    use crate::{api::isone::actual_interchange::*, db::prod_db::ProdDb};

    #[test]
    fn test_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_actual_interchange().duckdb_path, config).unwrap();
        let data = get_flows(
            &conn,
            date(2025, 7, 14),
            date(2025, 7, 14),
            Some(vec![4010]),
            Some(vec![FlowComponent::Net]),
        )
        .unwrap();
        assert_eq!(data.len(), 24);
        assert_eq!(
            data[0],
            Row {
                hour_beginning: "2025-07-14 00:00[America/New_York]".parse()?,
                ptid: 4010,
                component: FlowComponent::Net,
                mw: dec!(-236),
            }
        );

        Ok(())
    }

    #[test]
    fn api_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/actual_interchange/hourly/start/2025-01-01/end/2025-01-05?ptids=4010,4011",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
        println!("{:#?}", vs);
        assert_eq!(vs.len(), 5 * 24 * 2 * 3); // 5 days, 24 hours, 2 ptids, 3 components
        Ok(())
    }
}
