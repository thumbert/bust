use std::fmt;

use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{types::ValueRef, AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{civil::Date, tz::TimeZone, Timestamp, ToSpan, Zoned};
use rust_decimal::Decimal;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

use crate::db::prod_db::ProdDb;

#[derive(Debug, Deserialize)]
struct OffersQuery {
    /// One or more masked participant ids, separated by commas
    /// If not specified, return all of them.  Use carefully
    /// because it's a lot of data...
    masked_participant_ids: Option<String>,

    /// One or more masked asset ids, separated by commas
    /// If not specified, return all of them.  Use carefully
    /// because it's a lot of data...
    masked_asset_ids: Option<String>,
}

#[get("/isone/daas_offers/start/{start}/end/{end}")]
async fn api_offers(
    path: web::Path<(Date, Date)>,
    query: web::Query<OffersQuery>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(ProdDb::isone_masked_daas_offers().duckdb_path, config)
        .unwrap();

    let start_date = path.0;
    let end_date = path.1;

    let participant_ids: Option<Vec<i32>> = query
        .masked_participant_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.parse::<i32>().unwrap()).collect());

    let asset_ids: Option<Vec<i32>> = query
        .masked_asset_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.parse::<i32>().unwrap()).collect());

    let offers = get_offers(&conn, start_date, end_date, participant_ids, asset_ids).unwrap();
    HttpResponse::Ok().json(offers)
}

pub fn serialize_zoned_as_offset<S>(z: &Zoned, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&z.strftime("%Y-%m-%d %H:%M:%S%:z").to_string())
}

// Custom deserialization function for the Zoned field
pub fn deserialize_zoned_assume_ny<'de, D>(deserializer: D) -> Result<Zoned, D::Error>
where
    D: Deserializer<'de>,
{
    struct ZonedVisitor;

    impl Visitor<'_> for ZonedVisitor {
        type Value = Zoned;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a timestamp string with or without a zone name")
        }

        fn visit_str<E>(self, v: &str) -> Result<Zoned, E>
        where
            E: de::Error,
        {
            // Otherwise, append the assumed zone
            let s = format!("{v}[America/New_York]");
            Zoned::strptime("%F %T%:z[%Q]", &s).map_err(E::custom)
        }
    }

    deserializer.deserialize_str(ZonedVisitor)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DaasOffer {
    #[serde(serialize_with = "serialize_zoned_as_offset", deserialize_with = "deserialize_zoned_assume_ny")]
    hour_beginning: Zoned,
    masked_participant_id: i32,
    masked_asset_id: i32,
    #[serde(with = "rust_decimal::serde::float")]
    offer_mw: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    tmsr_offer_price: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    tmnsr_offer_price: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    tmor_offer_price: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    eir_offer_price: Decimal,
}

/// Get the energy offers between a [start, end] date for a list of units
/// (or all units)
pub fn get_offers(
    conn: &Connection,
    start: Date,
    end: Date,
    masked_participant_ids: Option<Vec<i32>>,
    masked_unit_ids: Option<Vec<i32>>,
) -> Result<Vec<DaasOffer>> {
    let query = format!(
        r#"
SELECT 
    hour_beginning,
    masked_lead_participant_id AS masked_participant_id, 
    masked_asset_id,
    offer_mw,
    tmsr_offer_price,
    tmnsr_offer_price,
    tmor_offer_price,
    eir_offer_price
FROM offers
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}{}
ORDER BY hour_beginning;    
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
        match masked_participant_ids {
            Some(ids) => format!(
                "\nAND masked_lead_participant_id in ({}) ",
                ids.iter().join(", ")
            ),
            None => "".to_string(),
        },
        match masked_unit_ids {
            Some(ids) => format!("\nAND masked_asset_id in ({}) ", ids.iter().join(", ")),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let micro: i64 = row.get(0).unwrap();
        Ok(DaasOffer {
            hour_beginning: Zoned::new(
                Timestamp::from_microsecond(micro).unwrap(),
                TimeZone::get("America/New_York").unwrap(),
            ),
            masked_participant_id: row.get(1).unwrap(),
            masked_asset_id: row.get(2).unwrap(),
            offer_mw: match row.get_ref_unwrap(3) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
            tmnsr_offer_price: match row.get_ref_unwrap(4) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
            tmsr_offer_price: match row.get_ref_unwrap(5) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
            tmor_offer_price: match row.get_ref_unwrap(6) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
            eir_offer_price: match row.get_ref_unwrap(7) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
        })
    })?;
    let offers: Vec<DaasOffer> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use rust_decimal_macros::dec;

    use crate::api::isone::masked_daas_offers::*;

    #[test]
    fn test_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::isone_masked_daas_offers().duckdb_path, config)
                .unwrap();
        let data = get_offers(
            &conn,
            date(2025, 3, 2),
            date(2025, 3, 2),
            None,
            Some(vec![98805]),
        )
        .unwrap();
        assert_eq!(data.len(), 24);
        assert_eq!(
            data[0],
            DaasOffer {
                hour_beginning: "2025-03-02 00:00[America/New_York]".parse()?,
                masked_participant_id: 504170,
                masked_asset_id: 98805,
                offer_mw: dec!(58.40),
                tmsr_offer_price: dec!(258.57),
                tmnsr_offer_price: dec!(258.57),
                tmor_offer_price: dec!(258.57),
                eir_offer_price: dec!(258.57),
            }
        );

        Ok(())
    }

    #[test]
    fn api_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/daas_offers/start/2025-03-02/end/2025-03-02?masked_asset_ids=98805",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<DaasOffer> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 24);
        Ok(())
    }
}
