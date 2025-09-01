use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{types::ValueRef, AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{civil::Date, Timestamp, ToSpan};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, Serializer};

use crate::db::{nyiso::dalmp::LmpComponent, prod_db::ProdDb};

#[derive(Debug, Deserialize)]
struct OffersQuery {
    /// One or more locations, separated by commas.
    /// If not specified, return all ptids.  Use carefully
    /// because it's a lot of data...
    locations: Option<String>,

    /// One or more LMP components, separated by commas.
    /// Valid values are: lmp, mcc, mlc.
    /// If not specified, return all of three.
    components: Option<String>,
}

#[get("/ieso/dalmp/hourly/start/{start}/end/{end}")]
async fn api_hourly_prices(
    path: web::Path<(Date, Date)>,
    query: web::Query<OffersQuery>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(ProdDb::ieso_dalmp_zonal().duckdb_path, config).unwrap();

    let start_date = path.0;
    let end_date = path.1;

    let locations: Option<Vec<String>> = query
        .locations
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.to_owned()).collect());

    let components: Option<Vec<LmpComponent>> = query.components.as_ref().map(|ids| {
        ids.split(',')
            .map(|e| e.parse::<LmpComponent>().unwrap())
            .collect()
    });

    let offers = get_prices(&conn, start_date, end_date, locations, components).unwrap();
    HttpResponse::Ok().json(offers)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        // deserialize_with = "deserialize_zoned_assume_ny"
    )]
    hour_beginning: Timestamp,
    location_name: String,
    component: LmpComponent,
    #[serde(with = "rust_decimal::serde::float")]
    price: Decimal,
}

pub fn serialize_zoned_as_offset<S>(z: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&z.strftime("%Y-%m-%d %H:%M:%S-05:00").to_string())
}


/// Get hourly prices between a [start, end] date for a list of ptids
///
pub fn get_prices(
    conn: &Connection,
    start: Date,
    end: Date,
    ptids: Option<Vec<String>>,
    components: Option<Vec<LmpComponent>>,
) -> Result<Vec<Row>> {
    let query = format!(
        r#"
WITH unpivot_alias AS (
    UNPIVOT da_lmp
    ON {}
    INTO
        NAME component
        VALUE price
)
SELECT 
    hour_beginning, 
    location_name,
    component,
    price
FROM unpivot_alias
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}
ORDER BY component, location_name, hour_beginning; 
    "#,
        match components {
            Some(cs) => cs.iter().join(", ").to_string(),
            None => "lmp, mcc, mcl".to_string(),
        },
        start.strftime("%Y-%m-%d 00:00:00.000-05:00"),
        end.checked_add(1.day())
            .ok()
            .unwrap()
            .strftime("%Y-%m-%d 00:00:00.000-05:00"),
        match ptids {
            Some(ids) => format!("\nAND location_name in ('{}') ", ids.iter().join("', '")),
            None => "".to_string(),
        },
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let micro: i64 = row.get(0).unwrap();
        Ok(Row {
            hour_beginning: Timestamp::from_microsecond(micro).unwrap(),
            location_name: row.get(1).unwrap(),
            component: row.get::<usize, String>(2).unwrap().parse().unwrap(),
            price: match row.get_ref_unwrap(3) {
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

    use crate::{api::ieso::dalmp::*, db::prod_db::ProdDb};

    #[test]
    fn test_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::ieso_dalmp_zonal().duckdb_path, config).unwrap();
        let data = get_prices(
            &conn,
            date(2025, 8, 31),
            date(2025, 8, 31),
            Some(vec!["TORONTO".to_owned()]),
            Some(vec![LmpComponent::Lmp]),
        )
        .unwrap();
        assert_eq!(data.len(), 24);
        assert_eq!(
            data[0],
            Row {
                hour_beginning: "2025-08-31 00:00:00.000-05:00".parse()?,
                location_name: "TORONTO".to_owned(),
                component: LmpComponent::Lmp,
                price: dec!(30.92),
            }
        );

        Ok(())
    }

    #[test]
    fn api_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/ieso/dalmp/hourly/start/2025-08-31/end/2025-08-31?locations=TORONTO,WEST&components=lmp",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 48);
        Ok(())
    }
}
