use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{
    types::{Value, ValueRef},
    AccessMode, Config, Connection, Result,
};
use itertools::Itertools;
use jiff::{civil::Date, Timestamp, ToSpan};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize, Serializer};

use crate::{
    bucket::Bucket,
    db::{ieso::da_lmp_nodes::IesoDaLmpNodalArchive, nyiso::dalmp::LmpComponent},
};

#[derive(Debug, Deserialize)]
struct LmpQuery {
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
    query: web::Query<LmpQuery>,
    db: web::Data<IesoDaLmpNodalArchive>
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(db.duckdb_path.clone(), config).unwrap();

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

    let offers = get_hourly_prices(&conn, start_date, end_date, locations, components).unwrap();
    HttpResponse::Ok().json(offers)
}

// Only ATC bucket currently implemented
#[get("/ieso/dalmp/daily/bucket/{bucket}/start/{start}/end/{end}")]
async fn api_daily_prices(
    path: web::Path<(Bucket, Date, Date)>,
    query: web::Query<LmpQuery>,
    db: web::Data<IesoDaLmpNodalArchive>
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(db.duckdb_path.clone(), config).unwrap();

    let bucket = path.0;
    let start_date = path.1;
    let end_date = path.2;
    if bucket != Bucket::Atc {
        return HttpResponse::NotImplemented().finish();
    }

    let locations: Option<Vec<String>> = query
        .locations
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.to_owned()).collect());

    let components: Option<Vec<LmpComponent>> = query.components.as_ref().map(|ids| {
        ids.split(',')
            .map(|e| e.parse::<LmpComponent>().unwrap())
            .collect()
    });

    let offers = get_daily_prices(&conn, start_date, end_date, locations, components).unwrap();
    HttpResponse::Ok().json(offers)
}

// for hourly data
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
    value: Decimal,
}

// for daily data
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RowD {
    date: Date,
    location_name: String,
    component: LmpComponent,
    bucket: Bucket,
    #[serde(with = "rust_decimal::serde::float")]
    value: Decimal,
}

/// One LMP component only, e.g. mcc, a vector of values per row.
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RowWide {
    date: Date,
    location_name: String,
    values: Vec<Decimal>,
}

pub fn serialize_zoned_as_offset<S>(z: &Timestamp, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&z.strftime("%Y-%m-%d %H:%M:%S-05:00").to_string())
}

pub fn serialize_f64_as_string<S>(v: &f64, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&format!("{:.2}", v))
}

/// Get hourly prices between a [start, end] date for a list of ptids
///
pub fn get_hourly_prices(
    conn: &Connection,
    start: Date,
    end: Date,
    locations: Option<Vec<String>>,
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
        match locations {
            Some(ids) => format!("\nAND location_name in ('{}') ", ids.iter().join("', '")),
            None => "".to_string(),
        },
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let prices_iter = stmt.query_map([], |row| {
        let micro: i64 = row.get(0).unwrap();
        Ok(Row {
            hour_beginning: Timestamp::from_microsecond(micro).unwrap(),
            location_name: row.get(1).unwrap(),
            component: row.get::<usize, String>(2).unwrap().parse().unwrap(),
            value: match row.get_ref_unwrap(3) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
        })
    })?;
    let prices: Vec<Row> = prices_iter.map(|e| e.unwrap()).collect();

    Ok(prices)
}

pub fn get_daily_prices(
    conn: &Connection,
    start: Date,
    end: Date,
    locations: Option<Vec<String>>,
    components: Option<Vec<LmpComponent>>,
) -> Result<Vec<RowD>> {
    let _ = conn.execute("SET TimeZone = 'America/Cancun';", []);
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
    component,
    location_name,
    hour_beginning::DATE AS day,
    'ATC' AS bucket,
    MEAN(price)::DECIMAL(9,4) AS price,
FROM unpivot_alias
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}
GROUP BY component, location_name, day
ORDER BY component, location_name, day; 
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
        match locations {
            Some(ids) => format!("\nAND location_name in ('{}') ", ids.iter().join("', '")),
            None => "".to_string(),
        },
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let prices_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(2).unwrap();
        Ok(RowD {
            date: Date::ZERO.checked_add(n.days()).unwrap(),
            location_name: row.get(1).unwrap(),
            component: row.get::<usize, String>(0).unwrap().parse().unwrap(),
            bucket: Bucket::Atc, // TODO
            value: match row.get_ref_unwrap(4) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
        })
    })?;
    let prices: Vec<RowD> = prices_iter.map(|e| e.unwrap()).collect();

    Ok(prices)
}

pub fn get_prices_wide(
    conn: &Connection,
    start: Date,
    end: Date,
    ptids: Option<Vec<String>>,
    component: LmpComponent,
) -> Result<Vec<RowWide>> {
    let _ = conn.execute("SET TimeZone = 'America/Cancun';", []);
    let query = format!(
        r#"
SELECT 
    location_name,
    hour_beginning::DATE AS day, 
    list({} ORDER BY hour_beginning) AS value 
FROM da_lmp
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}
GROUP BY location_name, day
ORDER BY location_name, day; 
    "#,
        component,
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

    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(1).unwrap();
        let prices = match row.get_ref(2).unwrap().to_owned() {
            Value::List(values) => values
                .iter()
                .map(|e| match e {
                    Value::Decimal(v) => v.to_owned(),
                    _ => panic!("Expected a decimal"),
                })
                .collect::<Vec<Decimal>>(),
            _ => panic!("Expected a list of decimals"),
        };
        Ok(RowWide {
            location_name: row.get(0).unwrap(),
            date: Date::ZERO.checked_add(n.days()).unwrap(),
            values: prices,
        })
    })?;
    let prices: Vec<RowWide> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(prices)
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error, path::Path, vec};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use rust_decimal_macros::dec;

    use crate::{api::ieso::dalmp::*, db::prod_db::ProdDb};

    #[test]
    fn test_hourly_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::ieso_dalmp_zonal().duckdb_path, config).unwrap();
        let data = get_hourly_prices(
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
                value: dec!(30.92),
            }
        );

        Ok(())
    }

    #[test]
    fn test_daily_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::ieso_dalmp_zonal().duckdb_path, config).unwrap();
        let data = get_daily_prices(
            &conn,
            date(2025, 8, 31),
            date(2025, 9, 4),
            Some(vec!["TORONTO".to_owned()]),
            Some(vec![LmpComponent::Lmp]),
        )
        .unwrap();
        // println!("{:?}", data);
        assert_eq!(data.len(), 5); // 5 days
        assert_eq!(
            data[0],
            RowD {
                date: "2025-08-31".parse()?,
                location_name: "TORONTO".to_owned(),
                component: LmpComponent::Lmp,
                bucket: Bucket::Atc,
                value: dec!(35.9858),
            }
        );

        Ok(())
    }

    #[test]
    fn test_data_wide() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::ieso_dalmp_zonal().duckdb_path, config).unwrap();
        let data = get_prices_wide(
            &conn,
            date(2025, 9, 1),
            date(2025, 9, 3),
            Some(vec!["TORONTO".to_owned()]),
            LmpComponent::Lmp,
        )
        .unwrap();
        assert_eq!(data.len(), 3); // three days
        assert_eq!(
            data[0],
            RowWide {
                location_name: "TORONTO".to_owned(),
                date: date(2025, 9, 1),
                values: vec![
                    dec!(29.77),
                    dec!(25.09),
                    dec!(15.32),
                    dec!(20.09),
                    dec!(20.09),
                    dec!(25.09),
                    dec!(25.37),
                    dec!(30.50),
                    dec!(31.49),
                    dec!(31.67),
                    dec!(35.50),
                    dec!(35.44),
                    dec!(36.41),
                    dec!(39.87),
                    dec!(40.49),
                    dec!(46.43),
                    dec!(58.93),
                    dec!(69.14),
                    dec!(68.12),
                    dec!(71.07),
                    dec!(49.11),
                    dec!(40.10),
                    dec!(36.39),
                    dec!(33.79)
                ],
            }
        );

        Ok(())
    }

    #[test]
    fn api_hourly_test() -> Result<(), reqwest::Error> {
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

    #[test]
    fn api_daily_test() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/ieso/dalmp/daily/bucket/atc/start/2025-08-31/end/2025-09-04?locations=TORONTO,WEST&components=lmp",
            env::var("RUST_SERVER").unwrap(),
        );
        println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<RowD> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 10); // 5 days x 2 locations
        assert_eq!(
            vs[0],
            RowD {
                date: "2025-08-31".parse()?,
                location_name: "TORONTO".to_owned(),
                component: LmpComponent::Lmp,
                bucket: Bucket::Atc,
                value: dec!(35.9858),
            }
        );

        Ok(())
    }
}
