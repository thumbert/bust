use actix_web::{get, web, HttpResponse, Responder};

use crate::{
    api::isone::{
        _api_isone_core::Market,
        masked_daas_offers::{deserialize_zoned_assume_ny, serialize_zoned_as_offset},
    },
    bucket::{Bucket, BucketLike},
    db::{
        calendar::buckets::BucketsArchive,
        isone::{dalmp_archive::IsoneDalmpArchive, rtlmp_archive::IsoneRtLmpArchive},
    },
    interval::{
        month::{month, Month},
        month_tz::MonthTz,
        term::Term,
    },
};
use duckdb::{types::ValueRef, AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{civil::Date, tz::TimeZone, Timestamp, ToSpan, Zoned};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::db::nyiso::dalmp::LmpComponent;

#[derive(Debug, Deserialize)]
struct LmpQuery {
    /// One or more ptids, separated by commas.
    /// If not specified, return all ptids.  Use carefully
    /// because it's a lot of data...
    ptids: Option<String>,

    /// One or more LMP components, separated by commas.
    /// Valid values are: lmp, mcc, mlc.
    /// If not specified, return all of three.
    components: Option<String>,

    /// Valid values are: default, compact.  The default value returns data in long format,
    /// each row of containing {'hour_beginning', 'ptid', 'component', 'price'}.
    /// The compact format returns data with following shape:
    /// {'2025-01-01': {4000: <num>[...], 4001: <num>[...]}, '2025-01-02': {...}, ...}
    format: Option<String>,
}

#[get("/isone/prices/{market}/hourly/start/{start}/end/{end}")]
async fn api_hourly_prices(
    path: web::Path<(Market, Date, Date)>,
    query: web::Query<LmpQuery>,
    db: web::Data<(IsoneDalmpArchive, IsoneRtLmpArchive, BucketsArchive)>,
) -> impl Responder {
    let market = path.0;
    let start_date = path.1;
    let end_date = path.2;

    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = match market {
        Market::DA => Connection::open_with_flags(db.0.duckdb_path.clone(), config).unwrap(),
        Market::RT => Connection::open_with_flags(db.1.duckdb_path.clone(), config).unwrap(),
    };

    let ptids: Option<Vec<u32>> = query
        .ptids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.parse::<u32>().unwrap()).collect());

    let components: Option<Vec<LmpComponent>> = query.components.as_ref().map(|ids| {
        ids.split(',')
            .map(|e| e.parse::<LmpComponent>().unwrap())
            .collect()
    });

    let format = query.format.clone().unwrap_or("default".into());
    match format.as_str() {
        "compact" => {
            let component = match &components {
                Some(cs) if cs.len() == 1 => cs[0],
                _ => {
                    return HttpResponse::BadRequest()
                        .body("Compact format requires exactly one component specified");
                }
            };
            let prices =
                get_hourly_prices_compact(&conn, start_date, end_date, ptids, component).unwrap();
            use actix_web::http::header::HeaderName;
            HttpResponse::Ok()
                .insert_header((HeaderName::from_static("content-type"), "application/json"))
                .body(prices)
        }
        _ => {
            let offers =
                get_hourly_prices(&conn, start_date, end_date, market, ptids, components).unwrap();
            HttpResponse::Ok().json(offers)
        }
    }
}

#[derive(Debug, Deserialize)]
struct LmpQuery2 {
    /// One or more ptids, separated by commas.
    /// If not specified, return all ptids.  Use carefully
    /// because it's a lot of data...
    ptids: Option<String>,

    /// One or more bucket names, separated by commas.
    /// Valid values are: 5x16, 2x16H, 7x8, atc, offpeak, etc.
    buckets: Option<String>,

    /// Default value: lmp
    component: Option<LmpComponent>,

    /// Statistic: mean, min, max, median, etc.  Default: mean
    statistic: Option<String>,
}

#[get("/isone/prices/{market}/daily/start/{start}/end/{end}")]
async fn api_daily_prices(
    path: web::Path<(Market, Date, Date)>,
    query: web::Query<LmpQuery2>,
    db: web::Data<(IsoneDalmpArchive, IsoneRtLmpArchive, BucketsArchive)>,
) -> impl Responder {
    let market = path.0;
    let start_date = path.1;
    let end_date = path.2;

    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = match market {
        Market::DA => Connection::open_with_flags(db.0.duckdb_path.clone(), config).unwrap(),
        Market::RT => Connection::open_with_flags(db.1.duckdb_path.clone(), config).unwrap(),
    };

    let ptids: Option<Vec<i32>> = query.ptids.as_ref().map(|ids| {
        ids.split(',')
            .map(|e| e.trim().parse::<i32>().unwrap())
            .collect()
    });

    let buckets: Vec<Bucket> = query
        .buckets
        .as_ref()
        .map(|ids| {
            ids.split(',')
                .map(|e| e.parse::<Bucket>().unwrap())
                .collect()
        })
        .unwrap_or(vec![Bucket::Atc]);

    let component = query.component.unwrap_or(LmpComponent::Lmp);
    let statistic = query.statistic.clone().unwrap_or("mean".into());

    let prices = get_daily_prices(
        &conn,
        Term {
            start: start_date,
            end: end_date,
        },
        ptids,
        component,
        buckets,
        statistic,
        &db.2.duckdb_path,
    )
    .unwrap();
    HttpResponse::Ok().json(prices)
}

#[get("/isone/prices/{market}/monthly/start/{start}/end/{end}")]
async fn api_monthly_prices(
    path: web::Path<(Market, Month, Month)>,
    query: web::Query<LmpQuery2>,
    db: web::Data<(IsoneDalmpArchive, IsoneRtLmpArchive, BucketsArchive)>,
) -> impl Responder {
    let market = path.0;
    let start_month = path.1;
    let end_month = path.2;

    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = match market {
        Market::DA => Connection::open_with_flags(db.0.duckdb_path.clone(), config).unwrap(),
        Market::RT => Connection::open_with_flags(db.1.duckdb_path.clone(), config).unwrap(),
    };

    let ptids: Option<Vec<i32>> = query.ptids.as_ref().map(|ids| {
        ids.split(',')
            .map(|e| e.trim().parse::<i32>().unwrap())
            .collect()
    });

    let buckets: Vec<Bucket> = query
        .buckets
        .as_ref()
        .map(|ids| {
            ids.split(',')
                .map(|e| e.parse::<Bucket>().unwrap())
                .collect()
        })
        .unwrap_or(vec![Bucket::Atc]);

    let component = query.component.unwrap_or(LmpComponent::Lmp);

    let statistic = query.statistic.clone().unwrap_or("mean".into());

    let prices = get_monthly_prices(
        &conn,
        start_month,
        end_month,
        ptids,
        component,
        buckets,
        statistic,
        &db.2.duckdb_path,
    )
    .unwrap();
    HttpResponse::Ok().json(prices)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    hour_beginning: Zoned,
    ptid: u32,
    component: LmpComponent,
    #[serde(with = "rust_decimal::serde::float")]
    price: Decimal,
}

/// Get hourly prices between a [start, end] date for a list of ptids
///
pub fn get_hourly_prices(
    conn: &Connection,
    start: Date,
    end: Date,
    market: Market,
    ptids: Option<Vec<u32>>,
    components: Option<Vec<LmpComponent>>,
) -> Result<Vec<Row>> {
    let query = format!(
        r#"
WITH unpivot_alias AS (
    UNPIVOT {}_lmp
    ON {}
    INTO
        NAME component
        VALUE price
)
SELECT 
    hour_beginning, 
    ptid,
    component,
    price
FROM unpivot_alias
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}
ORDER BY component, ptid, hour_beginning; 
    "#,
        market.to_string().to_lowercase(),
        match components {
            Some(cs) => cs.iter().join(", ").to_string(),
            None => "lmp, mcc, mcl".to_string(),
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
            price: match row.get_ref_unwrap(3) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
        })
    })?;
    let offers: Vec<Row> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

/// Get hourly prices between a [start, end] date for a list of ptids, only one component.
/// Return only one row of data as a String in this format:  
///   {"2025-07-01": {"4000":[...],"4001":[...]}, "2025-07-02":{...} ...}
pub fn get_hourly_prices_compact(
    conn: &Connection,
    start: Date,
    end: Date,
    ptids: Option<Vec<u32>>,
    component: LmpComponent,
) -> Result<String> {
    conn.execute("LOAD icu;", [])?;
    let query = format!(
        r#"        
SELECT '{{' || string_agg('"' || date || '":' || map_json, ',') || '}}' AS out
FROM (
    WITH per_ptid AS (
    SELECT
        strftime(hour_beginning, '%Y-%m-%d') AS date,
        ptid,
        list({} ORDER BY hour_beginning)::DECIMAL(9,4)[] AS prices
    FROM da_lmp
    WHERE hour_beginning >= '{}'
        AND hour_beginning <  '{}'{}
    GROUP BY date, ptid
    )
    SELECT 
        date,
        '{{' || string_agg('"' || ptid || '":' || to_json(prices), ',') || '}}' AS map_json
    FROM per_ptid
    GROUP BY date
    ORDER BY date
);
    "#,
        component,
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
    let out = stmt.query_row([], |row| Ok(row.get::<usize, String>(0).unwrap()));

    Ok(out.unwrap())
}

pub fn get_daily_prices(
    conn: &Connection,
    term: Term,
    ptids: Option<Vec<i32>>,
    component: LmpComponent,
    buckets: Vec<Bucket>,
    statistic: String,
    buckets_db_path: &str,
) -> Result<Vec<RowD>> {
    conn.execute_batch(
        format!(
            r"LOAD icu;
              ATTACH '{}' AS buckets;",
            buckets_db_path
        )
        .as_str(),
    )?;

    let mut prices: Vec<RowD> = Vec::new();
    for bucket in buckets {
        let mut ps = get_daily_prices_bucket(
            conn,
            term.start,
            term.end,
            ptids.clone(),
            bucket,
            component,
            statistic.clone(),
        )?;
        prices.append(&mut ps);
    }

    Ok(prices)
}

fn get_daily_prices_bucket(
    conn: &Connection,
    start: Date,
    end: Date,
    ptids: Option<Vec<i32>>,
    bucket: Bucket,
    component: LmpComponent,
    statistic: String,
) -> Result<Vec<RowD>> {
    let query = format!(
        r#"
SELECT
    ptid,
    hour_beginning::DATE AS day,
    {}({})::DECIMAL(9,4) AS price,
FROM da_lmp
JOIN buckets.buckets 
    USING (hour_beginning)
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}
GROUP BY ptid, day, buckets.buckets."{}"
HAVING buckets.buckets."{}" = TRUE
ORDER BY ptid, day;
        "#,
        statistic,
        component.to_string().to_lowercase(),
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
        bucket.name().to_lowercase(),
        bucket.name().to_lowercase(),
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let prices_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(1).unwrap();
        Ok(RowD {
            date: Date::ZERO.checked_add(n.days()).unwrap(),
            ptid: row.get(0).unwrap(),
            bucket,
            value: match row.get_ref_unwrap(2) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
        })
    })?;
    let prices: Vec<RowD> = prices_iter.map(|e| e.unwrap()).collect();

    Ok(prices)
}

// for daily data
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RowD {
    date: Date,
    ptid: i32,
    bucket: Bucket,
    #[serde(with = "rust_decimal::serde::float")]
    value: Decimal,
}

pub fn get_monthly_prices(
    conn: &Connection,
    start: Month,
    end: Month,
    ptids: Option<Vec<i32>>,
    component: LmpComponent,
    buckets: Vec<Bucket>,
    statistic: String,
    buckets_db_path: &str,
) -> Result<Vec<RowM>> {
    conn.execute_batch(
        format!(
            r"LOAD icu;
              ATTACH '{}' AS buckets;",
            buckets_db_path
        )
        .as_str(),
    )?;

    let mut prices: Vec<RowM> = Vec::new();
    for bucket in buckets {
        let mut ps = get_monthly_prices_bucket(
            conn,
            start,
            end,
            ptids.clone(),
            component,
            bucket,
            statistic.clone(),
        )?;
        prices.append(&mut ps);
    }

    Ok(prices)
}

fn get_monthly_prices_bucket(
    conn: &Connection,
    start: Month,
    end: Month,
    ptids: Option<Vec<i32>>,
    component: LmpComponent,
    bucket: Bucket,
    statistic: String,
) -> Result<Vec<RowM>> {
    let query = format!(
        r#"
SELECT
    ptid,
    date_trunc('month', hour_beginning) AS month_beginning,
    {}({})::DECIMAL(9,4) AS price,
FROM da_lmp
JOIN buckets.buckets 
    USING (hour_beginning)
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}
GROUP BY ptid, month_beginning, buckets.buckets."{}"
HAVING buckets.buckets."{}" = TRUE
ORDER BY ptid, month_beginning;
        "#,
        statistic,
        component.to_string().to_lowercase(),
        start
            .start()
            .in_tz("America/New_York")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        end.end()
            .in_tz("America/New_York")
            .unwrap()
            .checked_add(1.day())
            .ok()
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        match ptids {
            Some(ids) => format!("\nAND ptid in ({}) ", ids.iter().join(", ")),
            None => "".to_string(),
        },
        bucket.name().to_lowercase(),
        bucket.name().to_lowercase(),
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let prices_iter = stmt.query_map([], |row| {
        let micro: i64 = row.get(1).unwrap();
        let ts = Timestamp::from_second(micro / 1_000_000).unwrap();
        let month_tz = MonthTz::containing(ts.in_tz("America/New_York").unwrap());
        Ok(RowM {
            month: month(month_tz.start_date().year(), month_tz.start_date().month()),
            ptid: row.get(0).unwrap(),
            bucket,
            value: match row.get_ref_unwrap(2) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
        })
    })?;
    let prices: Vec<RowM> = prices_iter.map(|e| e.unwrap()).collect();

    Ok(prices)
}

// for monthly data
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RowM {
    month: Month,
    ptid: i32,
    bucket: Bucket,
    #[serde(with = "rust_decimal::serde::float")]
    value: Decimal,
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error, path::Path, vec};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use rust_decimal_macros::dec;

    use crate::{api::isone::lmp::*, db::prod_db::ProdDb, interval::month::month};

    #[test]
    fn test_hourly_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_dalmp().duckdb_path, config).unwrap();
        let data = get_hourly_prices(
            &conn,
            date(2025, 7, 1),
            date(2025, 7, 14),
            Market::DA,
            Some(vec![4000]),
            Some(vec![LmpComponent::Lmp]),
        )
        .unwrap();
        assert_eq!(data.len(), 24 * 14);
        assert_eq!(
            data[0],
            Row {
                hour_beginning: "2025-07-01 00:00[America/New_York]".parse()?,
                ptid: 4000,
                component: LmpComponent::Lmp,
                price: dec!(49.65),
            }
        );

        Ok(())
    }

    #[test]
    fn test_hourly_prices_compact() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_dalmp().duckdb_path, config).unwrap();
        let data = get_hourly_prices_compact(
            &conn,
            date(2025, 7, 1),
            date(2025, 7, 14),
            Some(vec![4000, 4001]),
            LmpComponent::Mcc,
        )
        .unwrap();
        assert!(data.contains("2025-07-01"));
        Ok(())
    }

    #[test]
    fn test_daily_prices() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_dalmp().duckdb_path, config).unwrap();
        let data = get_daily_prices(
            &conn,
            Term {
                start: date(2025, 7, 1),
                end: date(2025, 7, 14),
            },
            Some(vec![4000]),
            LmpComponent::Lmp,
            vec![
                Bucket::Atc,
                Bucket::B5x16,
                Bucket::B2x16H,
                Bucket::B7x8,
                Bucket::Offpeak,
            ],
            "mean".into(),
            ProdDb::buckets().duckdb_path.as_str(),
        )
        .unwrap();
        // println!("{:?}", data);
        assert_eq!(
            data[0],
            RowD {
                date: date(2025, 7, 1),
                ptid: 4000,
                bucket: Bucket::Atc,
                value: dec!(59.6663),
            }
        );

        Ok(())
    }

    #[test]
    fn test_daily_prices_5x16() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_dalmp().duckdb_path, config).unwrap();
        let data = get_daily_prices(
            &conn,
            Term {
                start: date(2025, 7, 1),
                end: date(2025, 7, 14),
            },
            Some(vec![4000]),
            LmpComponent::Lmp,
            vec![Bucket::B5x16],
            "mean".into(),
            ProdDb::buckets().duckdb_path.as_str(),
        )
        .unwrap();
        assert_eq!(data.len(), 9); // only 9 onpeak days
        assert_eq!(
            data[0],
            RowD {
                date: date(2025, 7, 1),
                ptid: 4000,
                bucket: Bucket::B5x16,
                value: dec!(67.4944),
            }
        );
        Ok(())
    }

    #[test]
    fn test_daily_prices_2x16h() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_dalmp().duckdb_path, config).unwrap();
        let data = get_daily_prices(
            &conn,
            Term {
                start: date(2025, 7, 1),
                end: date(2025, 7, 14),
            },
            Some(vec![4000]),
            LmpComponent::Lmp,
            vec![Bucket::B2x16H],
            "mean".into(),
            ProdDb::buckets().duckdb_path.as_str(),
        )
        .unwrap();
        assert_eq!(data.len(), 5); // only 2x16H days
        assert_eq!(
            data[0],
            RowD {
                date: date(2025, 7, 4),
                ptid: 4000,
                bucket: Bucket::B2x16H,
                value: dec!(39.1888),
            }
        );
        Ok(())
    }

    #[test]
    fn test_monthly_prices() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_dalmp().duckdb_path, config).unwrap();
        let data = get_monthly_prices(
            &conn,
            month(2025, 1),
            month(2025, 7),
            Some(vec![4000]),
            LmpComponent::Lmp,
            vec![
                Bucket::Atc,
                Bucket::B5x16,
                Bucket::B2x16H,
                Bucket::B7x8,
                Bucket::Offpeak,
            ],
            "mean".into(),
            ProdDb::buckets().duckdb_path.as_str(),
        )
        .unwrap();
        assert_eq!(
            data[0],
            RowM {
                month: month(2025, 1),
                ptid: 4000,
                bucket: Bucket::Atc,
                value: dec!(133.5564),
            }
        );

        Ok(())
    }

    #[test]
    fn api_hourly_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/prices/da/hourly/start/2025-01-01/end/2025-01-05?ptids=4000,4001",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 24);
        Ok(())
    }

    #[test]
    fn api_daily_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/prices/da/daily/start/2025-01-01/end/2025-01-05?ptids=4000,4001&buckets=5x16,2x16H",
            env::var("RUST_SERVER").unwrap(),
        );
        println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<RowD> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 5);
        Ok(())
    }
}
