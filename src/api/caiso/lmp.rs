use std::time::Duration;

use actix_web::{get, web, HttpResponse, Responder};

use crate::{
    api::isone::_api_isone_core::{deserialize_zoned_assume_ny, serialize_zoned_as_offset, Market},
    bucket::{Bucket, BucketLike},
    db::{
        caiso::{dalmp_archive::*, rtlmp_archive::CaisoRtLmpArchive},
        calendar::buckets::BucketsArchive,
    },
    interval::{
        month::{month, Month},
        month_tz::MonthTz,
        term::Term,
    },
    utils::lib_duckdb::open_with_retry,
};
use duckdb::{types::ValueRef, AccessMode, Connection, Result};
use itertools::Itertools;
use jiff::{
    civil::Date,
    Timestamp, ToSpan, Zoned,
};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

use crate::db::nyiso::dalmp::LmpComponent;

#[get("/caiso/prices/{market}/hourly/start/{start}/end/{end}")]
async fn api_hourly_prices(
    path: web::Path<(Market, Date, Date)>,
    query: web::Query<LmpQuery>,
    db: web::Data<(CaisoDaLmpArchive, CaisoRtLmpArchive, BucketsArchive)>,
) -> impl Responder {
    let market = path.0;
    let start_date = path.1;
    let end_date = path.2;

    let conn = match market {
        Market::DA => open_with_retry(
            &db.0.duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        ),
        Market::RT => open_with_retry(
            &db.1.duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        ),
    };
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database: {}",
            conn.err().unwrap()
        ));
    }

    let node_ids: Option<Vec<String>> = query
        .node_ids
        .as_ref()
        .map(|e| e.split(',').map(|s| s.to_string()).collect());

    let components: Vec<LmpComponent> = query
        .components
        .as_ref()
        .map(|ids| {
            ids.split(',')
                .map(|e| e.parse::<LmpComponent>().unwrap())
                .collect()
        })
        .unwrap_or(vec![
            LmpComponent::Lmp,
            LmpComponent::Mcc,
            LmpComponent::Mcl,
        ]);

    let mut filter = QueryFilterBuilder::new()
        .hour_beginning_gte(
            start_date
                .at(0, 0, 0, 0)
                .in_tz("America/Los_Angeles")
                .unwrap(),
        )
        .hour_beginning_lt(
            end_date
                .tomorrow()
                .unwrap()
                .at(0, 0, 0, 0)
                .in_tz("America/Los_Angeles")
                .unwrap(),
        );
    if let Some(node_ids) = node_ids {
        filter = filter.node_id_in(node_ids);
    }
    let filter = filter.build();

    let conn = conn.unwrap();
    conn.execute_batch("LOAD ICU;SET TimeZone = 'America/Los_Angeles';")
        .unwrap();
    let prices = get_data(&conn, &filter).unwrap();
    if prices.len() > 100_000 {
        return HttpResponse::BadRequest()
            .body("Query returned more than 100,000 rows. Please narrow your query.");
    }
    HttpResponse::Ok().json(
        prices
            .iter()
            .flat_map(|e| {
                let mut out = Vec::new();
                for component in &components {
                    out.push(RowH {
                        hour_beginning: e.hour_beginning.clone(),
                        name: e.node_id.clone(),
                        component: *component,
                        price: match *component {
                            LmpComponent::Lmp => e.lmp,
                            LmpComponent::Mcc => e.mcc,
                            LmpComponent::Mcl => e.mcl,
                        },
                    });
                }
                out
            })
            .collect::<Vec<RowH>>(),
    )
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RowH {
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    hour_beginning: Zoned,
    name: String,
    component: LmpComponent,
    #[serde(with = "rust_decimal::serde::float")]
    price: Decimal,
}

#[get("/caiso/prices/{market}/daily/start/{start}/end/{end}")]
async fn api_daily_prices(
    path: web::Path<(Market, Date, Date)>,
    query: web::Query<LmpQuery2>,
    db: web::Data<(CaisoDaLmpArchive, CaisoRtLmpArchive, BucketsArchive)>,
) -> impl Responder {
    let market = path.0;
    let start_date = path.1;
    let end_date = path.2;

    let conn = match market {
        Market::DA => open_with_retry(
            &db.0.duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        ),
        Market::RT => open_with_retry(
            &db.1.duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        ),
    };
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database: {}",
            conn.err().unwrap()
        ));
    }

    let node_ids: Option<Vec<String>> = query
        .node_ids
        .as_ref()
        .map(|e| e.split(',').map(|s| s.to_string()).collect());

    let buckets: Vec<Bucket> = match &query.buckets {
        Some(ids) => {
            let mut out = Vec::new();
            for e in ids.split(',') {
                match e.parse::<Bucket>() {
                    Ok(b) => out.push(b),
                    Err(_) => {
                        return HttpResponse::BadRequest()
                            .body(format!("Failed to parse bucket: {}", e));
                    }
                }
            }
            out
        }
        None => vec![Bucket::Atc],
    };

    let component = query.component.unwrap_or(LmpComponent::Lmp);
    let statistic = query.statistic.clone().unwrap_or("mean".into());

    let conn = conn.unwrap();
    conn.execute_batch(
        format!(
            r"LOAD icu;SET TimeZone = 'America/Los_Angeles';
              ATTACH '{}' AS buckets;",
            db.2.duckdb_path
        )
        .as_str(),
    )
    .unwrap();
    let prices = get_daily_prices(
        &conn,
        Term {
            start: start_date,
            end: end_date,
        },
        node_ids,
        component,
        buckets,
        statistic,
    )
    .unwrap();
    if prices.len() > 100_000 {
        return HttpResponse::BadRequest()
            .body("Query returned more than 100,000 rows. Please narrow your query.");
    }
    HttpResponse::Ok().json(prices)
}

#[get("/caiso/prices/{market}/monthly/start/{start}/end/{end}")]
async fn api_monthly_prices(
    path: web::Path<(Market, Month, Month)>,
    query: web::Query<LmpQuery2>,
    db: web::Data<(CaisoDaLmpArchive, CaisoRtLmpArchive, BucketsArchive)>,
) -> impl Responder {
    let market = path.0;
    let start_month = path.1;
    let end_month = path.2;

    let conn = match market {
        Market::DA => open_with_retry(
            &db.0.duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        ),
        Market::RT => open_with_retry(
            &db.1.duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        ),
    };
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database: {}",
            conn.err().unwrap()
        ));
    }

    let node_ids: Option<Vec<String>> = query
        .node_ids
        .as_ref()
        .map(|e| e.split(',').map(|s| s.to_string()).collect());

    let buckets: Vec<Bucket> = match &query.buckets {
        Some(ids) => {
            let mut out = Vec::new();
            for e in ids.split(',') {
                match e.parse::<Bucket>() {
                    Ok(b) => out.push(b),
                    Err(_) => {
                        return HttpResponse::BadRequest()
                            .body(format!("Failed to parse bucket: {}", e));
                    }
                }
            }
            out
        }
        None => vec![Bucket::Atc],
    };

    let component = query.component.unwrap_or(LmpComponent::Lmp);

    let statistic = query.statistic.clone().unwrap_or("avg".into());

    let conn = conn.unwrap();
    conn.execute_batch(
        format!(
            r"LOAD icu;SET TimeZone = 'America/Los_Angeles';
              ATTACH '{}' AS buckets;",
            db.2.duckdb_path
        )
        .as_str(),
    )
    .unwrap();
    let prices = get_monthly_prices(
        &conn,
        (start_month, end_month),
        node_ids,
        component,
        buckets,
        statistic,
    )
    .unwrap();
    HttpResponse::Ok().json(prices)
}

#[get("/caiso/prices/{market}")]
async fn api_term_prices(
    path: web::Path<Market>,
    query: web::Query<LmpQuery3>,
    db: web::Data<(CaisoDaLmpArchive, CaisoRtLmpArchive, BucketsArchive)>,
) -> impl Responder {
    let market = path.into_inner();

    let conn = match market {
        Market::DA => open_with_retry(
            &db.0.duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        ),
        Market::RT => open_with_retry(
            &db.1.duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        ),
    };
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database: {}",
            conn.err().unwrap()
        ));
    }

    let terms: Option<Vec<Term>> = query.terms.as_ref().map(|ids| {
        ids.split(';')
            .map(|e| e.trim().parse::<Term>().unwrap())
            .collect()
    });
    if terms.is_none() {
        return HttpResponse::BadRequest().body("terms parameter is required");
    }

    let node_ids: Option<Vec<String>> = query
        .node_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.trim().to_string()).collect());

    let buckets: Vec<Bucket> = match &query.buckets {
        Some(ids) => {
            let mut out = Vec::new();
            for e in ids.split(',') {
                match e.parse::<Bucket>() {
                    Ok(b) => out.push(b),
                    Err(_) => {
                        return HttpResponse::BadRequest()
                            .body(format!("Failed to parse bucket: {}", e));
                    }
                }
            }
            out
        }
        None => vec![Bucket::Atc],
    };

    let component = query.component.unwrap_or(LmpComponent::Lmp);

    let statistic = query.statistic.clone().unwrap_or("avg".into());

    let mut conn = conn.unwrap();
    conn.execute_batch(
        format!(
            r"LOAD icu;SET TimeZone = 'America/Los_Angeles';
              ATTACH '{}' AS buckets;",
            db.2.duckdb_path
        )
        .as_str(),
    )
    .unwrap();
    let prices = get_term_prices(
        &mut conn,
        &terms.unwrap(),
        node_ids,
        component,
        buckets,
        statistic,
    )
    .unwrap();
    HttpResponse::Ok().json(prices)
}

#[derive(Debug, Deserialize)]
struct LmpQuery {
    /// One or more node ids, separated by commas.
    /// If not specified, return all zonal ids.  
    node_ids: Option<String>,

    /// One or more LMP components, separated by commas.
    /// Valid values are: lmp, mcc, mlc.
    /// If not specified, return all of three.
    components: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LmpQuery2 {
    /// One or more node ids, separated by commas.
    /// If `None`, return all node ids.  
    node_ids: Option<String>,

    /// One or more bucket names, separated by commas.
    /// Valid values are: 5x16, 2x16H, 7x8, atc, offpeak, etc.
    buckets: Option<String>,

    /// Default value: lmp
    component: Option<LmpComponent>,

    /// Statistic: avg, min, max, median, etc.  Default: avg
    statistic: Option<String>,
}

#[derive(Debug, Deserialize)]
struct LmpQuery3 {
    /// One or more ptids, separated by commas.
    /// If not specified, return all ptids.  Use carefully
    /// because it's a lot of data...
    node_ids: Option<String>,

    /// One or more bucket names, separated by commas.
    /// Valid values are: 5x16, 2x16H, 7x8, atc, offpeak, etc.
    buckets: Option<String>,

    /// One or more terms, separated by semicolons.
    /// Valid values are: Cal24;3Sep22-15Oc23;Jul24-Aug25, etc.
    terms: Option<String>,

    /// Default value: lmp
    component: Option<LmpComponent>,

    /// Statistic: avg, min, max, median, etc.  Default: avg
    statistic: Option<String>,
}

pub fn get_daily_prices(
    conn: &Connection,
    term: Term,
    node_ids: Option<Vec<String>>,
    component: LmpComponent,
    buckets: Vec<Bucket>,
    statistic: String,
) -> Result<Vec<RowD>> {
    let mut prices: Vec<RowD> = Vec::new();
    for bucket in buckets {
        let mut ps = get_daily_prices_bucket(
            conn,
            term.start,
            term.end,
            node_ids.clone(),
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
    node_ids: Option<Vec<String>>,
    bucket: Bucket,
    component: LmpComponent,
    statistic: String,
) -> Result<Vec<RowD>> {
    let query = format!(
        r#"
SELECT
    node_id,
    hour_beginning::DATE AS day,
    {}({})::DECIMAL(18,5) AS price,
FROM lmp
JOIN buckets.buckets 
    USING (hour_beginning)
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}
GROUP BY node_id, day, buckets.buckets."{}"
HAVING buckets.buckets."{}" = TRUE
ORDER BY node_id, day;
        "#,
        statistic,
        component.to_string().to_lowercase(),
        start
            .in_tz("America/Los_Angeles")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        end.in_tz("America/Los_Angeles")
            .unwrap()
            .checked_add(1.day())
            .ok()
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        match node_ids {
            Some(ids) => format!("\nAND node_id in ('{}') ", ids.iter().join("','")),
            None => "".to_string(),
        },
        bucket.name().to_lowercase(),
        bucket.name().to_lowercase(),
    );
    let mut stmt = conn.prepare(&query).unwrap();
    let prices_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(1).unwrap();
        Ok(RowD {
            date: Date::ZERO.checked_add(n.days()).unwrap(),
            node_id: row.get(0).unwrap(),
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
    node_id: String,
    bucket: Bucket,
    #[serde(with = "rust_decimal::serde::float")]
    value: Decimal,
}

pub fn get_monthly_prices(
    conn: &Connection,
    start_end: (Month, Month),
    node_ids: Option<Vec<String>>,
    component: LmpComponent,
    buckets: Vec<Bucket>,
    statistic: String,
) -> Result<Vec<RowM>> {
    let (start, end) = start_end;
    let mut prices: Vec<RowM> = Vec::new();
    for bucket in buckets {
        let mut ps = get_monthly_prices_bucket(
            conn,
            start,
            end,
            node_ids.clone(),
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
    node_ids: Option<Vec<String>>,
    component: LmpComponent,
    bucket: Bucket,
    statistic: String,
) -> Result<Vec<RowM>> {
    let query = format!(
        r#"
SELECT
    node_id,
    date_trunc('month', hour_beginning) AS month_beginning,
    {}({})::DECIMAL(18,5) AS price,
FROM lmp
JOIN buckets.buckets 
    USING (hour_beginning)
WHERE hour_beginning >= '{}'
AND hour_beginning < '{}'{}
GROUP BY node_id, month_beginning, buckets.buckets."{}"
HAVING buckets.buckets."{}" = TRUE
ORDER BY node_id, month_beginning;
        "#,
        statistic,
        component.to_string().to_lowercase(),
        start
            .start()
            .in_tz("America/Los_Angeles")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        end.end()
            .in_tz("America/Los_Angeles")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        match node_ids {
            Some(ids) => format!("\nAND node_id in ('{}') ", ids.iter().join("','")),
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
            node_id: row.get(0).unwrap(),
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

/// Get prices for custom terms.
pub fn get_term_prices(
    conn: &mut Connection,
    terms: &Vec<Term>,
    node_ids: Option<Vec<String>>,
    component: LmpComponent,
    buckets: Vec<Bucket>,
    statistic: String,
) -> Result<Vec<RowT>> {
    conn.execute_batch(
        r#"
CREATE TEMPORARY TABLE terms (
    term VARCHAR NOT NULL,
    term_start TIMESTAMPTZ NOT NULL,
    term_end TIMESTAMPTZ NOT NULL
);"#
        .to_string()
        .as_str(),
    )?;

    let tx = conn.transaction()?;
    let mut stmt = tx.prepare("INSERT INTO terms (term, term_start, term_end) VALUES (?, ?, ?)")?;
    for term in terms {
        stmt.execute([
            format!("{}", term),
            term.start
                .in_tz("America/Los_Angeles")
                .unwrap()
                .strftime("%Y-%m-%d %H:%M:%S.000%:z")
                .to_string(),
            term.end
                .in_tz("America/Los_Angeles")
                .unwrap()
                .checked_add(1.day())
                .ok()
                .unwrap()
                .strftime("%Y-%m-%d %H:%M:%S.000%:z")
                .to_string(),
        ])?;
    }
    tx.commit()?;

    let mut prices: Vec<RowT> = Vec::new();
    for bucket in buckets {
        let mut ps =
            get_term_prices_bucket(conn, node_ids.clone(), component, bucket, statistic.clone())?;
        prices.append(&mut ps);
    }

    Ok(prices)
}

fn get_term_prices_bucket(
    conn: &Connection,
    node_ids: Option<Vec<String>>,
    component: LmpComponent,
    bucket: Bucket,
    statistic: String,
) -> Result<Vec<RowT>> {
    let query = format!(
        r#"
SELECT
    t.term,
    d.node_id,
    {}(d.{})::DECIMAL(18,5) AS price,
FROM da_lmp d
JOIN terms t
    ON d.hour_beginning >= t.term_start
    AND d.hour_beginning < t.term_end
JOIN buckets.buckets b
    ON d.hour_beginning = b.hour_beginning
WHERE b."{}" = TRUE{}    
GROUP BY t.term, d.node_id
ORDER BY t.term, d.node_id;
        "#,
        statistic,
        component.to_string().to_lowercase(),
        bucket.name().to_lowercase(),
        match node_ids {
            Some(ids) => format!("\nAND node_id in ('{}') ", ids.iter().join("', '")),
            None => "".to_string(),
        },
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let prices_iter = stmt.query_map([], |row| {
        Ok(RowT {
            term: row.get(0).unwrap(),
            ptid: row.get(1).unwrap(),
            bucket,
            value: match row.get_ref_unwrap(2) {
                ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            },
        })
    })?;
    let prices: Vec<RowT> = prices_iter.map(|e| e.unwrap()).collect();

    Ok(prices)
}

// for monthly data
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RowM {
    pub month: Month,
    pub node_id: String,
    pub bucket: Bucket,
    #[serde(with = "rust_decimal::serde::float")]
    pub value: Decimal,
}

// for term data
#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct RowT {
    pub term: String,
    pub ptid: i32,
    pub bucket: Bucket,
    #[serde(with = "rust_decimal::serde::float")]
    pub value: Decimal,
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error, path::Path, vec};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;
    use rust_decimal_macros::dec;

    use super::*;
    use crate::{
        api::caiso::lmp::RowH,
        bucket::Bucket,
        db::{nyiso::dalmp::LmpComponent, prod_db::ProdDb},
        interval::{month::month, term::Term},
        utils::lib_duckdb::open_with_retry,
    };

    fn get_connection() -> Result<Connection> {
        let conn = open_with_retry(
            &ProdDb::caiso_dalmp().duckdb_path,
            8,
            std::time::Duration::from_millis(25),
            AccessMode::ReadOnly,
        )
        .unwrap();
        conn.execute_batch(
            format!(
                r"LOAD icu;SET TimeZone = 'America/Los_Angeles';
              ATTACH '{}' AS buckets;",
                ProdDb::buckets().duckdb_path
            )
            .as_str(),
        )
        .unwrap();
        Ok(conn)
    }

    #[test]
    fn test_daily_prices() -> Result<(), Box<dyn Error>> {
        let conn = get_connection().unwrap();
        let data = get_daily_prices(
            &conn,
            Term {
                start: date(2025, 12, 1),
                end: date(2025, 12, 8),
            },
            Some(vec!["TH_NP15_GEN-APND".into(), "TH_SP15_GEN-APND".into()]),
            LmpComponent::Lmp,
            vec![
                Bucket::Caiso6x16,
                Bucket::Caiso1x16H,
                Bucket::Caiso7x8,
                Bucket::CaisoOffpeak,
            ],
            "mean".into(),
        )
        .unwrap();
        // println!("{:?}", data);
        let x0 = data
            .iter()
            .find(|e| {
                e.date == date(2025, 12, 1)
                    && e.node_id == "TH_NP15_GEN-APND"
                    && e.bucket == Bucket::Caiso6x16
            })
            .unwrap();
        assert_eq!(
            *x0,
            RowD {
                date: date(2025, 12, 1),
                node_id: "TH_NP15_GEN-APND".into(),
                bucket: Bucket::Caiso6x16,
                value: dec!(60.29863),
            }
        );

        Ok(())
    }

    #[test]
    fn test_monthly_prices() -> Result<(), Box<dyn Error>> {
        let conn = get_connection().unwrap();

        let data = get_monthly_prices(
            &conn,
            (month(2025, 12), month(2025, 12)),
            Some(vec!["TH_NP15_GEN-APND".into(), "TH_SP15_GEN-APND".into()]),
            LmpComponent::Lmp,
            vec![Bucket::Caiso6x16, Bucket::Caiso1x16H, Bucket::Caiso7x8],
            "mean".into(),
        )
        .unwrap();
        println!("{:?}", data);
        let x0 = data
            .iter()
            .find(|e| {
                e.month == month(2025, 12)
                    && e.node_id == "TH_NP15_GEN-APND"
                    && e.bucket == Bucket::Caiso6x16
            })
            .unwrap();
        assert_eq!(
            *x0,
            RowM {
                month: month(2025, 12),
                node_id: "TH_NP15_GEN-APND".into(),
                bucket: Bucket::Caiso6x16,
                value: dec!(45.41769),
            }
        );

        Ok(())
    }

    #[test]
    fn test_term_prices() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let mut conn =
            Connection::open_with_flags(ProdDb::isone_dalmp().duckdb_path, config).unwrap();
        let terms: Vec<Term> = vec!["Cal24", "Jul24", "Jul24-Aug24"]
            .into_iter()
            .map(|s| s.parse::<Term>().unwrap())
            .collect();

        let data = get_term_prices(
            &mut conn,
            &terms,
            Some(vec!["TH_NP15_GEN-APND".into(), "TH_SP15_GEN-APND".into()]),
            LmpComponent::Lmp,
            vec![Bucket::Caiso6x16, Bucket::CaisoOffpeak],
            "mean".into(),
        )
        .unwrap();
        // println!("{:?}", data);
        assert_eq!(
            data[0],
            RowT {
                term: "Cal24".into(),
                ptid: 4000,
                bucket: Bucket::B5x16,
                value: dec!(46.6208),
            }
        );

        Ok(())
    }

    #[test]
    fn api_hourly_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/caiso/prices/da/hourly/start/2025-12-01/end/2025-12-02?node_ids=TH_NP16_GEN-APND,TH_SP15_GEN-APND&components=lmp,mcl",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<RowH> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 720); // 2 locations x 3 components x 5 days x 24 hours = 720
        Ok(())
    }

    #[test]
    fn api_daily_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/caiso/prices/da/daily/start/2025-12-01/end/2025-12-10?node_ids=TH_NP16_GEN-APND,TH_SP15_GEN-APND&buckets=Caiso6x16,Caiso1x16H,Caiso7x8",
            env::var("RUST_SERVER").unwrap(),
        );
        println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<RowD> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 10); // 2 locations x 2 buckets x 5 days = 10
        Ok(())
    }

    #[test]
    fn api_monthly_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/prices/da/monthly/start/2024-01/end/2024-12?ptids=4000,4001&buckets=5x16,offpeak",
            env::var("RUST_SERVER").unwrap(),
        );
        // println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<RowM> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 48); // 2 locations x 2 buckets x 12 months = 48
        Ok(())
    }

    #[test]
    fn api_term_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/prices/da?ptids=4000&buckets=5x16,offpeak&terms=Cal24;Jul24;Jul24-Aug25",
            env::var("RUST_SERVER").unwrap(),
        );
        // println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<RowT> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 6); // 1 location x 2 buckets x 3 terms = 6
        assert_eq!(
            vs[0],
            RowT {
                term: "Cal24".into(),
                ptid: 4000,
                bucket: Bucket::B5x16,
                value: dec!(46.6208),
            }
        );

        Ok(())
    }
}
