use actix_web::{get, web, HttpResponse, Responder};

use crate::{
    bucket::{Bucket, BucketLike},
    db::{
        calendar::buckets::BucketsArchive,
        isone::{dalmp_archive::IsoneDaLmpArchive, ftr_prices_archive::IsoneFtrPricesArchive},
    },
    interval::month::Month,
    utils::lib_duckdb::{open_with_retry, WithRetry},
};
use duckdb::{types::ValueRef, AccessMode, Result};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};

#[get("/isone/ftr/settle_price/monthly/start/{start}/end/{end}")]
async fn api_monthly_settle_prices(
    path: web::Path<(Month, Month)>,
    query: web::Query<LmpQuery>,
    db: web::Data<(IsoneDaLmpArchive, BucketsArchive, IsoneFtrPricesArchive)>,
) -> impl Responder {
    let start_month = path.0;
    let end_month = path.1;

    let source_ptids: Vec<i32> = query
        .source_ptids
        .as_ref()
        .map(|ids| {
            ids.split(',')
                .map(|e| e.trim().parse::<i32>().unwrap())
                .collect()
        })
        .unwrap_or(vec![4000]);

    let sink_ptids: Vec<i32> = query
        .sink_ptids
        .as_ref()
        .map(|ids| {
            ids.split(',')
                .map(|e| e.trim().parse::<i32>().unwrap())
                .collect()
        })
        .unwrap_or(vec![4001]);

    let buckets: Vec<Bucket> = query
        .buckets
        .as_ref()
        .map(|ids| {
            ids.split(',')
                .map(|e| e.parse::<Bucket>().unwrap())
                .collect()
        })
        .unwrap_or(vec![Bucket::B5x16]);

    // Create paths from source and sink ptids by zipping them
    let mut paths: Vec<Path0> = vec![];
    for (source, sink) in source_ptids.iter().zip(sink_ptids.iter()) {
        paths.push(Path0 {
            source: *source,
            sink: *sink,
        });
    }

    let prices = get_monthly_settle_prices(
        db.get_ref().to_owned(),
        (start_month, end_month),
        paths,
        buckets,
    )
    .unwrap();
    HttpResponse::Ok().json(prices)
}

pub struct Path0 {
    source: i32,
    sink: i32,
}

#[derive(Debug, Deserialize)]
struct LmpQuery {
    /// One or more ptids, separated by commas.  Default is 4000.
    source_ptids: Option<String>,

    /// One or more ptids, separated by commas.  Default is 4001.
    sink_ptids: Option<String>,

    /// One or more bucket names.  Valid values are: peak, offpeak.
    buckets: Option<String>,
}

/// Get monthly FTR settle prices for many paths.
pub fn get_monthly_settle_prices(
    dbs: (IsoneDaLmpArchive, BucketsArchive, IsoneFtrPricesArchive),
    from_to: (Month, Month),
    paths: Vec<Path0>,
    buckets: Vec<Bucket>,
) -> Result<Vec<Row>> {
    let mut conn = open_with_retry(
        &dbs.2.duckdb_path,
        8,
        std::time::Duration::from_millis(25),
        AccessMode::ReadOnly,
    )
    .unwrap();
    conn.execute_batch_with_retry(
        format!(
            r#"
LOAD icu;
ATTACH '{}' AS dalmp;
ATTACH '{}' AS buckets;
CREATE TEMPORARY TABLE paths (
    source_ptid INT NOT NULL,
    sink_ptid INT NOT NULL
);"#,
            dbs.0.duckdb_path, dbs.1.duckdb_path
        )
        .as_str(),
        8,
        std::time::Duration::from_millis(25),
    )?;

    let tx = conn.transaction()?;
    let mut stmt = tx.prepare("INSERT INTO paths (source_ptid, sink_ptid) VALUES (?, ?)")?;
    for p in paths {
        stmt.execute([p.source, p.sink])?;
    }
    tx.commit()?;

    let mut prices: Vec<Row> = Vec::new();

    for bucket in buckets {
        let query = format!(
            r#"
SELECT
  p.source_ptid,
  p.sink_ptid,
  strftime(lmp_src.hour_beginning, '%Y-%m') AS month,
  AVG(lmp_src.mcc)::DECIMAL(9,4) AS source_price,
  AVG(lmp_sink.mcc)::DECIMAL(9,4) AS sink_price,
  AVG(lmp_sink.mcc)::DECIMAL(9,4) - AVG(lmp_src.mcc)::DECIMAL(9,4) AS settle_price
FROM paths p
JOIN dalmp.da_lmp AS lmp_src
  ON lmp_src.ptid = p.source_ptid
JOIN dalmp.da_lmp AS lmp_sink
  ON lmp_sink.ptid = p.sink_ptid
  AND lmp_sink.hour_beginning = lmp_src.hour_beginning
JOIN buckets.buckets
  ON lmp_src.hour_beginning = buckets.buckets.hour_beginning
WHERE buckets.buckets."{}" = true
  AND lmp_src.hour_beginning >= '{}'
  AND lmp_src.hour_beginning < '{}'
GROUP BY p.source_ptid, p.sink_ptid, month
ORDER BY p.source_ptid, p.sink_ptid, month;
        "#,
            bucket.name().to_lowercase(),
            from_to
                .0
                .start()
                .in_tz("America/New_York")
                .unwrap()
                .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
            from_to
                .1
                .end()
                .in_tz("America/New_York")
                .unwrap()
                .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        );
        // println!("{}", query);
        let mut stmt = conn.prepare(&query).unwrap();
        let prices_iter = stmt.query_map([], |row| {
            Ok(Row {
                source_ptid: row.get(0).unwrap(),
                sink_ptid: row.get(1).unwrap(),
                month: row.get(2).unwrap(),
                bucket: bucket.to_string(),
                source_price: match row.get_ref_unwrap(3) {
                    ValueRef::Decimal(v) => v,
                    _ => Decimal::MIN,
                },
                sink_price: match row.get_ref_unwrap(4) {
                    ValueRef::Decimal(v) => v,
                    _ => Decimal::MIN,
                },
                settle_price: match row.get_ref_unwrap(5) {
                    ValueRef::Decimal(v) => v,
                    _ => Decimal::MIN,
                },
            })
        })?;
        let mut ps: Vec<Row> = prices_iter.map(|e| e.unwrap()).collect();
        prices.append(&mut ps);
    }

    Ok(prices)
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Row {
    source_ptid: i32,
    sink_ptid: i32,
    bucket: String,
    month: String,
    #[serde(with = "rust_decimal::serde::float")]
    source_price: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    sink_price: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    settle_price: Decimal,
}

#[cfg(test)]
mod tests {
    use std::{env, error::Error, path::Path, vec};

    use duckdb::Result;

    use rust_decimal_macros::dec;

    use crate::{api::isone::ftr::*, db::prod_db::ProdDb, interval::month::month};

    #[test]
    fn test_monthly_settle_prices() -> Result<(), Box<dyn Error>> {
        let data = get_monthly_settle_prices(
            (
                ProdDb::isone_dalmp(),
                ProdDb::buckets(),
                ProdDb::isone_ftr_cleared_prices(),
            ),
            (month(2025, 1), month(2025, 7)),
            vec![
                Path0 {
                    source: 4000,
                    sink: 4001,
                },
                Path0 {
                    source: 4000,
                    sink: 4008,
                },
            ],
            vec![Bucket::B5x16, Bucket::Offpeak],
        )
        .unwrap();
        assert_eq!(data.len(), 2 * 7 * 2); // 2 paths, 7 months, 2 buckets
        assert_eq!(
            data[0],
            Row {
                source_ptid: 4000,
                sink_ptid: 4001,
                bucket: "5x16".to_string(),
                month: "2025-01".to_string(),
                source_price: dec!(-0.0184),
                sink_price: dec!(-0.3498),
                settle_price: dec!(-0.3314),
            }
        );

        Ok(())
    }

    #[test]
    fn api_monthly_settle_prices() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/ftr/settle_price/monthly/start/2025-01/end/2025-07?source_ptids=4000,4000&sink_ptids=4001,4008&buckets=peak,offpeak",
            env::var("RUST_SERVER").unwrap(),
        );
        // println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 2 * 7 * 2); // 2 locations x 2 buckets x 7 months
        Ok(())
    }
}
