use std::time::Duration;

use duckdb::{AccessMode, Connection, Result};
use jiff::tz::TimeZone;
use serde::Deserialize;

use crate::{
    bucket::{count_hours, Bucket},
    db::calendar::buckets::BucketsArchive,
    interval::{term::Term, term_tz::TermTz},
    utils::lib_duckdb::open_with_retry,
};
use actix_web::{get, web, HttpResponse, Responder};

#[get("/calendar/buckets/all")]
async fn api_get_all(db: web::Data<BucketsArchive>) -> impl Responder {
    let conn = open_with_retry(
        &db.duckdb_path,
        8,
        Duration::from_millis(25),
        AccessMode::ReadOnly,
    );
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database: {}",
            conn.err().unwrap()
        ));
    }

    let ids = get_all(&conn.unwrap());
    match ids {
        Ok(vs) => HttpResponse::Ok().json(vs),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

type Out = Vec<(Bucket, TermTz, i32)>;

#[get("/calendar/buckets/count_hours")]
async fn api_count_hours(query: web::Query<ApiQuery>) -> impl Responder {
    let tz = TimeZone::get(&query.timezone);
    if tz.is_err() {
        return HttpResponse::BadRequest().json(Err::<Out, String>(format!(
            "Invalid time zone specified: {}",
            &query.timezone
        )));
    }
    let tz = tz.unwrap();
    let buckets: Result<Vec<Bucket>, String> = query
        .buckets
        .split(',')
        .map(|b| b.parse::<Bucket>().map_err(|e| e.to_string()))
        .collect();

    let terms: Result<Vec<TermTz>, String> = query
        .terms
        .split(',')
        .map(|t| {
            t.parse::<Term>()
                .map_err(|e| e.to_string())
                .map(|term| term.with_tz(&tz))
        })
        .collect();

    let pairs: Result<Vec<(Bucket, TermTz)>, String> = match (buckets, terms) {
        (Ok(buckets), Ok(terms)) => Ok(buckets
            .into_iter()
            .flat_map(|bucket| terms.iter().cloned().map(move |term| (bucket, term)))
            .collect()),
        (Err(e), _) | (_, Err(e)) => Err(e),
    };
    match pairs {
        Ok(pairs) => {
            let res = count_hours(pairs);
            HttpResponse::Ok().json(res)
        }
        Err(e) => {
            HttpResponse::BadRequest().json(Err::<Out, String>(format!("Parse error: {}", e)))
        }
    }
}

#[derive(Deserialize)]
struct ApiQuery {
    pub buckets: String,
    pub terms: String,
    pub timezone: String,
}

fn get_all(conn: &Connection) -> Result<Vec<String>> {
    let query = r#"
SELECT name
FROM pragma_table_info('buckets')
WHERE type = 'BOOLEAN'
ORDER BY name;
"#;
    let mut stmt = conn.prepare(query).unwrap();
    let res_iter = stmt.query_map([], |row| Ok(row.get::<usize, String>(0).unwrap()))?;
    let res: Vec<String> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Result};

    use crate::db::prod_db::ProdDb;

    use super::*;

    #[test]
    fn test_bucket_names() -> Result<()> {
        let conn = open_with_retry(
            &ProdDb::buckets().duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        )
        .unwrap();
        let names = get_all(&conn).unwrap();
        assert!(names.contains(&"atc".to_string()));
        assert!(names.contains(&"2x16H".to_string()));
        assert!(names.contains(&"7x8".to_string()));
        assert!(names.contains(&"5x16".to_string()));
        assert!(names.contains(&"caiso_1x16H".to_string()));
        assert!(names.contains(&"caiso_6x16".to_string()));
        Ok(())
    }

    #[test]
    fn api_status() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!("{}/calendar/buckets/all", env::var("RUST_SERVER").unwrap(),);
        let response = reqwest::blocking::get(url)?.text()?;
        let names: Vec<String> = serde_json::from_str(&response).unwrap();
        assert!(names.contains(&"atc".to_string()));
        assert!(names.contains(&"2x16H".to_string()));
        assert!(names.contains(&"7x8".to_string()));
        assert!(names.contains(&"5x16".to_string()));
        assert!(names.contains(&"caiso_1x16H".to_string()));
        assert!(names.contains(&"caiso_6x16".to_string()));
        Ok(())
    }

    #[test]
    fn api_count_hours_1() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!("{}/calendar/buckets/count_hours?buckets=5x16,2x16H,7x8&terms=2022&timezone=America/New_York", env::var("RUST_SERVER").unwrap(),);
        println!("URL: {}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let counts: Vec<(Bucket, TermTz, i32)> = serde_json::from_str(&response).unwrap();
        println!("Counts: {:?}", counts);

        Ok(())
    }
}
