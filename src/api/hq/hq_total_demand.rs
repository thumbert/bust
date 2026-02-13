use actix_web::{get, web, HttpResponse, Responder};
use duckdb::AccessMode;
use serde::Deserialize;
use std::time::Duration;

use jiff::Zoned;
use rust_decimal::Decimal;

use crate::db::hq::electricity_demand::*;
use crate::utils::lib_duckdb::open_with_retry;

#[get("/hq/total_demand")]
pub async fn get_data_api(
    query: web::Query<ApiQuery>,
    data: web::Data<HqTotalDemandArchive>,
) -> impl Responder {
    let conn = open_with_retry(
        &data.duckdb_path,
        8,
        Duration::from_millis(25),
        AccessMode::ReadOnly,
    );
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database at {}: {}",
            &data.duckdb_path,
            conn.err().unwrap(),
        ));
    }
    let conn = conn.unwrap();

    let query_filter = query.to_query_filter();
    match get_data(&conn, &query_filter, query._limit) {
        Ok(records) => {
            if records.len() > 100_000 {
                HttpResponse::BadRequest()
                    .body(format!("Query returned {} records, only a max of 100,000 are allowed.  Please narrow your query.", records.len()))
            } else {
                HttpResponse::Ok().json(records)
            }
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("Error querying data: {}", e)),
    }
}

#[derive(Debug, Deserialize)]
struct ApiQuery {
    pub start_15min: Option<Zoned>,
    pub start_15min_gte: Option<Zoned>,
    pub start_15min_lt: Option<Zoned>,
    pub value: Option<Decimal>,
    pub value_in: Option<String>,
    pub value_gte: Option<Decimal>,
    pub value_lte: Option<Decimal>,
    pub _limit: Option<usize>,
}

impl ApiQuery {
    pub fn to_query_filter(&self) -> QueryFilter {
        QueryFilter {
            start_15min: self.start_15min.clone(),
            start_15min_gte: self.start_15min_gte.clone(),
            start_15min_lt: self.start_15min_lt.clone(),
            value: self.value,
            value_in: self
                .value_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            value_gte: self.value_gte,
            value_lte: self.value_lte,
        }
    }
}

#[cfg(test)]
mod api_tests {
    use crate::db::prod_db::ProdDb;

    use super::*;
    use actix_web::{test, web, App};

    #[actix_web::test]
    async fn test_get_data_api() {
        let data = web::Data::new(ProdDb::hq_total_demand());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let params = QueryFilterBuilder::new().build().to_query_url();
        let uri = format!("/hq/total_demand?{}&_limit=5", params);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 5);
    }
}
