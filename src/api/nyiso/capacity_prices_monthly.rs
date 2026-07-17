use actix_web::{get, web, HttpResponse, Responder};
use duckdb::AccessMode;
use serde::Deserialize;
use std::time::Duration;

use crate::db::nyiso::capacity_prices_monthly::*;
use crate::utils::lib_duckdb::open_with_retry;

#[get("/nyiso/capacity_prices/monthly")]
pub async fn get_data_api(
    query: web::Query<ApiQuery>,
    data: web::Data<NyisoCapacityPricesMonthlyArchive>,
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
            data.duckdb_path,
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
    pub capability_period: Option<String>,
    pub capability_period_like: Option<String>,
    pub capability_period_in: Option<String>,
    pub location: Option<String>,
    pub location_like: Option<String>,
    pub location_in: Option<String>,
    pub _limit: Option<usize>,
}

impl ApiQuery {
    pub fn to_query_filter(&self) -> QueryFilter {
        QueryFilter {
            capability_period: self.capability_period.clone(),
            capability_period_like: self.capability_period_like.clone(),
            capability_period_in: self
                .capability_period_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            location: self.location.clone(),
            location_like: self.location_like.clone(),
            location_in: self
                .location_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
        }
    }
}

#[cfg(test)]
mod api_tests {
    use super::*;
    use crate::db::prod_db::ProdDb;
    use actix_web::{test, web, App};

    #[actix_web::test]
    async fn test_get_data_api() {
        let data = web::Data::new(ProdDb::nyiso_capacity_prices_monthly());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let params = QueryFilterBuilder::new().build().to_query_url();
        let uri = format!("/nyiso/capacity_prices/monthly?{}&_limit=5", params);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 5);
    }
}
