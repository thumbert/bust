use actix_web::{get, web, HttpResponse, Responder};
use duckdb::AccessMode;
use serde::Deserialize;
use std::time::Duration;

use jiff::civil::Date;

use crate::db::isone::mis::sr_dalocsum::*;
use crate::utils::lib_duckdb::open_with_retry;

#[get("/isone/mis/sr_dalocsum/tab1")]
pub async fn get_data_api(
    query: web::Query<ApiQuery>,
    data: web::Data<SrDalocsumArchive>,
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
    pub report_date: Option<Date>,
    pub report_date_in: Option<String>,
    pub report_date_gte: Option<Date>,
    pub report_date_lte: Option<Date>,
    pub account_id: Option<u32>,
    pub account_id_in: Option<String>,
    pub account_id_gte: Option<u32>,
    pub account_id_lte: Option<u32>,
    pub subaccount_id: Option<u32>,
    pub subaccount_id_in: Option<String>,
    pub subaccount_id_gte: Option<u32>,
    pub subaccount_id_lte: Option<u32>,
    pub location_id: Option<u32>,
    pub location_id_in: Option<String>,
    pub location_id_gte: Option<u32>,
    pub location_id_lte: Option<u32>,
    pub _limit: Option<usize>,
}

impl ApiQuery {
    pub fn to_query_filter(&self) -> QueryFilter {
        QueryFilter {
            report_date: self.report_date,
            report_date_in: self.report_date_in.as_ref().map(|s| {
                s.split(',')
                    .map(|v| v.trim().parse::<Date>().unwrap())
                    .collect()
            }),
            report_date_gte: self.report_date_gte,
            report_date_lte: self.report_date_lte,
            account_id: self.account_id,
            account_id_in: self
                .account_id_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            account_id_gte: self.account_id_gte,
            account_id_lte: self.account_id_lte,
            subaccount_id: self.subaccount_id,
            subaccount_id_in: self
                .subaccount_id_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            subaccount_id_gte: self.subaccount_id_gte,
            subaccount_id_lte: self.subaccount_id_lte,
            location_id: self.location_id,
            location_id_in: self
                .location_id_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            location_id_gte: self.location_id_gte,
            location_id_lte: self.location_id_lte,
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
        let data = web::Data::new(ProdDb::sr_dalocsum());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let params = QueryFilterBuilder::new().build().to_query_url();
        let uri = format!("/mis/sr_dalocsum/tab1?{}&_limit=5", params);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 5);
    }
}
