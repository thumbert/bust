use actix_web::{get, web, HttpResponse, Responder};
use duckdb::AccessMode;
use serde::Deserialize;
use std::time::Duration;

use jiff::Zoned;

use crate::db::isone::ttc_archive::*;
use crate::utils::lib_duckdb::open_with_retry;

#[get("/isone/ttc")]
pub async fn get_data_api(
    query: web::Query<ApiQuery>,
    data: web::Data<IsoneTtcArchive>,
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
    pub hour_beginning: Option<Zoned>,
    pub hour_beginning_gte: Option<Zoned>,
    pub hour_beginning_lt: Option<Zoned>,
    pub interface_name: Option<String>,
    pub interface_name_like: Option<String>,
    pub interface_name_in: Option<String>,
    pub flow_direction: Option<FlowDirection>,
    pub flow_direction_in: Option<String>,
    pub mw: Option<i64>,
    pub mw_in: Option<String>,
    pub mw_gte: Option<i64>,
    pub mw_lte: Option<i64>,
    pub _limit: Option<usize>,
}

impl ApiQuery {
    pub fn to_query_filter(&self) -> QueryFilter {
        QueryFilter {
            hour_beginning: self.hour_beginning.clone(),
            hour_beginning_gte: self.hour_beginning_gte.clone(),
            hour_beginning_lt: self.hour_beginning_lt.clone(),
            interface_name: self.interface_name.clone(),
            interface_name_like: self.interface_name_like.clone(),
            interface_name_in: self.interface_name_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            flow_direction: self.flow_direction,
            flow_direction_in: self.flow_direction_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse::<FlowDirection>().unwrap()).collect()),
            mw: self.mw,
            mw_in: self.mw_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            mw_gte: self.mw_gte,
            mw_lte: self.mw_lte,
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
        let data = web::Data::new(ProdDb::isone_ttc());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let params = QueryFilterBuilder::new().build().to_query_url();
        let uri = format!("/isone/ttc?{}&_limit=5", params);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 5);
    }
}


