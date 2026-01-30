use std::time::Duration;
use actix_web::{get, web, HttpResponse, Responder};
use serde::Deserialize;
use duckdb::AccessMode;

use rust_decimal::Decimal;
use jiff::Zoned;

use crate::db::nyiso::binding_constraints::*;
use crate::utils::lib_duckdb::open_with_retry;

#[get("/nyiso/binding_constraints")]
pub async fn get_data_api(query: web::Query<ApiQuery>, data: web::Data<NyisoBindingConstraintsDaArchive>) -> impl Responder {
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
    pub market: Option<Market>,
    pub market_in: Option<String>,
    pub hour_beginning: Option<Zoned>,
    pub hour_beginning_gte: Option<Zoned>,
    pub hour_beginning_lt: Option<Zoned>,
    pub limiting_facility: Option<String>,
    pub limiting_facility_like: Option<String>,
    pub limiting_facility_in: Option<String>,
    pub facility_ptid: Option<i64>,
    pub facility_ptid_in: Option<String>,
    pub facility_ptid_gte: Option<i64>,
    pub facility_ptid_lte: Option<i64>,
    pub contingency: Option<String>,
    pub contingency_like: Option<String>,
    pub contingency_in: Option<String>,
    pub constraint_cost: Option<Decimal>,
    pub constraint_cost_in: Option<String>,
    pub constraint_cost_gte: Option<Decimal>,
    pub constraint_cost_lte: Option<Decimal>,
    pub _limit: Option<usize>,
}

impl ApiQuery {
    pub fn to_query_filter(&self) -> QueryFilter {
        QueryFilter {
            market: self.market,
            market_in: self.market_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse::<Market>().unwrap()).collect()),
            hour_beginning: self.hour_beginning.clone(),
            hour_beginning_gte: self.hour_beginning_gte.clone(),
            hour_beginning_lt: self.hour_beginning_lt.clone(),
            limiting_facility: self.limiting_facility.clone(),
            limiting_facility_like: self.limiting_facility_like.clone(),
            limiting_facility_in: self.limiting_facility_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            facility_ptid: self.facility_ptid,
            facility_ptid_in: self.facility_ptid_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            facility_ptid_gte: self.facility_ptid_gte,
            facility_ptid_lte: self.facility_ptid_lte,
            contingency: self.contingency.clone(),
            contingency_like: self.contingency_like.clone(),
            contingency_in: self.contingency_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            constraint_cost: self.constraint_cost,
            constraint_cost_in: self.constraint_cost_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            constraint_cost_gte: self.constraint_cost_gte,
            constraint_cost_lte: self.constraint_cost_lte,
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
        let data = web::Data::new(ProdDb::nyiso_binding_constraints_da());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let params = QueryFilterBuilder::new().build().to_query_url();
        let uri = format!("/nyiso/binding_constraints?{}&_limit=5", params);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 5);
    }
}
