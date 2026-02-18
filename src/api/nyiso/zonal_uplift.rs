use std::time::Duration;
use actix_web::{get, web, HttpResponse, Responder};
use serde::Deserialize;
use duckdb::AccessMode;

use jiff::civil::Date;
use rust_decimal::Decimal;

use crate::{db::nyiso::zonal_uplift::{NyisoZonalUpliftArchive, *}, utils::lib_duckdb::open_with_retry};

#[get("/nyiso/zonal_uplift")]
pub async fn get_data_api(query: web::Query<ApiQuery>, data: web::Data<NyisoZonalUpliftArchive>) -> impl Responder {
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
    pub day: Option<Date>,
    pub day_in: Option<String>,
    pub day_gte: Option<Date>,
    pub day_lte: Option<Date>,
    pub ptid: Option<String>,
    pub ptid_like: Option<String>,
    pub ptid_in: Option<String>,
    pub name: Option<String>,
    pub name_like: Option<String>,
    pub name_in: Option<String>,
    pub uplift_category: Option<String>,
    pub uplift_category_like: Option<String>,
    pub uplift_category_in: Option<String>,
    pub uplift_payment: Option<Decimal>,
    pub uplift_payment_in: Option<String>,
    pub uplift_payment_gte: Option<Decimal>,
    pub uplift_payment_lte: Option<Decimal>,
    pub _limit: Option<usize>,
}

impl ApiQuery {
    pub fn to_query_filter(&self) -> QueryFilter {
        QueryFilter {
            day: self.day,
            day_in: self.day_in.as_ref().map(|s| {s.split(',').map(|v| v.trim().parse::<Date>().unwrap()).collect()}),
            day_gte: self.day_gte,
            day_lte: self.day_lte,
            ptid: self.ptid.clone(),
            ptid_like: self.ptid_like.clone(),
            ptid_in: self.ptid_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            name: self.name.clone(),
            name_like: self.name_like.clone(),
            name_in: self.name_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            uplift_category: self.uplift_category.clone(),
            uplift_category_like: self.uplift_category_like.clone(),
            uplift_category_in: self.uplift_category_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            uplift_payment: self.uplift_payment,
            uplift_payment_in: self.uplift_payment_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            uplift_payment_gte: self.uplift_payment_gte,
            uplift_payment_lte: self.uplift_payment_lte,
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
        let data = web::Data::new(ProdDb::nyiso_zonal_uplift());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let params = QueryFilterBuilder::new().build().to_query_url();
        let uri = format!("/nyiso/zonal_uplift?{}&_limit=5", params);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 5);
    }
}
