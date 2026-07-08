use std::time::Duration;
use actix_web::{get, web, HttpResponse, Responder};
use serde::Deserialize;
use duckdb::AccessMode;

use crate::utils::lib_duckdb::open_with_retry;
use crate::db::nyiso::capacity_seasons::*;

#[get("/nyiso/capacity_seasons")]
pub async fn get_data_api(query: web::Query<ApiQuery>, data: web::Data<NyisoCapacitySeasonsArchive>) -> impl Responder {
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

    match get_data(&conn, query._limit) {
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
    pub _limit: Option<usize>,
}


#[cfg(test)]
mod api_tests {
    use super::*;
    use crate::db::prod_db::ProdDb;
    use actix_web::{test, web, App};

    #[actix_web::test]
    async fn test_get_data_api() {
        let data = web::Data::new(ProdDb::nyiso_capacity_seasons());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let uri = "/nyiso/capacity_seasons?_limit=5".to_owned();
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 5);
    }
}
