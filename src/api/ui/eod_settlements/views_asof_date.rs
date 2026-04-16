use actix_web::{get, web, HttpResponse, Responder};
use duckdb::AccessMode;
use serde::Deserialize;
use std::time::Duration;

use crate::db::ui::eod_settlements::views_asof_date::*;
use crate::utils::lib_duckdb::open_with_retry;

#[get("/ui/eod_settlements/asof_date/users_views")]
pub async fn get_users_views(data: web::Data<UiEodSettlementsAsOfDateArchive>) -> impl Responder {
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

    match users_views(&conn) {
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

#[get("/ui/eod_settlements/asof_date")]
pub async fn get_data_api(query: web::Query<ApiQuery>, data: web::Data<UiEodSettlementsAsOfDateArchive>) -> impl Responder {
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
    pub user_id: Option<String>,
    pub user_id_like: Option<String>,
    pub user_id_in: Option<String>,
    pub view_name: Option<String>,
    pub view_name_like: Option<String>,
    pub view_name_in: Option<String>,
    pub _limit: Option<usize>,
}

impl ApiQuery {
    pub fn to_query_filter(&self) -> QueryFilter {
        QueryFilter {
            user_id: self.user_id.clone(),
            user_id_like: self.user_id_like.clone(),
            user_id_in: self.user_id_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            view_name: self.view_name.clone(),
            view_name_like: self.view_name_like.clone(),
            view_name_in: self.view_name_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
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
        let data = web::Data::new(ProdDb::ui_eod_settlements_asof_date());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let params = QueryFilterBuilder::new().build().to_query_url();
        let uri = format!("/ui/eod_settlements/asof_date?{}&_limit=5", params);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 5);
    }

    #[actix_web::test]
    async fn test_get_users_views() {
        let data = web::Data::new(ProdDb::ui_eod_settlements_asof_date());
        let app =
            test::init_service(App::new().app_data(data.clone()).service(get_users_views)).await;
        let uri = "/ui/eod_settlements/asof_date/users_views";
        let req = test::TestRequest::get().uri(uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<(String, String)> = test::read_body_json(resp).await;
        let users: Vec<String> = rs.iter().map(|(user, _)| user.clone()).collect();
        assert!(users.contains(&"adrian".to_string()));
    }
}
