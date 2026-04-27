use actix_web::{get, post, web, HttpResponse, Responder};
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
pub async fn get_data_api(
    query: web::Query<ApiQuery>,
    data: web::Data<UiEodSettlementsAsOfDateArchive>,
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
            user_id_in: self
                .user_id_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            view_name: self.view_name.clone(),
            view_name_like: self.view_name_like.clone(),
            view_name_in: self
                .view_name_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
        }
    }
}

#[derive(Debug, Deserialize)]
struct UploadRequest {
    pub user_id: String,
    pub view_name: String,
    pub records: Vec<Record>,
}

#[post("/ui/eod_settlements/asof_date")]
pub async fn post_data_api(
    body: web::Json<UploadRequest>,
    data: web::Data<UiEodSettlementsAsOfDateArchive>,
) -> impl Responder {
    // Validate all records belong to the declared user_id + view_name
    for record in &body.records {
        if record.user_id != body.user_id || record.view_name != body.view_name {
            return HttpResponse::BadRequest()
                .body("All records must have the same user_id and view_name as the request");
        }
    }

    let conn = open_with_retry(
        &data.duckdb_path,
        8,
        Duration::from_millis(25),
        AccessMode::ReadWrite,
    );
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database at {}: {}",
            &data.duckdb_path,
            conn.err().unwrap(),
        ));
    }
    let conn = conn.unwrap();

    match write_records(&conn, &body.user_id, &body.view_name, &body.records) {
        Ok(()) => HttpResponse::Ok().body(format!("Uploaded {} records", body.records.len())),
        Err(e) => HttpResponse::InternalServerError().body(format!("Error uploading data: {}", e)),
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

    #[actix_web::test]
    async fn test_post_data_api() {
        use jiff::civil::date;

        let data = web::Data::new(ProdDb::ui_eod_settlements_asof_date());
        let app = test::init_service(
            App::new()
                .app_data(data.clone())
                .service(post_data_api)
                .service(get_data_api),
        )
        .await;

        let user_id = "test_user_post";
        let view_name = "test_view_post";

        let records = vec![
            Record {
                user_id: user_id.to_string(),
                view_name: view_name.to_string(),
                row_id: 1,
                source: "ICE".to_string(),
                ice_category: Some("Power".to_string()),
                ice_hub: Some("NE".to_string()),
                ice_product: Some("Electricity".to_string()),
                endur_curve_name: None,
                nodal_contract_name: None,
                as_of_date: date(2026, 1, 15),
                strip: Some("2026-02".to_string()),
                unit_conversion: None,
                label: Some("test label".to_string()),
            },
            Record {
                user_id: user_id.to_string(),
                view_name: view_name.to_string(),
                row_id: 2,
                source: "Nodal".to_string(),
                ice_category: None,
                ice_hub: None,
                ice_product: None,
                endur_curve_name: None,
                nodal_contract_name: Some("HB_NORTH".to_string()),
                as_of_date: date(2026, 1, 15),
                strip: Some("2026-03".to_string()),
                unit_conversion: None,
                label: None,
            },
        ];

        // Upload the records
        let upload_request = serde_json::json!({
            "user_id": user_id,
            "view_name": view_name,
            "records": records,
        });
        println!("{}", upload_request);

        let req = test::TestRequest::post()
            .uri("/ui/eod_settlements/asof_date")
            .set_json(&upload_request)
            .to_request();
        let resp = test::call_service(&app, req).await;
        if !resp.status().is_success() {
            let status = resp.status();
            let body_bytes = test::read_body(resp).await;
            let body_text = String::from_utf8_lossy(&body_bytes);
            panic!("POST failed: {} body: {}", status, body_text);
        }

        // Verify records were inserted via GET
        let filter_url = QueryFilterBuilder::new()
            .user_id(user_id)
            .view_name(view_name)
            .build()
            .to_query_url();
        let uri = format!("/ui/eod_settlements/asof_date?{}", filter_url);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 2);
        assert_eq!(rs[0].source, "ICE");
        assert_eq!(rs[1].nodal_contract_name, Some("HB_NORTH".to_string()));

        // Clean up: upload an empty list to delete the test records
        let cleanup = serde_json::json!({
            "user_id": user_id,
            "view_name": view_name,
            "records": [],
        });
        let req = test::TestRequest::post()
            .uri("/ui/eod_settlements/asof_date")
            .set_json(&cleanup)
            .to_request();
        let resp = test::call_service(&app, req).await;
        assert!(
            resp.status().is_success(),
            "Cleanup POST failed: {:?}",
            resp.status()
        );
    }
}
