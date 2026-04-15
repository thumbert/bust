use actix_web::{get, web, HttpResponse, Responder};
use duckdb::AccessMode;
use serde::Deserialize;
use std::time::Duration;

use jiff::civil::Date;

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
    pub row_id: Option<u32>,
    pub row_id_in: Option<String>,
    pub row_id_gte: Option<u32>,
    pub row_id_lte: Option<u32>,
    pub source: Option<String>,
    pub source_like: Option<String>,
    pub source_in: Option<String>,
    pub ice_category: Option<String>,
    pub ice_category_like: Option<String>,
    pub ice_category_in: Option<String>,
    pub ice_hub: Option<String>,
    pub ice_hub_like: Option<String>,
    pub ice_hub_in: Option<String>,
    pub ice_product: Option<String>,
    pub ice_product_like: Option<String>,
    pub ice_product_in: Option<String>,
    pub endur_curve_name: Option<String>,
    pub endur_curve_name_like: Option<String>,
    pub endur_curve_name_in: Option<String>,
    pub nodal_contract_name: Option<String>,
    pub nodal_contract_name_like: Option<String>,
    pub nodal_contract_name_in: Option<String>,
    pub as_of_date: Option<Date>,
    pub as_of_date_in: Option<String>,
    pub as_of_date_gte: Option<Date>,
    pub as_of_date_lte: Option<Date>,
    pub strip: Option<String>,
    pub strip_like: Option<String>,
    pub strip_in: Option<String>,
    pub unit_conversion: Option<String>,
    pub unit_conversion_like: Option<String>,
    pub unit_conversion_in: Option<String>,
    pub label: Option<String>,
    pub label_like: Option<String>,
    pub label_in: Option<String>,
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
            row_id: self.row_id,
            row_id_in: self
                .row_id_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            row_id_gte: self.row_id_gte,
            row_id_lte: self.row_id_lte,
            source: self.source.clone(),
            source_like: self.source_like.clone(),
            source_in: self
                .source_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            ice_category: self.ice_category.clone(),
            ice_category_like: self.ice_category_like.clone(),
            ice_category_in: self
                .ice_category_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            ice_hub: self.ice_hub.clone(),
            ice_hub_like: self.ice_hub_like.clone(),
            ice_hub_in: self
                .ice_hub_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            ice_product: self.ice_product.clone(),
            ice_product_like: self.ice_product_like.clone(),
            ice_product_in: self
                .ice_product_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            endur_curve_name: self.endur_curve_name.clone(),
            endur_curve_name_like: self.endur_curve_name_like.clone(),
            endur_curve_name_in: self
                .endur_curve_name_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            nodal_contract_name: self.nodal_contract_name.clone(),
            nodal_contract_name_like: self.nodal_contract_name_like.clone(),
            nodal_contract_name_in: self
                .nodal_contract_name_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            as_of_date: self.as_of_date,
            as_of_date_in: self.as_of_date_in.as_ref().map(|s| {
                s.split(',')
                    .map(|v| v.trim().parse::<Date>().unwrap())
                    .collect()
            }),
            as_of_date_gte: self.as_of_date_gte,
            as_of_date_lte: self.as_of_date_lte,
            strip: self.strip.clone(),
            strip_like: self.strip_like.clone(),
            strip_in: self
                .strip_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            unit_conversion: self.unit_conversion.clone(),
            unit_conversion_like: self.unit_conversion_like.clone(),
            unit_conversion_in: self
                .unit_conversion_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            label: self.label.clone(),
            label_like: self.label_like.clone(),
            label_in: self
                .label_in
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
