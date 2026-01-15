use actix_web::{get, web, HttpResponse, Responder};
use duckdb::AccessMode;
use serde::Deserialize;
use std::time::Duration;

use rust_decimal::Decimal;

use crate::{db::isone::masked_data::ara_archive::*, utils::lib_duckdb::open_with_retry};

#[get("/isone/capacity/ara/bids_offers")]
pub async fn get_data_api(
    query: web::Query<ApiQuery>,
    data: web::Data<IsoneAraBidsOffersArchive>,
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
    pub capability_period: Option<String>,
    pub capability_period_like: Option<String>,
    pub capability_period_in: Option<String>,
    pub auction_type: Option<AuctionType>,
    pub auction_type_in: Option<String>,
    pub masked_resource_id: Option<u32>,
    pub masked_resource_id_in: Option<String>,
    pub masked_resource_id_gte: Option<u32>,
    pub masked_resource_id_lte: Option<u32>,
    pub masked_participant_id: Option<u32>,
    pub masked_participant_id_in: Option<String>,
    pub masked_participant_id_gte: Option<u32>,
    pub masked_participant_id_lte: Option<u32>,
    pub masked_capacity_zone_id: Option<u16>,
    pub masked_capacity_zone_id_in: Option<String>,
    pub masked_capacity_zone_id_gte: Option<u16>,
    pub masked_capacity_zone_id_lte: Option<u16>,
    pub resource_type: Option<ResourceType>,
    pub resource_type_in: Option<String>,
    pub bid_offer: Option<BidOffer>,
    pub bid_offer_in: Option<String>,
    pub segment: Option<u8>,
    pub segment_in: Option<String>,
    pub segment_gte: Option<u8>,
    pub segment_lte: Option<u8>,
    pub quantity: Option<Decimal>,
    pub quantity_in: Option<String>,
    pub quantity_gte: Option<Decimal>,
    pub quantity_lte: Option<Decimal>,
    pub price: Option<Decimal>,
    pub price_in: Option<String>,
    pub price_gte: Option<Decimal>,
    pub price_lte: Option<Decimal>,
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
            auction_type: self.auction_type,
            auction_type_in: self.auction_type_in.as_ref().map(|s| {
                s.split(',')
                    .map(|v| v.trim().parse::<AuctionType>().unwrap())
                    .collect()
            }),
            masked_resource_id: self.masked_resource_id,
            masked_resource_id_in: self
                .masked_resource_id_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            masked_resource_id_gte: self.masked_resource_id_gte,
            masked_resource_id_lte: self.masked_resource_id_lte,
            masked_participant_id: self.masked_participant_id,
            masked_participant_id_in: self
                .masked_participant_id_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            masked_participant_id_gte: self.masked_participant_id_gte,
            masked_participant_id_lte: self.masked_participant_id_lte,
            masked_capacity_zone_id: self.masked_capacity_zone_id,
            masked_capacity_zone_id_in: self
                .masked_capacity_zone_id_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            masked_capacity_zone_id_gte: self.masked_capacity_zone_id_gte,
            masked_capacity_zone_id_lte: self.masked_capacity_zone_id_lte,
            resource_type: self.resource_type,
            resource_type_in: self.resource_type_in.as_ref().map(|s| {
                s.split(',')
                    .map(|v| v.trim().parse::<ResourceType>().unwrap())
                    .collect()
            }),
            bid_offer: self.bid_offer,
            bid_offer_in: self.bid_offer_in.as_ref().map(|s| {
                s.split(',')
                    .map(|v| v.trim().parse::<BidOffer>().unwrap())
                    .collect()
            }),
            segment: self.segment,
            segment_in: self
                .segment_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            segment_gte: self.segment_gte,
            segment_lte: self.segment_lte,
            quantity: self.quantity,
            quantity_in: self
                .quantity_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            quantity_gte: self.quantity_gte,
            quantity_lte: self.quantity_lte,
            price: self.price,
            price_in: self
                .price_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            price_gte: self.price_gte,
            price_lte: self.price_lte,
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
        let data = web::Data::new(ProdDb::isone_masked_ara_bids_offers());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let params = QueryFilterBuilder::new().build().to_query_url();
        let uri = format!("/isone/capacity/mra/bids_offers?{}&_limit=5", params);
        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let rs: Vec<Record> = test::read_body_json(resp).await;
        assert_eq!(rs.len(), 5);
    }
}
