use std::time::Duration;

use duckdb::AccessMode;
use jiff::Zoned;
use rust_decimal::Decimal;
use serde::Deserialize;

use crate::{db::caiso::public_bids_archive::*, utils::lib_duckdb::open_with_retry};
use actix_web::{get, web, HttpResponse, Responder};

#[get("/caiso/public_bids")]
async fn api_get_data(
    query: web::Query<ApiQuery>,
    db: web::Data<CaisoPublicBidsArchive>,
) -> impl Responder {
    let conn = open_with_retry(
        &db.duckdb_path,
        8,
        Duration::from_millis(25),
        AccessMode::ReadOnly,
    );
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database: {}",
            conn.err().unwrap()
        ));
    }

    let query_filter = query.into_inner().to_query_filter();
    // let query_filter = match query_filter {
    //     Ok(qf) => qf,
    //     Err(e) => {
    //         return HttpResponse::BadRequest().body(format!("Invalid query parameters: {}", e));
    //     }
    // };

    let ids = get_data(&conn.unwrap(), &query_filter);
    match ids {
        Ok(vs) => HttpResponse::Ok().json(vs),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

#[derive(Debug, Deserialize)]
pub struct ApiQuery {
    pub hour_beginning: Option<Zoned>,
    pub hour_beginning_gte: Option<Zoned>,
    pub hour_beginning_lt: Option<Zoned>,
    pub resource_type: Option<ResourceType>,
    pub resource_type_in: Option<String>,
    pub scheduling_coordinator_seq: Option<u32>,
    pub scheduling_coordinator_seq_in: Option<String>,
    pub scheduling_coordinator_seq_gte: Option<u32>,
    pub scheduling_coordinator_seq_lte: Option<u32>,
    pub resource_bid_seq: Option<u32>,
    pub resource_bid_seq_in: Option<String>,
    pub resource_bid_seq_gte: Option<u32>,
    pub resource_bid_seq_lte: Option<u32>,
    pub time_interval_start: Option<Zoned>,
    pub time_interval_start_gte: Option<Zoned>,
    pub time_interval_start_lt: Option<Zoned>,
    pub time_interval_end: Option<Zoned>,
    pub time_interval_end_gte: Option<Zoned>,
    pub time_interval_end_lt: Option<Zoned>,
    pub product_bid_desc: Option<String>,
    pub product_bid_desc_like: Option<String>,
    pub product_bid_desc_in: Option<String>,
    pub product_bid_mrid: Option<String>,
    pub product_bid_mrid_like: Option<String>,
    pub product_bid_mrid_in: Option<String>,
    pub market_product_desc: Option<String>,
    pub market_product_desc_like: Option<String>,
    pub market_product_desc_in: Option<String>,
    pub market_product_type: Option<String>,
    pub market_product_type_like: Option<String>,
    pub market_product_type_in: Option<String>,
    pub self_sched_mw: Option<Decimal>,
    pub self_sched_mw_in: Option<String>,
    pub self_sched_mw_gte: Option<Decimal>,
    pub self_sched_mw_lte: Option<Decimal>,
    pub sch_bid_time_interval_start: Option<Zoned>,
    pub sch_bid_time_interval_start_gte: Option<Zoned>,
    pub sch_bid_time_interval_start_lt: Option<Zoned>,
    pub sch_bid_time_interval_end: Option<Zoned>,
    pub sch_bid_time_interval_end_gte: Option<Zoned>,
    pub sch_bid_time_interval_end_lt: Option<Zoned>,
    pub sch_bid_xaxis_data: Option<Decimal>,
    pub sch_bid_xaxis_data_in: Option<String>,
    pub sch_bid_xaxis_data_gte: Option<Decimal>,
    pub sch_bid_xaxis_data_lte: Option<Decimal>,
    pub sch_bid_y1axis_data: Option<Decimal>,
    pub sch_bid_y1axis_data_in: Option<String>,
    pub sch_bid_y1axis_data_gte: Option<Decimal>,
    pub sch_bid_y1axis_data_lte: Option<Decimal>,
    pub sch_bid_y2axis_data: Option<Decimal>,
    pub sch_bid_y2axis_data_in: Option<String>,
    pub sch_bid_y2axis_data_gte: Option<Decimal>,
    pub sch_bid_y2axis_data_lte: Option<Decimal>,
    pub sch_bid_curve_type: Option<SchBidCurveType>,
    pub sch_bid_curve_type_in: Option<String>,
    pub min_eoh_state_of_charge: Option<Decimal>,
    pub min_eoh_state_of_charge_in: Option<String>,
    pub min_eoh_state_of_charge_gte: Option<Decimal>,
    pub min_eoh_state_of_charge_lte: Option<Decimal>,
    pub max_eoh_state_of_charge: Option<Decimal>,
    pub max_eoh_state_of_charge_in: Option<String>,
    pub max_eoh_state_of_charge_gte: Option<Decimal>,
    pub max_eoh_state_of_charge_lte: Option<Decimal>,
}

impl ApiQuery {
    pub fn to_query_filter(&self) -> QueryFilter {
        QueryFilter {
            hour_beginning: self.hour_beginning.clone(),
            hour_beginning_gte: self.hour_beginning_gte.clone(),
            hour_beginning_lt: self.hour_beginning_lt.clone(),
            resource_type: self.resource_type.clone(),
            resource_type_in: self
                .resource_type_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            scheduling_coordinator_seq: self.scheduling_coordinator_seq,
            scheduling_coordinator_seq_in: self
                .scheduling_coordinator_seq_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            scheduling_coordinator_seq_gte: self.scheduling_coordinator_seq_gte,
            scheduling_coordinator_seq_lte: self.scheduling_coordinator_seq_lte,
            resource_bid_seq: self.resource_bid_seq,
            resource_bid_seq_in: self
                .resource_bid_seq_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            resource_bid_seq_gte: self.resource_bid_seq_gte,
            resource_bid_seq_lte: self.resource_bid_seq_lte,
            time_interval_start: self.time_interval_start.clone(),
            time_interval_start_gte: self.time_interval_start_gte.clone(),
            time_interval_start_lt: self.time_interval_start_lt.clone(),
            time_interval_end: self.time_interval_end.clone(),
            time_interval_end_gte: self.time_interval_end_gte.clone(),
            time_interval_end_lt: self.time_interval_end_lt.clone(),
            product_bid_desc: self.product_bid_desc.clone(),
            product_bid_desc_like: self.product_bid_desc_like.clone(),
            product_bid_desc_in: self
                .product_bid_desc_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            product_bid_mrid: self.product_bid_mrid.clone(),
            product_bid_mrid_like: self.product_bid_mrid_like.clone(),
            product_bid_mrid_in: self
                .product_bid_mrid_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            market_product_desc: self.market_product_desc.clone(),
            market_product_desc_like: self.market_product_desc_like.clone(),
            market_product_desc_in: self
                .market_product_desc_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            market_product_type: self.market_product_type.clone(),
            market_product_type_like: self.market_product_type_like.clone(),
            market_product_type_in: self
                .market_product_type_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            self_sched_mw: self.self_sched_mw,
            self_sched_mw_in: self
                .self_sched_mw_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            self_sched_mw_gte: self.self_sched_mw_gte,
            self_sched_mw_lte: self.self_sched_mw_lte,
            sch_bid_time_interval_start: self.sch_bid_time_interval_start.clone(),
            sch_bid_time_interval_start_gte: self.sch_bid_time_interval_start_gte.clone(),
            sch_bid_time_interval_start_lt: self.sch_bid_time_interval_start_lt.clone(),
            sch_bid_time_interval_end: self.sch_bid_time_interval_end.clone(),
            sch_bid_time_interval_end_gte: self.sch_bid_time_interval_end_gte.clone(),
            sch_bid_time_interval_end_lt: self.sch_bid_time_interval_end_lt.clone(),
            sch_bid_xaxis_data: self.sch_bid_xaxis_data,
            sch_bid_xaxis_data_in: self
                .sch_bid_xaxis_data_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            sch_bid_xaxis_data_gte: self.sch_bid_xaxis_data_gte,
            sch_bid_xaxis_data_lte: self.sch_bid_xaxis_data_lte,
            sch_bid_y1axis_data: self.sch_bid_y1axis_data,
            sch_bid_y1axis_data_in: self
                .sch_bid_y1axis_data_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            sch_bid_y1axis_data_gte: self.sch_bid_y1axis_data_gte,
            sch_bid_y1axis_data_lte: self.sch_bid_y1axis_data_lte,
            sch_bid_y2axis_data: self.sch_bid_y2axis_data,
            sch_bid_y2axis_data_in: self
                .sch_bid_y2axis_data_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            sch_bid_y2axis_data_gte: self.sch_bid_y2axis_data_gte,
            sch_bid_y2axis_data_lte: self.sch_bid_y2axis_data_lte,
            sch_bid_curve_type: self.sch_bid_curve_type.clone(),
            sch_bid_curve_type_in: self
                .sch_bid_curve_type_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            min_eoh_state_of_charge: self.min_eoh_state_of_charge,
            min_eoh_state_of_charge_in: self
                .min_eoh_state_of_charge_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            min_eoh_state_of_charge_gte: self.min_eoh_state_of_charge_gte,
            min_eoh_state_of_charge_lte: self.min_eoh_state_of_charge_lte,
            max_eoh_state_of_charge: self.max_eoh_state_of_charge,
            max_eoh_state_of_charge_in: self
                .max_eoh_state_of_charge_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            max_eoh_state_of_charge_gte: self.max_eoh_state_of_charge_gte,
            max_eoh_state_of_charge_lte: self.max_eoh_state_of_charge_lte,
            resource_type_like: todo!(),
            sch_bid_curve_type_like: todo!(),
        }
    }
}

// impl ApiQuery {
//     pub fn to_query_filter(&self) -> QueryFilter {
//         QueryFilter {
//             hour_beginning: self.hour_beginning.clone(),
//             hour_beginning_gte: self.hour_beginning_gte.clone(),
//             hour_beginning_lt: self.hour_beginning_lt.clone(),
//             resource_type: self.resource_type,
//             resource_type_in: self.resource_type_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             scheduling_coordinator_seq: self.scheduling_coordinator_seq,
//             scheduling_coordinator_seq_in: self.scheduling_coordinator_seq_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             scheduling_coordinator_seq_gte: self.scheduling_coordinator_seq_gte,
//             scheduling_coordinator_seq_lte: self.scheduling_coordinator_seq_lte,
//             resource_bid_seq: self.resource_bid_seq,
//             resource_bid_seq_in: self.resource_bid_seq_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             resource_bid_seq_gte: self.resource_bid_seq_gte,
//             resource_bid_seq_lte: self.resource_bid_seq_lte,
//             time_interval_start: self.time_interval_start.clone(),
//             time_interval_start_gte: self.time_interval_start_gte.clone(),
//             time_interval_start_lt: self.time_interval_start_lt.clone(),
//             time_interval_end: self.time_interval_end.clone(),
//             time_interval_end_gte: self.time_interval_end_gte.clone(),
//             time_interval_end_lt: self.time_interval_end_lt.clone(),
//             product_bid_desc: self.product_bid_desc.clone(),
//             product_bid_desc_like: self.product_bid_desc_like.clone(),
//             product_bid_desc_in: self.product_bid_desc_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             product_bid_mrid: self.product_bid_mrid.clone(),
//             product_bid_mrid_like: self.product_bid_mrid_like.clone(),
//             product_bid_mrid_in: self.product_bid_mrid_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             market_product_desc: self.market_product_desc.clone(),
//             market_product_desc_like: self.market_product_desc_like.clone(),
//             market_product_desc_in: self.market_product_desc_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             market_product_type: self.market_product_type.clone(),
//             market_product_type_like: self.market_product_type_like.clone(),
//             market_product_type_in: self.market_product_type_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             self_sched_mw: self.self_sched_mw,
//             self_sched_mw_in: self.self_sched_mw_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             self_sched_mw_gte: self.self_sched_mw_gte.clone(),
//             self_sched_mw_lte: self.self_sched_mw_lte.clone(),
//             sch_bid_time_interval_start: self.sch_bid_time_interval_start.clone(),
//             sch_bid_time_interval_start_gte: self.sch_bid_time_interval_start_gte.clone(),
//             sch_bid_time_interval_start_lt: self.sch_bid_time_interval_start_lt.clone(),
//             sch_bid_time_interval_end: self.sch_bid_time_interval_end.clone(),
//             sch_bid_time_interval_end_gte: self.sch_bid_time_interval_end_gte.clone(),
//             sch_bid_time_interval_end_lt: self.sch_bid_time_interval_end_lt.clone(),
//             sch_bid_xaxis_data: self.sch_bid_xaxis_data.clone(),
//             sch_bid_xaxis_data_in: self.sch_bid_xaxis_data_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             sch_bid_xaxis_data_gte: self.sch_bid_xaxis_data_gte.clone(),
//             sch_bid_xaxis_data_lte: self.sch_bid_xaxis_data_lte.clone(),
//             sch_bid_y1axis_data: self.sch_bid_y1axis_data.clone(),
//             sch_bid_y1axis_data_in: self.sch_bid_y1axis_data_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             sch_bid_y1axis_data_gte: self.sch_bid_y1axis_data_gte.clone(),
//             sch_bid_y1axis_data_lte: self.sch_bid_y1axis_data_lte.clone(),
//             sch_bid_y2axis_data: self.sch_bid_y2axis_data.clone(),
//             sch_bid_y2axis_data_in: self.sch_bid_y2axis_data_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             sch_bid_y2axis_data_gte: self.sch_bid_y2axis_data_gte.clone(),
//             sch_bid_y2axis_data_lte: self.sch_bid_y2axis_data_lte.clone(),
//             sch_bid_curve_type: self.sch_bid_curve_type.clone(),
//             sch_bid_curve_type_in: self.sch_bid_curve_type_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             min_eoh_state_of_charge: self.min_eoh_state_of_charge.clone(),
//             min_eoh_state_of_charge_in: self.min_eoh_state_of_charge_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             min_eoh_state_of_charge_gte: self.min_eoh_state_of_charge_gte.clone(),
//             min_eoh_state_of_charge_lte: self.min_eoh_state_of_charge_lte.clone(),
//             max_eoh_state_of_charge: self.max_eoh_state_of_charge.clone(),
//             max_eoh_state_of_charge_in: self.max_eoh_state_of_charge_in.as_ref().map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
//             max_eoh_state_of_charge_gte: self.max_eoh_state_of_charge_gte.clone(),
//             max_eoh_state_of_charge_lte: self.max_eoh_state_of_charge_lte.clone(),
//             resource_type_like: self.resource_type_like,
//             sch_bid_curve_type_like: todo!(),
//             // ..QueryFilter::default()
//         }
//     }
// }

// fn convert(api_query: &ApiQuery) -> Result<QueryFilter, Box<dyn std::error::Error>> {
//     let builder = QueryFilterBuilder::new();
//     let filter = builder.build();
//     Ok(filter)
// }

/// Convert ApiQuery to QueryFilter
// fn convert(api_query: &ApiQuery) -> Result<QueryFilter, Box<dyn std::error::Error>> {
//     let mut builder = QueryFilterBuilder::new();
//     if let Some(hour_beginning) = api_query.hour_beginning {
//         builder = builder.hour_beginning_eq(hour_beginning);
//     }
//     if let Some(hour_beginning_gte) = api_query.hour_beginning_gte {
//         builder = builder.hour_beginning_gte(hour_beginning_gte);
//     }
//     if let Some(hour_beginning_lt) = api_query.hour_beginning_lt {
//         builder = builder.hour_beginning_lt(hour_beginning_lt);
//     }
//     if let Some(resource_type) = api_query.resource_type {
//         builder = builder.resource_type_eq(resource_type);
//     }
//     if let Some(resource_type_in) = &api_query.resource_type_in {
//         let vec: Vec<ResourceType> = resource_type_in
//             .split(',')
//             .map(|s| s.trim().parse())
//             .collect::<Result<Vec<ResourceType>, _>>()?;
//         builder = builder.resource_type_in(vec);
//     }
// }

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Result};

    use crate::db::prod_db::ProdDb;

    use super::*;

    // #[test]
    // fn test_names() -> Result<()> {
    //     let conn = open_with_retry(
    //         &ProdDb::caiso_public_bids().duckdb_path,
    //         8,
    //         Duration::from_millis(25),
    //         AccessMode::ReadOnly,
    //     )
    //     .unwrap();

    //     // let names = get_all(&conn).unwrap();
    //     // assert!(names.len() >= 110);
    //     Ok(())
    // }

    #[test]
    fn api_get_data() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!("{}/caiso/public_bids", env::var("RUST_SERVER").unwrap(),);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Record> = serde_json::from_str(&response).unwrap();
        assert!(vs.len() > 1000);
        // println!("{:?}", vs.iter().take(5).collect::<Vec<&Row>>());
        Ok(())
    }
}
