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

    let query_filter = convert(&query.into_inner());
    let query_filter = match query_filter {
        Ok(qf) => qf,
        Err(e) => {
            return HttpResponse::BadRequest().body(format!("Invalid query parameters: {}", e));
        }
    };

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

fn convert(api_query: &ApiQuery) -> Result<QueryFilter, Box<dyn std::error::Error>> {
    let builder = QueryFilterBuilder::new();
    let filter = builder.build();
    Ok(filter)
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Result};

    use crate::db::prod_db::ProdDb;

    use super::*;

    #[test]
    fn test_names() -> Result<()> {
        let conn = open_with_retry(
            &ProdDb::caiso_dalmp().duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        )
        .unwrap();
        // let names = get_all(&conn).unwrap();
        // assert!(names.len() >= 110);
        Ok(())
    }

    // #[test]
    // fn api_status() -> Result<(), reqwest::Error> {
    //     dotenvy::from_path(Path::new(".env/test.env")).unwrap();
    //     let url = format!("{}/caiso/node_table/all", env::var("RUST_SERVER").unwrap(),);
    //     let response = reqwest::blocking::get(url)?.text()?;
    //     let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
    //     assert!(vs.len() > 1000);
    //     // println!("{:?}", vs.iter().take(5).collect::<Vec<&Row>>());
    //     Ok(())
    // }
}
