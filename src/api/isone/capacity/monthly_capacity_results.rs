use duckdb::{
    arrow::array::StringArray, types::EnumType::UInt8, types::ValueRef, AccessMode, Config,
    Connection, Result,
};
// use r2d2::PooledConnection;
// use r2d2_duckdb::DuckDBConnectionManager;

use actix_web::{get, web, HttpResponse, Responder};
use serde::Serialize;

#[get("/isone/capacity/mra/bids_offers/participant_ids")]
async fn participant_ids() -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let ids = get_participant_ids(conn);
    HttpResponse::Ok().json(ids)
}

#[get("/isone/capacity/mra/results/interface/start/{start}/end/{end}")]
async fn results_interface(path: web::Path<(String, String)>) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let start = match path.0.replace('-', "").parse::<u32>() {
        Ok(v) => v,
        Err(e) => return HttpResponse::BadRequest().body(format!("Invalid start month. {}", e)),
    };
    let end = match path.1.replace('-', "").parse::<u32>() {
        Ok(v) => v,
        Err(e) => return HttpResponse::BadRequest().body(format!("Invalid end month. {}", e)),
    };
    let res = get_results_interface(conn, start, end).unwrap();
    HttpResponse::Ok().json(res)
}

#[get("/isone/capacity/mra/results/zone/start/{start}/end/{end}")]
async fn results_zone(path: web::Path<(String, String)>) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let start = match path.0.replace('-', "").parse::<u32>() {
        Ok(v) => v,
        Err(e) => return HttpResponse::BadRequest().body(format!("Invalid start month. {}", e)),
    };
    let end = match path.1.replace('-', "").parse::<u32>() {
        Ok(v) => v,
        Err(e) => return HttpResponse::BadRequest().body(format!("Invalid end month. {}", e)),
    };
    let res = get_results_zone(conn, start, end).unwrap();
    HttpResponse::Ok().json(res)
}

#[derive(Debug, PartialEq, Serialize)]
enum CapacityZoneType {
    Rop,
    Export,
    Import,
}

#[derive(Debug, PartialEq, Serialize)]
struct ZoneResult {
    month: u32,
    capacity_zone_id: u32,
    capacity_zone_type: CapacityZoneType,
    capacity_zone_name: String,
    supply_offers_submitted: f32,
    demand_bids_submitted: f32,
    supply_offers_cleared: f32,
    demand_bids_cleared: f32,
    net_capacity_cleared: f32,
    clearing_price: f32,
}

#[derive(Debug, PartialEq, Serialize)]
struct InterfaceResult {
    month: u32,
    external_interface_id: u32,
    external_interface_name: String,
    supply_offers_submitted: f32,
    demand_bids_submitted: f32,
    supply_offers_cleared: f32,
    demand_bids_cleared: f32,
    net_capacity_cleared: f32,
    clearing_price: f32,
}

/// Get MRA zonal clearing results between a start and end month
fn get_results_zone(conn: Connection, start_month: u32, end_month: u32) -> Result<Vec<ZoneResult>> {
    let query = format!(
        r#"
SELECT month,
    capacityZoneId,
    capacityZoneType,
    capacityZoneName,
    supplyOffersSubmitted,
    demandBidsSubmitted,
    supplyOffersCleared,
    demandBidsCleared,
    netCapacityCleared,
    clearingPrice
FROM results_zone 
WHERE month >= {}
AND month <= {};    
    "#,
        start_month, end_month,
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        let capacity_zone_type = match row.get_ref_unwrap(2) {
            ValueRef::Enum(e, idx) => match e {
                UInt8(v) => v
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(v.key(idx).unwrap()),
                _ => panic!("Unknown state"),
            },
            _ => panic!("Oops, first column should be an enum"),
        };

        Ok(ZoneResult {
            month: row.get(0)?,
            capacity_zone_id: row.get(1)?,
            capacity_zone_type: match capacity_zone_type {
                "ROP" => CapacityZoneType::Rop,
                "Export" => CapacityZoneType::Export,
                "Import" => CapacityZoneType::Import,
                _ => panic!("Unknown capacity zone type {}", capacity_zone_type),
            },
            capacity_zone_name: row.get(3)?,
            supply_offers_submitted: row.get(4)?,
            demand_bids_submitted: row.get(5)?,
            supply_offers_cleared: row.get(6)?,
            demand_bids_cleared: row.get(7)?,
            net_capacity_cleared: row.get(8)?,
            clearing_price: row.get(9)?,
        })
    })?;
    let res: Vec<ZoneResult> = res_iter.map(|e| e.unwrap()).collect();

    Ok(res)
}

/// Get MRA interface clearing results between a start and end month
fn get_results_interface(
    conn: Connection,
    start_month: u32,
    end_month: u32,
) -> Result<Vec<InterfaceResult>> {
    let query = format!(
        r#"
SELECT month,
    externalInterfaceId,
    externalInterfaceName,
    supplyOffersSubmitted,
    demandBidsSubmitted,
    supplyOffersCleared,
    demandBidsCleared,
    netCapacityCleared,
    clearingPrice
FROM results_interface 
WHERE month >= {}
AND month <= {};    
    "#,
        start_month, end_month,
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        Ok(InterfaceResult {
            month: row.get(0)?,
            external_interface_id: row.get(1)?,
            external_interface_name: row.get(2)?,
            supply_offers_submitted: row.get(3)?,
            demand_bids_submitted: row.get(4)?,
            supply_offers_cleared: row.get(5)?,
            demand_bids_cleared: row.get(6)?,
            net_capacity_cleared: row.get(7)?,
            clearing_price: row.get(8)?,
        })
    })?;
    let res: Vec<InterfaceResult> = res_iter.map(|e| e.unwrap()).collect();

    Ok(res)
}

fn get_participant_ids(conn: Connection) -> Vec<i64> {
    let mut stmt = conn
        .prepare("SELECT DISTINCT maskedParticipantId from bids_offers")
        .unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut ids: Vec<i64> = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        ids.push(row.get(0).unwrap());
    }
    ids
}

fn get_path() -> String {
    "/home/adrian/Downloads/Archive/IsoExpress/Capacity/mra.duckdb".to_string()
}

#[cfg(test)]
mod tests {

    use crate::api::isone::capacity::monthly_capacity_results::*;
    use duckdb::{AccessMode, Config, Connection, Result};

    #[test]
    fn test_get_results_zone() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let data = get_results_zone(conn, 202401, 202403).unwrap();
        assert!(data.len() >= 12);
        Ok(())
    }

    #[test]
    fn test_get_results_interface() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let data = get_results_interface(conn, 202401, 202403).unwrap();
        let sene = data
            .iter()
            .find(|e| e.month == 202401 && e.external_interface_name == "New York AC Ties")
            .unwrap();
        assert_eq!(sene.clearing_price, 3.938);
        assert_eq!(data.len(), 15);
        Ok(())
    }

    #[test]
    fn test_participant_ids() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let ids = get_participant_ids(conn);
        assert!(ids.len() >= 107);
        Ok(())
    }
}
