use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{
    arrow::array::StringArray, types::EnumType::UInt8, types::ValueRef, AccessMode, Config,
    Connection, Result,
};
use serde::Serialize;

#[get("/isone/capacity/mra/bids_offers/start/{start}/end/{end}")]
async fn bids_offers(path: web::Path<(String, String)>) -> impl Responder {
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
    let bids_offers = get_bids_offers(&conn, start, end).unwrap();
    HttpResponse::Ok().json(bids_offers)
}

#[derive(Debug, PartialEq, Serialize)]
enum BidOffer {
    Bid,
    Offer,
}

#[derive(Debug, PartialEq, Serialize)]
enum ResourceType {
    Generating,
    Demand,
    Import,
}

#[derive(Debug, PartialEq, Serialize)]
pub struct Record {
    month: u32,
    masked_asset_id: u32,
    resource_type: ResourceType,
    masked_capacity_zone_id: u32,
    bid_offer: BidOffer,
    segment: u8,
    quantity: f32,
    price: f32,
}

// Get the energy offers between a [start, end] date for a list of units and participant ids
pub fn get_bids_offers(conn: &Connection, start: u32, end: u32) -> Result<Vec<Record>> {
    let query = format!(
        r#"
SELECT month, 
    maskedResourceId,
    resourceType,
    maskedCapacityZoneId,
    bidOffer,
    segment,
    quantity,
    price, 
FROM bids_offers
WHERE month >= {} 
AND month <= {}
ORDER BY month;    
    "#,
        start, end,
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let bid_offer = match row.get_ref_unwrap(4) {
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

        let resource_type = match row.get_ref_unwrap(2) {
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

        Ok(Record {
            month: row.get(0)?,
            masked_asset_id: row.get(1)?,
            resource_type: match resource_type {
                "generating" => ResourceType::Generating,
                "demand" => ResourceType::Demand,
                "import" => ResourceType::Import,
                _ => panic!("Unknown resource type {}", resource_type),
            }, 
            masked_capacity_zone_id: row.get(3)?,
            bid_offer: match bid_offer {
                "bid" => BidOffer::Bid,
                "offer" => BidOffer::Offer,
                _ => panic!("Unknown bid/offer {}", bid_offer),
            },
            segment: row.get(5)?,
            quantity: row.get(6)?,
            price: row.get(7)?,
        })
    })?;
    let offers: Vec<Record> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}


fn get_path() -> String {
    "/home/adrian/Downloads/Archive/IsoExpress/Capacity/mra.duckdb".to_string()
}

#[cfg(test)]
mod tests {
    use duckdb::{AccessMode, Config, Connection, Result};

    use crate::api::isone::capacity::monthly_capacity_bidsoffers::*;

    #[test]
    fn test_get_offers() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let xs = get_bids_offers(&conn, 202403, 202403).unwrap();
        conn.close().unwrap();
        let x0 = xs
            .iter()
            .find(|e| e.month == 202403 && e.masked_asset_id == 10066 && e.segment == 0)
            .unwrap();
        assert_eq!(x0.quantity, 1.0);
        assert_eq!(x0.price, 13.05);
        assert_eq!(x0.masked_capacity_zone_id, 8506);
        assert_eq!(x0.resource_type, ResourceType::Generating);
        assert_eq!(x0.bid_offer, BidOffer::Bid);
        Ok(())
    }
}
