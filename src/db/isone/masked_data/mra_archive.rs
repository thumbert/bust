use duckdb::{
    arrow::array::StringArray, types::EnumType::UInt8, types::ValueRef, Connection, Result,
};
use serde::{Deserialize, Serialize};

use crate::api::isone::_api_isone_core::{BidOffer, ResourceType};



#[derive(Clone)]
pub struct IsoneMraBidsOffersArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    month: u32,
    masked_asset_id: u32,
    masked_participant_id: u32,
    resource_type: ResourceType,
    masked_capacity_zone_id: u32,
    masked_external_interface_id: Option<u32>,
    bid_offer: BidOffer,
    segment: u8,
    quantity: f32,
    price: f32,
}

// Get the MRA bids/offers between a [start, end] date for a list of units and participant ids
pub fn get_bids_offers(conn: &Connection, start: u32, end: u32) -> Result<Vec<Record>> {
    let query = format!(
        r#"
SELECT month, 
    maskedResourceId,
    maskedParticipantId,
    resourceType,
    maskedCapacityZoneId,
    maskedExternalInterfaceId,
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
        let bid_offer = match row.get_ref_unwrap(6) {
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

        let resource_type = match row.get_ref_unwrap(3) {
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
            masked_participant_id: row.get(2)?,
            resource_type: match resource_type {
                "generating" => ResourceType::Generating,
                "demand" => ResourceType::Demand,
                "import" => ResourceType::Import,
                _ => panic!("Unknown resource type {}", resource_type),
            },
            masked_capacity_zone_id: row.get(4)?,
            masked_external_interface_id: row.get(5)?,
            bid_offer: match bid_offer {
                "bid" => BidOffer::Bid,
                "offer" => BidOffer::Offer,
                _ => panic!("Unknown bid/offer {}", bid_offer),
            },
            segment: row.get(7)?,
            quantity: row.get(8)?,
            price: row.get(9)?,
        })
    })?;
    let offers: Vec<Record> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

#[cfg(test)]
mod tests {
    use duckdb::{AccessMode, Config, Connection};
    use std::error::Error;
    use crate::db::{isone::masked_data::mra_archive::*, prod_db::ProdDb};

    #[test]
    fn test_get_offers() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::isone_mra_bids_offers().duckdb_path, config)
            .unwrap();
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
