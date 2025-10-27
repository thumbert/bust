// use std::error::Error;

// use duckdb::Connection;
// use jiff::civil::Date;
// use serde::{Deserialize, Serialize};



// #[derive(Clone)]
// pub struct IsoneParticipantsArchive {
//     pub base_dir: String,
//     pub duckdb_path: String,
// }

// #[derive(Debug, PartialEq, Serialize, Deserialize)]
// pub struct Record {
//     as_of: Date,
//     id: i64,
//     name: String,
//     address1: Option<String>,
//     address2: Option<String>,
//     address3: Option<String>,
//     city: Option<String>,
//     state: Option<String>,
//     zip: Option<String>,
//     country: Option<String>,
//     phone: Option<String>,
//     status: ParticipantStatus, 
//     sector: Sector,
//     participant_type: ParticipantType,
//     classification: Classification,
//     sub_classification: Option<String>,
//     has_voting_rights: bool,
//     termination_date: Option<Date>,
// }


// #[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
// pub enum ParticipantStatus {
//     Active,
//     Suspended
// }

// #[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
// pub enum Sector {
//     AlternativeResources,
//     EndUser,
//     Generation,
//     MarketParticipant,
//     NonApplicable,
//     Other,
//     PubliclyOwnedEntity,
//     Transmission,
// }

// #[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
// pub enum ParticipantType {
//     NonParticipant,
//     Participant,
//     PoolOperator,
// }

// #[derive(Copy, Clone, Debug, PartialEq, Serialize, Deserialize)]
// pub enum Classification {
//     GovernanceOnly,
//     GroupMember,
//     LocalControlCenter,
//     MarketParticipant,
//     Other,
//     PublicUtilityCommission,
//     TransmissionOnly,
// }

// pub fn get_participants(conn: &Connection) -> Result<Vec<Record>, Box<dyn Error>> {
//     let query = format!(
//         r#"
// SELECT 
//     id,
//     customer_name,
//     address1,
//     address2,
//     address3,
//     city,   
//     state,
//     zip,
//     country,
//     phone,
//     status,
//     sector,
//     participant_type,
//     classification,
//     sub_classification,
//     has_voting_rights,
//     termination_date
// FROM participants
// WHERE as_of = (
//     SELECT MAX(as_of) FROM participants
// )
// ORDER BY id;
//     "#,
//     );
//     // println!("{}", query);
//     let mut stmt = conn.prepare(&query).unwrap();
//     let offers_iter = stmt.query_map([], |row| {
//         let bid_offer = match row.get_ref_unwrap(6) {
//             ValueRef::Enum(e, idx) => match e {
//                 UInt8(v) => v
//                     .values()
//                     .as_any()
//                     .downcast_ref::<StringArray>()
//                     .unwrap()
//                     .value(v.key(idx).unwrap()),
//                 _ => panic!("Unknown state"),
//             },
//             _ => panic!("Oops, first column should be an enum"),
//         };

//         let resource_type = match row.get_ref_unwrap(3) {
//             ValueRef::Enum(e, idx) => match e {
//                 UInt8(v) => v
//                     .values()
//                     .as_any()
//                     .downcast_ref::<StringArray>()
//                     .unwrap()
//                     .value(v.key(idx).unwrap()),
//                 _ => panic!("Unknown state"),
//             },
//             _ => panic!("Oops, first column should be an enum"),
//         };

//         Ok(Record {
//             month: row.get(0)?,
//             masked_asset_id: row.get(1)?,
//             masked_participant_id: row.get(2)?,
//             resource_type: match resource_type {
//                 "generating" => ResourceType::Generating,
//                 "demand" => ResourceType::Demand,
//                 "import" => ResourceType::Import,
//                 _ => panic!("Unknown resource type {}", resource_type),
//             },
//             masked_capacity_zone_id: row.get(4)?,
//             masked_external_interface_id: row.get(5)?,
//             bid_offer: match bid_offer {
//                 "bid" => BidOffer::Bid,
//                 "offer" => BidOffer::Offer,
//                 _ => panic!("Unknown bid/offer {}", bid_offer),
//             },
//             segment: row.get(7)?,
//             quantity: row.get(8)?,
//             price: row.get(9)?,
//         })
//     })?;
//     let offers: Vec<Record> = offers_iter.map(|e| e.unwrap()).collect();

//     Ok(offers)
// }

// #[cfg(test)]
// mod tests {
//     use duckdb::{AccessMode, Config, Connection};
//     use std::error::Error;
//     use crate::db::{isone::masked_data::mra_archive::*, prod_db::ProdDb};

//     #[test]
//     fn test_get_offers() -> Result<(), Box<dyn Error>> {
//         let config = Config::default().access_mode(AccessMode::ReadOnly)?;
//         let conn = Connection::open_with_flags(ProdDb::isone_mra_bids_offers().duckdb_path, config)
//             .unwrap();
//         let xs = get_bids_offers(&conn, 202403, 202403).unwrap();
//         conn.close().unwrap();
//         let x0 = xs
//             .iter()
//             .find(|e| e.month == 202403 && e.masked_asset_id == 10066 && e.segment == 0)
//             .unwrap();
//         assert_eq!(x0.quantity, 1.0);
//         assert_eq!(x0.price, 13.05);
//         assert_eq!(x0.masked_capacity_zone_id, 8506);
//         assert_eq!(x0.resource_type, ResourceType::Generating);
//         assert_eq!(x0.bid_offer, BidOffer::Bid);
//         Ok(())
//     }
// }
