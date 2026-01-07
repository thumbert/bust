use duckdb::{
    arrow::array::StringArray, types::EnumType::UInt8, types::ValueRef, Connection, Result,
};
use jiff::civil::Date;
use log::{error, info};
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::path::Path;
use std::process::Command;

use crate::db::isone::lib_isoexpress;
use crate::interval::month::Month;

use crate::api::isone::_api_isone_core::{BidOffer, ResourceType};

#[derive(Clone)]
pub struct IsoneAraBidsOffersArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IsoneAraBidsOffersArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/hbfcmara_"
            + &date.to_string()
            + ".json"
    }

    /// https://webservices.iso-ne.com/api/v1.1/hbfcmara/cp/2025-26/ara/ARA3
    pub fn download_file(&self, date: &Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/hbfcmara/day/{}",
                yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(date)),
            true,
        )
    }

    /// Upload one month to DuckDB.
    /// Assumes all json.gz file exists for DA and RT.  Skips the day if it doesn't exist.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting daily DAAS energy offers files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS offers (
    hour_beginning TIMESTAMPTZ NOT NULL,
    masked_lead_participant_id INTEGER NOT NULL,
    masked_asset_id INTEGER NOT NULL,
    offer_mw DECIMAL(9,2) NOT NULL,
    tmsr_offer_price DECIMAL(9,2) NOT NULL,
    tmnsr_offer_price DECIMAL(9,2) NOT NULL,
    tmor_offer_price DECIMAL(9,2) NOT NULL,
    eir_offer_price DECIMAL(9,2) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT * 
    FROM (
        SELECT unnest(isone_web_services.offer_publishing.day_ahead_ancillary_services.daas_gen_offer_data, recursive := true)
        FROM read_json('{}/Raw/{}/hbdaasenergyoffer_{}-*.json.gz')
    )
    ORDER BY local_day
;

INSERT INTO offers
(
    SELECT 
        local_day::TIMESTAMPTZ as hour_beginning,
        masked_lead_participant_id::INTEGER as masked_lead_participant_id,
        masked_asset_id::INTEGER as masked_asset_id,
        offer_mw::DECIMAL(9,2) as offer_mw,
        tmsr_offer_price::DECIMAL(9,2) as tmsr_offer_price,
        tmnsr_offer_price::DECIMAL(9,2) as tmnsr_offer_price,
        tmor_offer_price::DECIMAL(9,2) as tmor_offer_price,
        eir_offer_price::DECIMAL(9,2) as eir_offer_price
    FROM tmp t
WHERE NOT EXISTS (
        SELECT * FROM offers o
        WHERE
            o.hour_beginning = t.local_day AND
            o.masked_lead_participant_id = t.masked_lead_participant_id AND
            o.masked_asset_id = t.masked_asset_id AND
            o.tmsr_offer_price = t.tmsr_offer_price AND
            o.tmnsr_offer_price = t.tmnsr_offer_price AND
            o.tmor_offer_price = t.tmor_offer_price AND
            o.eir_offer_price = t.eir_offer_price
    )
) ORDER BY hour_beginning, masked_lead_participant_id, masked_asset_id; 
"#,
            self.base_dir,
            month.start_date().year(),
            month
        );
        // println!("{}", sql);

        let output = Command::new("duckdb")
            .arg("-c")
            .arg(&sql)
            .arg(&self.duckdb_path)
            .output()
            .expect("Failed to invoke duckdb command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() {
            info!("{}", stdout);
            info!("done");
        } else {
            error!("Failed to update duckdb for month {}: {}", month, stderr);
        }

        Ok(())
    }
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
    // use crate::db::{isone::masked_data::mra_archive::*, prod_db::ProdDb};
    // use duckdb::{AccessMode, Config, Connection};
    // use std::error::Error;

    // #[test]
    // fn test_get_offers() -> Result<(), Box<dyn Error>> {
    //     let config = Config::default().access_mode(AccessMode::ReadOnly)?;
    //     let conn = Connection::open_with_flags(ProdDb::isone_mra_bids_offers().duckdb_path, config)
    //         .unwrap();
    //     let xs = get_bids_offers(&conn, 202403, 202403).unwrap();
    //     conn.close().unwrap();
    //     let x0 = xs
    //         .iter()
    //         .find(|e| e.month == 202403 && e.masked_asset_id == 10066 && e.segment == 0)
    //         .unwrap();
    //     assert_eq!(x0.quantity, 1.0);
    //     assert_eq!(x0.price, 13.05);
    //     assert_eq!(x0.masked_capacity_zone_id, 8506);
    //     assert_eq!(x0.resource_type, ResourceType::Generating);
    //     assert_eq!(x0.bid_offer, BidOffer::Bid);
    //     Ok(())
    // }
}
