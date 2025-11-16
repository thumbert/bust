use std::{
    fmt::{self},
    str::FromStr,
};

use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{
    arrow::array::StringArray, types::EnumType::UInt8, types::ValueRef, AccessMode, Config,
    Connection, Result,
};
use itertools::Itertools;
use jiff::{civil::Date, Timestamp, ToSpan, Zoned};
use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    api::isone::_api_isone_core::{deserialize_zoned_assume_ny, serialize_zoned_as_offset},
    db::isone::masked_data::demand_bids_archive::DemandBidsArchive,
    elec::iso::ISONE,
};

#[derive(Debug, Deserialize)]
struct OffersQuery {
    /// One or more masked asset ids, separated by commas
    /// If asset_ids are not specified, return all of them.  Use carefully
    /// because it's a lot of data...
    masked_asset_ids: Option<String>,

    masked_participant_ids: Option<i32>,

    /// One or more bid types, separated by commas
    /// Valid types are: INC, DEC, FIXED, PRICE.  If not specified, return all.
    bid_types: Option<String>,
}

/// Get DA demand bids + virtuals between a start/end date
#[get("/isone/demand_bids/da/start/{start}/end/{end}")]
async fn api_offers(
    path: web::Path<(Date, Date)>,
    query: web::Query<OffersQuery>,
    db: web::Data<DemandBidsArchive>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(db.duckdb_path.clone(), config).unwrap();

    let start_date = path.0;
    let end_date = path.1;
    let asset_ids: Option<Vec<i32>> = query
        .masked_asset_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.parse::<i32>().unwrap()).collect());

    let bid_types: Option<Vec<BidType>> = query
        .bid_types
        .as_ref()
        .map(|ids| {
            ids.split(',')
                .map(|e| e.parse::<BidType>().unwrap())
                .collect()
        });

    let offers = get_demand_bids(
        &conn,
        start_date,
        end_date,
        query.masked_participant_ids,
        asset_ids,
        bid_types,
    )
    .unwrap();
    HttpResponse::Ok().json(offers)
}

#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum BidType {
    Inc,
    Dec,
    Fixed,
    PriceSensitive,
}

impl fmt::Display for BidType {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            BidType::Inc => write!(f, "INC"),
            BidType::Dec => write!(f, "DEC"),
            BidType::Fixed => write!(f, "FIXED"),
            BidType::PriceSensitive => write!(f, "PRICE"),
        }
    }
}

impl FromStr for BidType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "INC" => Ok(BidType::Inc),
            "DEC" => Ok(BidType::Dec),
            "FIXED" => Ok(BidType::Fixed),
            "PRICE" | "PRICESENSITIVE" => Ok(BidType::PriceSensitive),
            _ => Err(format!("Can't parse bid type: {}", s)),
        }
    }
}

// Custom deserializer using FromStr so that Actix path path can parse different casing, e.g.
// "fixed" and "Fixed", not only the canonical one "FIXED".
impl<'de> Deserialize<'de> for BidType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        BidType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum LocationType {
    Hub,
    LoadZone,
    NetworkNode,
}

impl FromStr for LocationType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "HUB" => Ok(LocationType::Hub),
            "LOAD ZONE" => Ok(LocationType::LoadZone),
            "NETWORK NODE" => Ok(LocationType::NetworkNode),
            _ => Err(format!("Can't parse location type: {}", s)),
        }
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub struct DemandBid {
    masked_participant_id: u32,
    masked_asset_id: u32,
    bid_type: BidType,
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
        deserialize_with = "deserialize_zoned_assume_ny"
    )]
    hour_beginning: Zoned,
    segment: u8,
    quantity: f32,
    price: f32,
}

/// Get the demand bids between a [start, end] date for a list of units
/// (or all units)
pub fn get_demand_bids(
    conn: &Connection,
    start: Date,
    end: Date,
    masked_participant_ids: Option<i32>,
    masked_asset_ids: Option<Vec<i32>>,
    bid_types: Option<Vec<BidType>>,
) -> Result<Vec<DemandBid>> {
    let mut query = format!(
        r#"
SELECT 
    MaskedParticipantId,
    MaskedAssetId, 
    BidType,
    HourBeginning,
    Segment,
    Price,
    MW AS Quantity
FROM da_bids
WHERE HourBeginning >= '{}'
AND HourBeginning < '{}'"#,
        start
            .in_tz("America/New_York")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        end.in_tz("America/New_York")
            .unwrap()
            .checked_add(1.day())
            .ok()
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
    );
    if let Some(ids) = masked_participant_ids {
        query.push_str(&format!("\nAND \"MaskedParticipantId\" = {} ", ids));
    }
    if let Some(types) = bid_types {
        let types: Vec<String> = types.iter().map(|e| format!("'{}'", e)).collect();
        query.push_str(&format!(
            "\nAND \"BidType\" in ({}) ",
            types.iter().join(", ")
        ));
    }
    if let Some(ids) = masked_asset_ids {
        query.push_str(&format!(
            "\nAND \"MaskedAssetId\" in ({}) ",
            ids.iter().join(", ")
        ));
    }
    query.push_str("\nORDER BY \"MaskedAssetId\", \"HourBeginning\";");
    // println!("{}", query);

    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let bid_type = match row.get_ref_unwrap(2) {
            ValueRef::Enum(e, idx) => match e {
                UInt8(v) => v
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(v.key(idx).unwrap()),
                _ => panic!("Unknown unit status"),
            },
            _ => panic!("Oops, column should be an enum"),
        };
        let micro: i64 = row.get(3).unwrap();
        Ok(DemandBid {
            masked_participant_id: row.get(0).unwrap(),
            masked_asset_id: row.get(1).unwrap(),
            bid_type: bid_type.parse::<BidType>().unwrap(),
            hour_beginning: Zoned::new(
                Timestamp::from_microsecond(micro).unwrap(),
                ISONE.tz.clone(),
            ),
            segment: row.get(4)?,
            price: row.get(5)?,
            quantity: row.get(6)?,
        })
    })?;
    let offers: Vec<DemandBid> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::date;

    use crate::{api::isone::masked_demand_bids::*, db::prod_db::ProdDb};

    #[test]
    fn test_get_offers() -> Result<()> {
        let archive = ProdDb::isone_masked_demand_bids();
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(archive.duckdb_path, config).unwrap();
        let xs = get_demand_bids(
            &conn,
            date(2025, 6, 1),
            date(2025, 6, 1),
            Some(504170),
            None,
            Some(vec![BidType::PriceSensitive, BidType::Fixed]),
        )
        .unwrap();
        println!("Found {:?}", xs.first().unwrap());
        // conn.close().unwrap();
        let x0 = xs
            .iter()
            .find(|&x| {
                x.masked_asset_id == 28934
                    && x.bid_type == BidType::PriceSensitive
                    && x.segment == 0
            })
            .unwrap();
        // println!("{:?}", x0);
        assert_eq!(
            *x0,
            DemandBid {
                masked_participant_id: 504170,
                masked_asset_id: 28934,
                bid_type: BidType::PriceSensitive,
                hour_beginning: "2025-06-01 00:00:00-04:00[America/New_York]"
                    .parse()
                    .unwrap(),
                segment: 0,
                quantity: 19.6,
                price: 992.0,
            }
        );
        Ok(())
    }

    #[test]
    fn api_demand_bids() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/demand_bids/da/start/2025-06-01/end/2025-06-01?masked_asset_ids=28934&masked_participant_ids=504170",
            env::var("RUST_SERVER").unwrap(),
        );
        // println!("URL: {}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let xs: Vec<DemandBid> = serde_json::from_str(&response).unwrap();
        assert_eq!(xs.len(), 360);

        Ok(())
    }
}
