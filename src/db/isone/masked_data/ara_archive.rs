// Auto-generated Rust stub for DuckDB table: bids_offers
// Created on 2026-01-11 with Dart package reduct

use std::collections::HashMap;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use convert_case::{Case, Casing};
use rust_decimal::Decimal;
use std::str::FromStr;

use jiff::civil::date;
use jiff::{ToSpan, Zoned};
use log::{error, info};
use std::error::Error;
use std::fmt;
use std::path::Path;
use std::process::Command;

use crate::db::isone::lib_isoexpress;

pub struct CapabilityYear(Zoned);

impl CapabilityYear {
    pub fn with_start_year(year: i16) -> Self {
        let zoned = date(year, 6, 1).at(0, 0, 0, 0).in_tz("America/New_York");
        Self(zoned.unwrap())
    }

    pub fn start(&self) -> &Zoned {
        &self.0
    }

    pub fn end(&self) -> Zoned {
        self.0.saturating_add(1.year())
    }

    pub fn fca_name(&self) -> String {
        format!("FCA{}", self.0.year() - 2000 + 1)
    }
}

impl fmt::Display for CapabilityYear {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.0.year(), self.end().strftime("%y"))
    }
}

#[derive(Clone)]
pub struct IsoneAraBidsOffersArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IsoneAraBidsOffersArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, capability_year: &CapabilityYear, ara: AuctionType) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &capability_year.to_string()
            + "/hbfcmara_"
            + &capability_year.to_string()
            + "_"
            + &ara.to_string()
            + ".json"
    }

    /// Data avaliable only after the capability period has started!
    /// https://webservices.iso-ne.com/api/v1.1/hbfcmara/cp/2025-26/ara/ARA1
    pub fn download_file(
        &self,
        capability_year: &CapabilityYear,
        ara: AuctionType,
    ) -> Result<(), Box<dyn Error>> {
        lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/hbfcmara/cp/{}/ara/{}",
                capability_year, ara
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(capability_year, ara)),
            true,
        )
    }

    /// Upload one file to DuckDB.
    ///  
    pub fn update_duckdb(
        &self,
        capability_year: &CapabilityYear,
        ara: AuctionType,
    ) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting ARA bids/offers files for period {}, {} ...",
            capability_year, ara
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS bids_offers (
    capability_period VARCHAR NOT NULL,
    auction_type ENUM('ARA1', 'ARA2', 'ARA3') NOT NULL,
    masked_resource_id UINTEGER NOT NULL,
    masked_participant_id UINTEGER NOT NULL,
    masked_capacity_zone_id USMALLINT NOT NULL,
    resource_type ENUM('Import', 'Generating', 'Demand') NOT NULL,
    bid_offer ENUM('Demand_Bid', 'Supply_Offer') NOT NULL,
    segment UTINYINT NOT NULL,
    quantity DECIMAL(9,4) NOT NULL,
    price DECIMAL(9,4) NOT NULL
);
CREATE TEMPORARY TABLE tmp AS (
PIVOT (
    SELECT 
        COLUMNS(* EXCLUDE (pq)),
        string_split(pq, '_')[1]::VARCHAR AS variable,
        string_split(pq, '_')[2]::UTINYINT AS segment,
    FROM (
        UNPIVOT (
            SELECT 
                json_extract(aux, '$.Cp')::VARCHAR as capability_period,
                json_extract_string(aux, '$.AucType')::ENUM('ARA1', 'ARA2', 'ARA3') AS auction_type,
                json_extract(aux, '$.MaskResID')::UINTEGER as masked_resource_id,
                json_extract(aux, '$.MaskLPID')::UINTEGER as  masked_participant_id,
                json_extract(aux, '$.MaskCZID')::USMALLINT as masked_capacity_zone_id,
                json_extract_string(aux, '$.ResType')::ENUM('Import', 'Generating', 'Demand') as resource_type,
                json_extract_string(aux, '$.BidType')::ENUM('Demand_Bid', 'Supply_Offer') as bid_offer,
                json_extract(aux, '$.Seg1Mw')::DECIMAL(9,4) AS quantity_1,
                json_extract(aux, '$.Seg2Mw')::DECIMAL(9,4) AS quantity_2,
                json_extract(aux, '$.Seg3Mw')::DECIMAL(9,4) AS quantity_3,
                json_extract(aux, '$.Seg4Mw')::DECIMAL(9,4) AS quantity_4,
                json_extract(aux, '$.Seg5Mw')::DECIMAL(9,4) AS quantity_5,
                json_extract(aux, '$.Seg1Price')::DECIMAL(9,4) AS price_1,
                json_extract(aux, '$.Seg2Price')::DECIMAL(9,4) AS price_2,
                json_extract(aux, '$.Seg3Price')::DECIMAL(9,4) AS price_3,
                json_extract(aux, '$.Seg4Price')::DECIMAL(9,4) AS price_4,
                json_extract(aux, '$.Seg5Price')::DECIMAL(9,4) AS price_5
            FROM (
                SELECT unnest(Hbfcmaras.Hbfcmara)::JSON as aux
                FROM read_json(
                    '{}/Raw/{}/hbfcmara_{}_{}.json.gz'
                )
            )
        ) ON price_1, quantity_1,
        price_2, quantity_2,
        price_3, quantity_3,
        price_4, quantity_4,
        price_5, quantity_5
        INTO 
            NAME pq
            VALUE value
    )
) ON variable
USING first(value)
);

INSERT INTO bids_offers
    SELECT * FROM tmp t
WHERE NOT EXISTS (
    SELECT * FROM bids_offers b
    WHERE
        b.capability_period = t.capability_period
        AND b.auction_type = t.auction_type
        AND b.masked_resource_id = t.masked_resource_id
        AND b.masked_participant_id = t.masked_participant_id
        AND b.masked_capacity_zone_id = t.masked_capacity_zone_id
        AND b.resource_type = t.resource_type
        AND b.bid_offer = t.bid_offer
        AND b.segment = t.segment
);
"#,
            self.base_dir, capability_year, capability_year, ara
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
            error!(
                "Failed to update DuckDB for capability year {} and ARA {}: {}",
                capability_year, ara, stderr
            );
        }

        Ok(())
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub capability_period: String,
    pub auction_type: AuctionType,
    pub masked_resource_id: u32,
    pub masked_participant_id: u32,
    pub masked_capacity_zone_id: u16,
    pub resource_type: ResourceType,
    pub bid_offer: BidOffer,
    pub segment: u8,
    #[serde(with = "rust_decimal::serde::float")]
    pub quantity: Decimal,
    #[serde(with = "rust_decimal::serde::float")]
    pub price: Decimal,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum AuctionType {
    Ara1,
    Ara2,
    Ara3,
}

impl std::str::FromStr for AuctionType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "ARA_1" => Ok(AuctionType::Ara1),
            "ARA_2" => Ok(AuctionType::Ara2),
            "ARA_3" => Ok(AuctionType::Ara3),
            _ => Err(format!("Invalid value for AuctionType: {}", s)),
        }
    }
}

impl std::fmt::Display for AuctionType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            AuctionType::Ara1 => write!(f, "ARA1"),
            AuctionType::Ara2 => write!(f, "ARA2"),
            AuctionType::Ara3 => write!(f, "ARA3"),
        }
    }
}

impl serde::Serialize for AuctionType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            AuctionType::Ara1 => "ARA1",
            AuctionType::Ara2 => "ARA2",
            AuctionType::Ara3 => "ARA3",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for AuctionType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        AuctionType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResourceType {
    Demand,
    Generating,
    Import,
}

impl std::str::FromStr for ResourceType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "DEMAND" => Ok(ResourceType::Demand),
            "GENERATING" => Ok(ResourceType::Generating),
            "IMPORT" => Ok(ResourceType::Import),
            _ => Err(format!("Invalid value for ResourceType: {}", s)),
        }
    }
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ResourceType::Demand => write!(f, "Demand"),
            ResourceType::Generating => write!(f, "Generating"),
            ResourceType::Import => write!(f, "Import"),
        }
    }
}

impl serde::Serialize for ResourceType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            ResourceType::Demand => "Demand",
            ResourceType::Generating => "Generating",
            ResourceType::Import => "Import",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for ResourceType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ResourceType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum BidOffer {
    DemandBid,
    SupplyOffer,
}

impl std::str::FromStr for BidOffer {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "DEMAND_BID" => Ok(BidOffer::DemandBid),
            "SUPPLY_OFFER" => Ok(BidOffer::SupplyOffer),
            _ => Err(format!("Invalid value for BidOffer: {}", s)),
        }
    }
}

impl std::fmt::Display for BidOffer {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BidOffer::DemandBid => write!(f, "Demand_Bid"),
            BidOffer::SupplyOffer => write!(f, "Supply_Offer"),
        }
    }
}

impl serde::Serialize for BidOffer {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            BidOffer::DemandBid => "Demand_Bid",
            BidOffer::SupplyOffer => "Supply_Offer",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for BidOffer {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        BidOffer::from_str(&s).map_err(serde::de::Error::custom)
    }
}

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
    limit: Option<usize>,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    capability_period,
    auction_type,
    masked_resource_id,
    masked_participant_id,
    masked_capacity_zone_id,
    resource_type,
    bid_offer,
    segment,
    quantity,
    price
FROM bids_offers WHERE 1=1"#,
    );
    if let Some(capability_period) = &query_filter.capability_period {
        query.push_str(&format!(
            "
    AND capability_period = '{}'",
            capability_period
        ));
    }
    if let Some(capability_period_like) = &query_filter.capability_period_like {
        query.push_str(&format!(
            "
    AND capability_period LIKE '{}'",
            capability_period_like
        ));
    }
    if let Some(capability_period_in) = &query_filter.capability_period_in {
        query.push_str(&format!(
            "
    AND capability_period IN ('{}')",
            capability_period_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(auction_type) = &query_filter.auction_type {
        query.push_str(&format!(
            "
    AND auction_type = '{}'",
            auction_type
        ));
    }
    if let Some(auction_type_in) = &query_filter.auction_type_in {
        query.push_str(&format!(
            "
    AND auction_type IN ('{}')",
            auction_type_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(masked_resource_id) = &query_filter.masked_resource_id {
        query.push_str(&format!(
            "
    AND masked_resource_id = {}",
            masked_resource_id
        ));
    }
    if let Some(masked_resource_id_in) = &query_filter.masked_resource_id_in {
        query.push_str(&format!(
            "
    AND masked_resource_id IN ({})",
            masked_resource_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(masked_resource_id_gte) = &query_filter.masked_resource_id_gte {
        query.push_str(&format!(
            "
    AND masked_resource_id >= {}",
            masked_resource_id_gte
        ));
    }
    if let Some(masked_resource_id_lte) = &query_filter.masked_resource_id_lte {
        query.push_str(&format!(
            "
    AND masked_resource_id <= {}",
            masked_resource_id_lte
        ));
    }
    if let Some(masked_participant_id) = &query_filter.masked_participant_id {
        query.push_str(&format!(
            "
    AND masked_participant_id = {}",
            masked_participant_id
        ));
    }
    if let Some(masked_participant_id_in) = &query_filter.masked_participant_id_in {
        query.push_str(&format!(
            "
    AND masked_participant_id IN ({})",
            masked_participant_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(masked_participant_id_gte) = &query_filter.masked_participant_id_gte {
        query.push_str(&format!(
            "
    AND masked_participant_id >= {}",
            masked_participant_id_gte
        ));
    }
    if let Some(masked_participant_id_lte) = &query_filter.masked_participant_id_lte {
        query.push_str(&format!(
            "
    AND masked_participant_id <= {}",
            masked_participant_id_lte
        ));
    }
    if let Some(masked_capacity_zone_id) = &query_filter.masked_capacity_zone_id {
        query.push_str(&format!(
            "
    AND masked_capacity_zone_id = {}",
            masked_capacity_zone_id
        ));
    }
    if let Some(masked_capacity_zone_id_in) = &query_filter.masked_capacity_zone_id_in {
        query.push_str(&format!(
            "
    AND masked_capacity_zone_id IN ({})",
            masked_capacity_zone_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(masked_capacity_zone_id_gte) = &query_filter.masked_capacity_zone_id_gte {
        query.push_str(&format!(
            "
    AND masked_capacity_zone_id >= {}",
            masked_capacity_zone_id_gte
        ));
    }
    if let Some(masked_capacity_zone_id_lte) = &query_filter.masked_capacity_zone_id_lte {
        query.push_str(&format!(
            "
    AND masked_capacity_zone_id <= {}",
            masked_capacity_zone_id_lte
        ));
    }
    if let Some(resource_type) = &query_filter.resource_type {
        query.push_str(&format!(
            "
    AND resource_type = '{}'",
            resource_type
        ));
    }
    if let Some(resource_type_in) = &query_filter.resource_type_in {
        query.push_str(&format!(
            "
    AND resource_type IN ('{}')",
            resource_type_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(bid_offer) = &query_filter.bid_offer {
        query.push_str(&format!(
            "
    AND bid_offer = '{}'",
            bid_offer
        ));
    }
    if let Some(bid_offer_in) = &query_filter.bid_offer_in {
        query.push_str(&format!(
            "
    AND bid_offer IN ('{}')",
            bid_offer_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(segment) = &query_filter.segment {
        query.push_str(&format!(
            "
    AND segment = {}",
            segment
        ));
    }
    if let Some(segment_in) = &query_filter.segment_in {
        query.push_str(&format!(
            "
    AND segment IN ({})",
            segment_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(segment_gte) = &query_filter.segment_gte {
        query.push_str(&format!(
            "
    AND segment >= {}",
            segment_gte
        ));
    }
    if let Some(segment_lte) = &query_filter.segment_lte {
        query.push_str(&format!(
            "
    AND segment <= {}",
            segment_lte
        ));
    }
    if let Some(quantity) = &query_filter.quantity {
        query.push_str(&format!(
            "
    AND quantity = {}",
            quantity
        ));
    }
    if let Some(quantity_in) = &query_filter.quantity_in {
        query.push_str(&format!(
            "
    AND quantity IN ({})",
            quantity_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(quantity_gte) = &query_filter.quantity_gte {
        query.push_str(&format!(
            "
    AND quantity >= {}",
            quantity_gte
        ));
    }
    if let Some(quantity_lte) = &query_filter.quantity_lte {
        query.push_str(&format!(
            "
    AND quantity <= {}",
            quantity_lte
        ));
    }
    if let Some(price) = &query_filter.price {
        query.push_str(&format!(
            "
    AND price = {}",
            price
        ));
    }
    if let Some(price_in) = &query_filter.price_in {
        query.push_str(&format!(
            "
    AND price IN ({})",
            price_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(price_gte) = &query_filter.price_gte {
        query.push_str(&format!(
            "
    AND price >= {}",
            price_gte
        ));
    }
    if let Some(price_lte) = &query_filter.price_lte {
        query.push_str(&format!(
            "
    AND price <= {}",
            price_lte
        ));
    }
    match limit {
        Some(l) => {
            query.push_str(&format!(
                "
LIMIT {};",
                l
            ));
        }
        None => {
            query.push(';');
        }
    }

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let capability_period: String = row.get::<usize, String>(0)?;
        let _n1 = match row.get_ref_unwrap(1).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum auction_type"),
        };
        let auction_type = AuctionType::from_str(&_n1).unwrap();
        let masked_resource_id: u32 = row.get::<usize, u32>(2)?;
        let masked_participant_id: u32 = row.get::<usize, u32>(3)?;
        let masked_capacity_zone_id: u16 = row.get::<usize, u16>(4)?;
        let _n5 = match row.get_ref_unwrap(5).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum resource_type"),
        };
        let resource_type = ResourceType::from_str(&_n5).unwrap();
        let _n6 = match row.get_ref_unwrap(6).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum bid_offer"),
        };
        let bid_offer = BidOffer::from_str(&_n6).unwrap();
        let segment: u8 = row.get::<usize, u8>(7)?;
        let quantity: Decimal = match row.get_ref_unwrap(8) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let price: Decimal = match row.get_ref_unwrap(9) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Record {
            capability_period,
            auction_type,
            masked_resource_id,
            masked_participant_id,
            masked_capacity_zone_id,
            resource_type,
            bid_offer,
            segment,
            quantity,
            price,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub capability_period: Option<String>,
    pub capability_period_like: Option<String>,
    pub capability_period_in: Option<Vec<String>>,
    pub auction_type: Option<AuctionType>,
    pub auction_type_in: Option<Vec<AuctionType>>,
    pub masked_resource_id: Option<u32>,
    pub masked_resource_id_in: Option<Vec<u32>>,
    pub masked_resource_id_gte: Option<u32>,
    pub masked_resource_id_lte: Option<u32>,
    pub masked_participant_id: Option<u32>,
    pub masked_participant_id_in: Option<Vec<u32>>,
    pub masked_participant_id_gte: Option<u32>,
    pub masked_participant_id_lte: Option<u32>,
    pub masked_capacity_zone_id: Option<u16>,
    pub masked_capacity_zone_id_in: Option<Vec<u16>>,
    pub masked_capacity_zone_id_gte: Option<u16>,
    pub masked_capacity_zone_id_lte: Option<u16>,
    pub resource_type: Option<ResourceType>,
    pub resource_type_in: Option<Vec<ResourceType>>,
    pub bid_offer: Option<BidOffer>,
    pub bid_offer_in: Option<Vec<BidOffer>>,
    pub segment: Option<u8>,
    pub segment_in: Option<Vec<u8>>,
    pub segment_gte: Option<u8>,
    pub segment_lte: Option<u8>,
    pub quantity: Option<Decimal>,
    pub quantity_in: Option<Vec<Decimal>>,
    pub quantity_gte: Option<Decimal>,
    pub quantity_lte: Option<Decimal>,
    pub price: Option<Decimal>,
    pub price_in: Option<Vec<Decimal>>,
    pub price_gte: Option<Decimal>,
    pub price_lte: Option<Decimal>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.capability_period {
            params.insert("capability_period", value.to_string());
        }
        if let Some(value) = &self.capability_period_like {
            params.insert("capability_period_like", value.to_string());
        }
        if let Some(value) = &self.capability_period_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("capability_period_in", joined);
        }
        if let Some(value) = &self.auction_type {
            params.insert("auction_type", value.to_string());
        }
        if let Some(value) = &self.auction_type_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("auction_type_in", joined);
        }
        if let Some(value) = &self.masked_resource_id {
            params.insert("masked_resource_id", value.to_string());
        }
        if let Some(value) = &self.masked_resource_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("masked_resource_id_in", joined);
        }
        if let Some(value) = &self.masked_resource_id_gte {
            params.insert("masked_resource_id_gte", value.to_string());
        }
        if let Some(value) = &self.masked_resource_id_lte {
            params.insert("masked_resource_id_lte", value.to_string());
        }
        if let Some(value) = &self.masked_participant_id {
            params.insert("masked_participant_id", value.to_string());
        }
        if let Some(value) = &self.masked_participant_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("masked_participant_id_in", joined);
        }
        if let Some(value) = &self.masked_participant_id_gte {
            params.insert("masked_participant_id_gte", value.to_string());
        }
        if let Some(value) = &self.masked_participant_id_lte {
            params.insert("masked_participant_id_lte", value.to_string());
        }
        if let Some(value) = &self.masked_capacity_zone_id {
            params.insert("masked_capacity_zone_id", value.to_string());
        }
        if let Some(value) = &self.masked_capacity_zone_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("masked_capacity_zone_id_in", joined);
        }
        if let Some(value) = &self.masked_capacity_zone_id_gte {
            params.insert("masked_capacity_zone_id_gte", value.to_string());
        }
        if let Some(value) = &self.masked_capacity_zone_id_lte {
            params.insert("masked_capacity_zone_id_lte", value.to_string());
        }
        if let Some(value) = &self.resource_type {
            params.insert("resource_type", value.to_string());
        }
        if let Some(value) = &self.resource_type_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("resource_type_in", joined);
        }
        if let Some(value) = &self.bid_offer {
            params.insert("bid_offer", value.to_string());
        }
        if let Some(value) = &self.bid_offer_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("bid_offer_in", joined);
        }
        if let Some(value) = &self.segment {
            params.insert("segment", value.to_string());
        }
        if let Some(value) = &self.segment_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("segment_in", joined);
        }
        if let Some(value) = &self.segment_gte {
            params.insert("segment_gte", value.to_string());
        }
        if let Some(value) = &self.segment_lte {
            params.insert("segment_lte", value.to_string());
        }
        if let Some(value) = &self.quantity {
            params.insert("quantity", value.to_string());
        }
        if let Some(value) = &self.quantity_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("quantity_in", joined);
        }
        if let Some(value) = &self.quantity_gte {
            params.insert("quantity_gte", value.to_string());
        }
        if let Some(value) = &self.quantity_lte {
            params.insert("quantity_lte", value.to_string());
        }
        if let Some(value) = &self.price {
            params.insert("price", value.to_string());
        }
        if let Some(value) = &self.price_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("price_in", joined);
        }
        if let Some(value) = &self.price_gte {
            params.insert("price_gte", value.to_string());
        }
        if let Some(value) = &self.price_lte {
            params.insert("price_lte", value.to_string());
        }
        form_urlencoded::Serializer::new(String::new())
            .extend_pairs(&params)
            .finish()
    }
}

#[derive(Default)]
pub struct QueryFilterBuilder {
    inner: QueryFilter,
}

impl QueryFilterBuilder {
    pub fn new() -> Self {
        Self {
            inner: QueryFilter::default(),
        }
    }

    pub fn build(self) -> QueryFilter {
        self.inner
    }

    pub fn capability_period<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.capability_period = Some(value.into());
        self
    }

    pub fn capability_period_like(mut self, value_like: String) -> Self {
        self.inner.capability_period_like = Some(value_like);
        self
    }

    pub fn capability_period_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.capability_period_in = Some(values_in);
        self
    }

    pub fn auction_type(mut self, value: AuctionType) -> Self {
        self.inner.auction_type = Some(value);
        self
    }

    pub fn auction_type_in(mut self, values_in: Vec<AuctionType>) -> Self {
        self.inner.auction_type_in = Some(values_in);
        self
    }

    pub fn masked_resource_id(mut self, value: u32) -> Self {
        self.inner.masked_resource_id = Some(value);
        self
    }

    pub fn masked_resource_id_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.masked_resource_id_in = Some(values_in);
        self
    }

    pub fn masked_resource_id_gte(mut self, value: u32) -> Self {
        self.inner.masked_resource_id_gte = Some(value);
        self
    }

    pub fn masked_resource_id_lte(mut self, value: u32) -> Self {
        self.inner.masked_resource_id_lte = Some(value);
        self
    }

    pub fn masked_participant_id(mut self, value: u32) -> Self {
        self.inner.masked_participant_id = Some(value);
        self
    }

    pub fn masked_participant_id_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.masked_participant_id_in = Some(values_in);
        self
    }

    pub fn masked_participant_id_gte(mut self, value: u32) -> Self {
        self.inner.masked_participant_id_gte = Some(value);
        self
    }

    pub fn masked_participant_id_lte(mut self, value: u32) -> Self {
        self.inner.masked_participant_id_lte = Some(value);
        self
    }

    pub fn masked_capacity_zone_id(mut self, value: u16) -> Self {
        self.inner.masked_capacity_zone_id = Some(value);
        self
    }

    pub fn masked_capacity_zone_id_in(mut self, values_in: Vec<u16>) -> Self {
        self.inner.masked_capacity_zone_id_in = Some(values_in);
        self
    }

    pub fn masked_capacity_zone_id_gte(mut self, value: u16) -> Self {
        self.inner.masked_capacity_zone_id_gte = Some(value);
        self
    }

    pub fn masked_capacity_zone_id_lte(mut self, value: u16) -> Self {
        self.inner.masked_capacity_zone_id_lte = Some(value);
        self
    }

    pub fn resource_type(mut self, value: ResourceType) -> Self {
        self.inner.resource_type = Some(value);
        self
    }

    pub fn resource_type_in(mut self, values_in: Vec<ResourceType>) -> Self {
        self.inner.resource_type_in = Some(values_in);
        self
    }

    pub fn bid_offer(mut self, value: BidOffer) -> Self {
        self.inner.bid_offer = Some(value);
        self
    }

    pub fn bid_offer_in(mut self, values_in: Vec<BidOffer>) -> Self {
        self.inner.bid_offer_in = Some(values_in);
        self
    }

    pub fn segment(mut self, value: u8) -> Self {
        self.inner.segment = Some(value);
        self
    }

    pub fn segment_in(mut self, values_in: Vec<u8>) -> Self {
        self.inner.segment_in = Some(values_in);
        self
    }

    pub fn segment_gte(mut self, value: u8) -> Self {
        self.inner.segment_gte = Some(value);
        self
    }

    pub fn segment_lte(mut self, value: u8) -> Self {
        self.inner.segment_lte = Some(value);
        self
    }

    pub fn quantity(mut self, value: Decimal) -> Self {
        self.inner.quantity = Some(value);
        self
    }

    pub fn quantity_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.quantity_in = Some(values_in);
        self
    }

    pub fn quantity_gte(mut self, value: Decimal) -> Self {
        self.inner.quantity_gte = Some(value);
        self
    }

    pub fn quantity_lte(mut self, value: Decimal) -> Self {
        self.inner.quantity_lte = Some(value);
        self
    }

    pub fn price(mut self, value: Decimal) -> Self {
        self.inner.price = Some(value);
        self
    }

    pub fn price_in(mut self, values_in: Vec<Decimal>) -> Self {
        self.inner.price_in = Some(values_in);
        self
    }

    pub fn price_gte(mut self, value: Decimal) -> Self {
        self.inner.price_gte = Some(value);
        self
    }

    pub fn price_lte(mut self, value: Decimal) -> Self {
        self.inner.price_lte = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use crate::db::prod_db::ProdDb;
    use duckdb::{AccessMode, Config, Connection};
    use std::error::Error;

    use super::*;

    #[test]
    fn download_file_test() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_masked_ara_bids_offers();
        let capability_year = CapabilityYear::with_start_year(2023);
        archive.download_file(&capability_year, AuctionType::Ara1)?;
        archive.download_file(&capability_year, AuctionType::Ara2)?;
        archive.download_file(&capability_year, AuctionType::Ara3)?;
        Ok(())
    }

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::isone_masked_ara_bids_offers().duckdb_path, config)
                .unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }
}
