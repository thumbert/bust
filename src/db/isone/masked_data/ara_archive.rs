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
    capacity_period VARCHAR NOT NULL, 
    auction_type ENUM('ARA1', 'ARA2', 'ARA3') NOT NULL,
    masked_resource_id UINTEGER NOT NULL,
    masked_participant_id UINTEGER NOT NULL,
    masked_capacity_zone_id UINTEGER NOT NULL,
    masked_interface_id UINTEGER,
    resource_type ENUM('Generating', 'Demand', 'Import') NOT NULL,
    bid_type ENUM('Demand_Bid', 'Supply_Offer') NOT NULL,
    segment UTINYINT NOT NULL, -- 0-4
    mw DECIMAL(9,4) NOT NULL,
    price DECIMAL(9,4) NOT NULL
); 

CREATE TEMPORARY TABLE tmp AS
    SELECT 
        cp AS capacity_period,
        AucType AS auction_type,
        MaskResId AS masked_resource_id,
        MaskLPID AS masked_participant_id,
        MaskCzid AS masked_capacity_zone_id,
        MaskIntfcId AS masked_interface_id,
        ResType AS resource_type,
        BidType AS bid_type,
        Seg1Mw,
        Seg1Price,
        Seg2Mw,
        Seg2Price,
        Seg3Mw,
        Seg3Price,
        Seg4Mw,
        Seg4Price,
        Seg5Mw,
        Seg5Price
    FROM (
        SELECT unnest(Hbfcmaras.Hbfcmara, recursive := true)
        FROM read_json('{}/Raw/{}/hbfcmara_{}_{}.json.gz')
    )
;

--- transpose the segments into rows, and filter out the nulls
CREATE TEMPORARY TABLE tmp1 AS (
    SELECT
        w.capacity_period,
        w.auction_type,
        w.masked_resource_id,
        w.masked_capacity_zone_id,
        w.masked_participant_id,
        w.masked_interface_id,
        w.resource_type,
        w.bid_type,
        v.seg_num AS segment,
        v.mw,
        v.price
    FROM tmp w
    CROSS JOIN LATERAL (
        VALUES
            (0, w.Seg1Mw, w.Seg1Price),
            (1, w.Seg2Mw, w.Seg2Price),
            (2, w.Seg3Mw, w.Seg3Price),
            (3, w.Seg4Mw, w.Seg4Price),
            (4, w.Seg5Mw, w.Seg5Price)

    ) AS v(seg_num, mw, price)
    WHERE v.mw IS NOT NULL
    ORDER BY w.capacity_period, w.auction_type, w.masked_resource_id, w.bid_type, v.seg_num
);


INSERT INTO bids_offers
(SELECT * FROM tmp1
WHERE NOT EXISTS (
        SELECT * FROM bids_offers d
        WHERE d.capacity_period = tmp1.capacity_period
        AND d.auction_type = tmp1.auction_type
        AND d.masked_resource_id = tmp1.masked_resource_id
        AND d.segment = tmp1.segment
        AND d.bid_type = tmp1.bid_type
    )
)
ORDER BY capacity_period, auction_type, masked_resource_id, bid_type, segment;
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
    pub capacity_period: String,
    pub auction_type: AuctionType,
    pub masked_resource_id: u32,
    pub masked_participant_id: u32,
    pub masked_capacity_zone_id: u32,
    pub masked_interface_id: Option<u32>,
    pub resource_type: ResourceType,
    pub bid_type: BidType,
    pub segment: u8,
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
pub enum BidType {
    DemandBid,
    SupplyOffer,
}

impl std::str::FromStr for BidType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "DEMAND_BID" => Ok(BidType::DemandBid),
            "SUPPLY_OFFER" => Ok(BidType::SupplyOffer),
            _ => Err(format!("Invalid value for BidType: {}", s)),
        }
    }
}

impl std::fmt::Display for BidType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            BidType::DemandBid => write!(f, "Demand_Bid"),
            BidType::SupplyOffer => write!(f, "Supply_Offer"),
        }
    }
}

impl serde::Serialize for BidType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            BidType::DemandBid => "Demand_Bid",
            BidType::SupplyOffer => "Supply_Offer",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for BidType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        BidType::from_str(&s).map_err(serde::de::Error::custom)
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
    capacity_period,
    auction_type,
    masked_resource_id,
    masked_participant_id,
    masked_capacity_zone_id,
    masked_interface_id,
    resource_type,
    bid_type,
    segment,
    price
FROM bids_offers WHERE 1=1"#,
    );
    if let Some(capacity_period) = &query_filter.capacity_period {
        query.push_str(&format!(
            "
    AND capacity_period = '{}'",
            capacity_period
        ));
    }
    if let Some(capacity_period_like) = &query_filter.capacity_period_like {
        query.push_str(&format!(
            "
    AND capacity_period LIKE '{}'",
            capacity_period_like
        ));
    }
    if let Some(capacity_period_in) = &query_filter.capacity_period_in {
        query.push_str(&format!(
            "
    AND capacity_period IN ('{}')",
            capacity_period_in
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
    if let Some(masked_interface_id) = &query_filter.masked_interface_id {
        query.push_str(&format!(
            "
    AND masked_interface_id = {}",
            masked_interface_id
        ));
    }
    if let Some(masked_interface_id_in) = &query_filter.masked_interface_id_in {
        query.push_str(&format!(
            "
    AND masked_interface_id IN ({})",
            masked_interface_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(masked_interface_id_gte) = &query_filter.masked_interface_id_gte {
        query.push_str(&format!(
            "
    AND masked_interface_id >= {}",
            masked_interface_id_gte
        ));
    }
    if let Some(masked_interface_id_lte) = &query_filter.masked_interface_id_lte {
        query.push_str(&format!(
            "
    AND masked_interface_id <= {}",
            masked_interface_id_lte
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
    if let Some(bid_type) = &query_filter.bid_type {
        query.push_str(&format!(
            "
    AND bid_type = '{}'",
            bid_type
        ));
    }
    if let Some(bid_type_in) = &query_filter.bid_type_in {
        query.push_str(&format!(
            "
    AND bid_type IN ('{}')",
            bid_type_in
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
        let capacity_period: String = row.get::<usize, String>(0)?;
        let _n1 = match row.get_ref_unwrap(1).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum auction_type"),
        };
        let auction_type = AuctionType::from_str(&_n1).unwrap();
        let masked_resource_id: u32 = row.get::<usize, u32>(2)?;
        let masked_participant_id: u32 = row.get::<usize, u32>(3)?;
        let masked_capacity_zone_id: u32 = row.get::<usize, u32>(4)?;
        let masked_interface_id: Option<u32> = row.get::<usize, Option<u32>>(5)?;
        let _n6 = match row.get_ref_unwrap(6).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum resource_type"),
        };
        let resource_type = ResourceType::from_str(&_n6).unwrap();
        let _n7 = match row.get_ref_unwrap(7).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum bid_type"),
        };
        let bid_type = BidType::from_str(&_n7).unwrap();
        let segment: u8 = row.get::<usize, u8>(8)?;
        let price: Decimal = match row.get_ref_unwrap(9) {
            duckdb::types::ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Record {
            capacity_period,
            auction_type,
            masked_resource_id,
            masked_participant_id,
            masked_capacity_zone_id,
            masked_interface_id,
            resource_type,
            bid_type,
            segment,
            price,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub capacity_period: Option<String>,
    pub capacity_period_like: Option<String>,
    pub capacity_period_in: Option<Vec<String>>,
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
    pub masked_capacity_zone_id: Option<u32>,
    pub masked_capacity_zone_id_in: Option<Vec<u32>>,
    pub masked_capacity_zone_id_gte: Option<u32>,
    pub masked_capacity_zone_id_lte: Option<u32>,
    pub masked_interface_id: Option<u32>,
    pub masked_interface_id_in: Option<Vec<u32>>,
    pub masked_interface_id_gte: Option<u32>,
    pub masked_interface_id_lte: Option<u32>,
    pub resource_type: Option<ResourceType>,
    pub resource_type_in: Option<Vec<ResourceType>>,
    pub bid_type: Option<BidType>,
    pub bid_type_in: Option<Vec<BidType>>,
    pub segment: Option<u8>,
    pub segment_in: Option<Vec<u8>>,
    pub segment_gte: Option<u8>,
    pub segment_lte: Option<u8>,
    pub price: Option<Decimal>,
    pub price_in: Option<Vec<Decimal>>,
    pub price_gte: Option<Decimal>,
    pub price_lte: Option<Decimal>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.capacity_period {
            params.insert("capacity_period", value.to_string());
        }
        if let Some(value) = &self.capacity_period_like {
            params.insert("capacity_period_like", value.to_string());
        }
        if let Some(value) = &self.capacity_period_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("capacity_period_in", joined);
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
        if let Some(value) = &self.masked_interface_id {
            params.insert("masked_interface_id", value.to_string());
        }
        if let Some(value) = &self.masked_interface_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("masked_interface_id_in", joined);
        }
        if let Some(value) = &self.masked_interface_id_gte {
            params.insert("masked_interface_id_gte", value.to_string());
        }
        if let Some(value) = &self.masked_interface_id_lte {
            params.insert("masked_interface_id_lte", value.to_string());
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
        if let Some(value) = &self.bid_type {
            params.insert("bid_type", value.to_string());
        }
        if let Some(value) = &self.bid_type_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("bid_type_in", joined);
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

    pub fn capacity_period<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.capacity_period = Some(value.into());
        self
    }

    pub fn capacity_period_like(mut self, value_like: String) -> Self {
        self.inner.capacity_period_like = Some(value_like);
        self
    }

    pub fn capacity_period_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.capacity_period_in = Some(values_in);
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

    pub fn masked_capacity_zone_id(mut self, value: u32) -> Self {
        self.inner.masked_capacity_zone_id = Some(value);
        self
    }

    pub fn masked_capacity_zone_id_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.masked_capacity_zone_id_in = Some(values_in);
        self
    }

    pub fn masked_capacity_zone_id_gte(mut self, value: u32) -> Self {
        self.inner.masked_capacity_zone_id_gte = Some(value);
        self
    }

    pub fn masked_capacity_zone_id_lte(mut self, value: u32) -> Self {
        self.inner.masked_capacity_zone_id_lte = Some(value);
        self
    }

    pub fn masked_interface_id(mut self, value: u32) -> Self {
        self.inner.masked_interface_id = Some(value);
        self
    }

    pub fn masked_interface_id_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.masked_interface_id_in = Some(values_in);
        self
    }

    pub fn masked_interface_id_gte(mut self, value: u32) -> Self {
        self.inner.masked_interface_id_gte = Some(value);
        self
    }

    pub fn masked_interface_id_lte(mut self, value: u32) -> Self {
        self.inner.masked_interface_id_lte = Some(value);
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

    pub fn bid_type(mut self, value: BidType) -> Self {
        self.inner.bid_type = Some(value);
        self
    }

    pub fn bid_type_in(mut self, values_in: Vec<BidType>) -> Self {
        self.inner.bid_type_in = Some(values_in);
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
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();

        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_masked_ara_bids_offers();
        let capability_year = CapabilityYear::with_start_year(2025);
        let auctions = vec![AuctionType::Ara1, AuctionType::Ara2, AuctionType::Ara3];
        for auction in auctions {
            // archive.download_file(&capability_year, auction)?;
            archive.update_duckdb(&capability_year, auction)?;
        }
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
