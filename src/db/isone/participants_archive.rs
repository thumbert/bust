// Auto-generated Rust stub for DuckDB table: participants
// Created on 2025-10-27 with elec_server/utils/lib_duckdb_builder.dart

use duckdb::Connection;
use serde::{Deserialize, Serialize};

use jiff::{civil::Date, ToSpan};
use url::form_urlencoded;
use std::{collections::HashMap, str::FromStr};
use convert_case::{Case, Casing};

#[derive(Clone)]
pub struct IsoneParticipantsArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub as_of: Date,
    pub id: i64,
    pub customer_name: String,
    pub address1: Option<String>,
    pub address2: Option<String>,
    pub address3: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub zip: Option<String>,
    pub country: Option<String>,
    pub phone: Option<String>,
    pub status: Status,
    pub sector: Sector,
    pub participant_type: ParticipantType,
    pub classification: Classification,
    pub sub_classification: Option<String>,
    pub has_voting_rights: Option<bool>,
    pub termination_date: Option<Date>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Status {
    Active,
    Suspended,
}

impl std::str::FromStr for Status {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "ACTIVE" => Ok(Status::Active),
            "SUSPENDED" => Ok(Status::Suspended),
            _ => Err(format!("Invalid value for Status: {}", s)),
        }
    }
}

impl std::fmt::Display for Status {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Status::Active => write!(f, "ACTIVE"),
            Status::Suspended => write!(f, "SUSPENDED"),
        }
    }
}

impl serde::Serialize for Status {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            Status::Active => "ACTIVE",
            Status::Suspended => "SUSPENDED",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for Status {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Status::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Sector {
    AlternativeResources,
    EndUser,
    Generation,
    MarketParticipant,
    NotApplicable,
    PubliclyOwnedEntity,
    Supplier,
    Transmission,
}

impl std::str::FromStr for Sector {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "ALTERNATIVE_RESOURCES" => Ok(Sector::AlternativeResources),
            "END_USER" => Ok(Sector::EndUser),
            "GENERATION" => Ok(Sector::Generation),
            "MARKET_PARTICIPANT" => Ok(Sector::MarketParticipant),
            "NOT_APPLICABLE" => Ok(Sector::NotApplicable),
            "PUBLICLY_OWNED_ENTITY" => Ok(Sector::PubliclyOwnedEntity),
            "SUPPLIER" => Ok(Sector::Supplier),
            "TRANSMISSION" => Ok(Sector::Transmission),
            _ => Err(format!("Invalid value for Sector: {}", s)),
        }
    }
}

impl std::fmt::Display for Sector {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Sector::AlternativeResources => write!(f, "Alternative Resources"),
            Sector::EndUser => write!(f, "End User"),
            Sector::Generation => write!(f, "Generation"),
            Sector::MarketParticipant => write!(f, "Market Participant"),
            Sector::NotApplicable => write!(f, "Not applicable"),
            Sector::PubliclyOwnedEntity => write!(f, "Publicly-Owned Entity"),
            Sector::Supplier => write!(f, "Supplier"),
            Sector::Transmission => write!(f, "Transmission"),
        }
    }
}

impl serde::Serialize for Sector {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            Sector::AlternativeResources => "Alternative Resources",
            Sector::EndUser => "End User",
            Sector::Generation => "Generation",
            Sector::MarketParticipant => "Market Participant",
            Sector::NotApplicable => "Not applicable",
            Sector::PubliclyOwnedEntity => "Publicly-Owned Entity",
            Sector::Supplier => "Supplier",
            Sector::Transmission => "Transmission",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for Sector {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Sector::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ParticipantType {
    NonParticipant,
    Participant,
    PoolOperator,
}

impl std::str::FromStr for ParticipantType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "NON_PARTICIPANT" => Ok(ParticipantType::NonParticipant),
            "PARTICIPANT" => Ok(ParticipantType::Participant),
            "POOL_OPERATOR" => Ok(ParticipantType::PoolOperator),
            _ => Err(format!("Invalid value for ParticipantType: {}", s)),
        }
    }
}

impl std::fmt::Display for ParticipantType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ParticipantType::NonParticipant => write!(f, "Non-Participant"),
            ParticipantType::Participant => write!(f, "Participant"),
            ParticipantType::PoolOperator => write!(f, "Pool Operator"),
        }
    }
}

impl serde::Serialize for ParticipantType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            ParticipantType::NonParticipant => "Non-Participant",
            ParticipantType::Participant => "Participant",
            ParticipantType::PoolOperator => "Pool Operator",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for ParticipantType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        ParticipantType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Classification {
    GovernanceOnly,
    GroupMember,
    LocalControlCenter,
    MarketParticipant,
    Other,
    PublicUtilityCommission,
    TransmissionOnly,
}

impl std::str::FromStr for Classification {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "GOVERNANCE_ONLY" => Ok(Classification::GovernanceOnly),
            "GROUP_MEMBER" => Ok(Classification::GroupMember),
            "LOCAL_CONTROL_CENTER" => Ok(Classification::LocalControlCenter),
            "MARKET_PARTICIPANT" => Ok(Classification::MarketParticipant),
            "OTHER" => Ok(Classification::Other),
            "PUBLIC_UTILITY_COMMISSION" => Ok(Classification::PublicUtilityCommission),
            "TRANSMISSION_ONLY" => Ok(Classification::TransmissionOnly),
            _ => Err(format!("Invalid value for Classification: {}", s)),
        }
    }
}

impl std::fmt::Display for Classification {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Classification::GovernanceOnly => write!(f, "Governance Only"),
            Classification::GroupMember => write!(f, "Group Member"),
            Classification::LocalControlCenter => write!(f, "Local Control Center"),
            Classification::MarketParticipant => write!(f, "Market Participant"),
            Classification::Other => write!(f, "Other"),
            Classification::PublicUtilityCommission => write!(f, "Public Utility Commission"),
            Classification::TransmissionOnly => write!(f, "Transmission Only"),
        }
    }
}

impl serde::Serialize for Classification {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            Classification::GovernanceOnly => "Governance Only",
            Classification::GroupMember => "Group Member",
            Classification::LocalControlCenter => "Local Control Center",
            Classification::MarketParticipant => "Market Participant",
            Classification::Other => "Other",
            Classification::PublicUtilityCommission => "Public Utility Commission",
            Classification::TransmissionOnly => "Transmission Only",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for Classification {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Classification::from_str(&s).map_err(serde::de::Error::custom)
    }
}

pub fn get_data(conn: &Connection, query_filter: &QueryFilter, limit: Option<usize>) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
   let mut query = String::from(r#"
SELECT
    as_of,
    id,
    customer_name,
    address1,
    address2,
    address3,
    city,
    state,
    zip,
    country,
    phone,
    status,
    sector,
    participant_type,
    classification,
    sub_classification,
    has_voting_rights,
    termination_date
FROM participants WHERE 1=1"#);
    if let Some(status) = &query_filter.status {
        query.push_str(&format!("
    AND status = '{}'", status));
    }
    if let Some(status_in) = &query_filter.status_in {
        query.push_str(&format!("
    AND status IN ('{}')", status_in.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("','")));
    }
    match limit {
        Some(l) => {
            query.push_str(&format!("
LIMIT {};", l));
        },
        None => {
            query.push(';');
        },
    }

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let _n0 = 719528 + row.get::<usize, i32>(0)?;
        let as_of = Date::ZERO + _n0.days();
        let id: i64 = row.get::<usize, i64>(1)?;
        let customer_name: String = row.get::<usize, String>(2)?;
        let address1: Option<String> = row.get::<usize, Option<String>>(3)?;
        let address2: Option<String> = row.get::<usize, Option<String>>(4)?;
        let address3: Option<String> = row.get::<usize, Option<String>>(5)?;
        let city: Option<String> = row.get::<usize, Option<String>>(6)?;
        let state: Option<String> = row.get::<usize, Option<String>>(7)?;
        let zip: Option<String> = row.get::<usize, Option<String>>(8)?;
        let country: Option<String> = row.get::<usize, Option<String>>(9)?;
        let phone: Option<String> = row.get::<usize, Option<String>>(10)?;
        let _n11 = match row.get_ref_unwrap(11).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum status"),
        };
        let status = Status::from_str(&_n11).unwrap();
        let _n12 = match row.get_ref_unwrap(12).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum sector"),
        };
        let sector = Sector::from_str(&_n12).unwrap();
        let _n13 = match row.get_ref_unwrap(13).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum participant_type"),
        };
        let participant_type = ParticipantType::from_str(&_n13).unwrap();
        let _n14 = match row.get_ref_unwrap(14).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum classification"),
        };
        let classification = Classification::from_str(&_n14).unwrap();
        let sub_classification: Option<String> = row.get::<usize, Option<String>>(15)?;
        let has_voting_rights: Option<bool> = row.get::<usize, Option<bool>>(16)?;
        let termination_date = row
            .get::<usize, Option<i32>>(17)?
            .map(|n| {Date::ZERO + (719528 + n).days() });
        Ok(Record {
            as_of,
            id,
            customer_name,
            address1,
            address2,
            address3,
            city,
            state,
            zip,
            country,
            phone,
            status,
            sector,
            participant_type,
            classification,
            sub_classification,
            has_voting_rights,
            termination_date,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub status: Option<Status>,
    pub status_in: Option<Vec<Status>>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.status {
            params.insert("status", value.to_string());
        }
        if let Some(value) = &self.status_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("status_in", joined);
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

    pub fn status(mut self, value: Status) -> Self {
        self.inner.status = Some(value);
        self
    }

    pub fn status_in(mut self, values_in: Vec<Status>) -> Self {
        self.inner.status_in = Some(values_in);
        self
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::prod_db::ProdDb;
    use duckdb::{AccessMode, Config, Connection};
    use std::error::Error;
    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::isone_participants_archive().duckdb_path, config)
                .unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 590);
        Ok(())
    }
}
