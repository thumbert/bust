// Auto-generated Rust stub for DuckDB table: participants
// Created on 2025-10-27 with elec_server/utils/lib_duckdb_builder.dart

use duckdb::Connection;
use serde::{Deserialize, Serialize};

use jiff::{civil::Date, ToSpan};
use std::str::FromStr;

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

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum Status {
    Active,
    Suspended,
}

impl std::str::FromStr for Status {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ACTIVE" => Ok(Status::Active),
            "SUSPENDED" => Ok(Status::Suspended),
            _ => Err(()),
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

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
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
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Alternative Resources" => Ok(Sector::AlternativeResources),
            "End User" => Ok(Sector::EndUser),
            "Generation" => Ok(Sector::Generation),
            "Market Participant" => Ok(Sector::MarketParticipant),
            "Not applicable" => Ok(Sector::NotApplicable),
            "Publicly-Owned Entity" => Ok(Sector::PubliclyOwnedEntity),
            "Supplier" => Ok(Sector::Supplier),
            "Transmission" => Ok(Sector::Transmission),
            _ => Err(()),
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

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
pub enum ParticipantType {
    NonParticipant,
    Participant,
    PoolOperator,
}

impl std::str::FromStr for ParticipantType {
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Non-Participant" => Ok(ParticipantType::NonParticipant),
            "Participant" => Ok(ParticipantType::Participant),
            "Pool Operator" => Ok(ParticipantType::PoolOperator),
            _ => Err(()),
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

#[derive(Clone, Copy, Debug, PartialEq, Serialize, Deserialize)]
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
    type Err = ();
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Governance Only" => Ok(Classification::GovernanceOnly),
            "Group Member" => Ok(Classification::GroupMember),
            "Local Control Center" => Ok(Classification::LocalControlCenter),
            "Market Participant" => Ok(Classification::MarketParticipant),
            "Other" => Ok(Classification::Other),
            "Public Utility Commission" => Ok(Classification::PublicUtilityCommission),
            "Transmission Only" => Ok(Classification::TransmissionOnly),
            _ => Err(()),
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

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
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
FROM participants WHERE 1=1
   "#,
    );
    if let Some(as_of) = query_filter.as_of {
        query.push_str(&format!("AND as_of = '{}'", as_of));
    }
    if let Some(as_of_gte) = query_filter.as_of_gte {
        query.push_str(&format!("AND as_of_gte >= '{}'", as_of_gte));
    }
    if let Some(as_of_lte) = query_filter.as_of_lte {
        query.push_str(&format!("AND as_of_lte <= '{}'", as_of_lte));
    }
    if let Some(id) = query_filter.id {
        query.push_str(&format!("AND id = '{}'", id));
    }
    if let Some(customer_name) = &query_filter.customer_name {
        query.push_str(&format!("AND customer_name = '{}'", customer_name));
    }
    if let Some(address1) = &query_filter.address1 {
        query.push_str(&format!("AND address1 = '{}'", address1));
    }
    if let Some(address2) = &query_filter.address2 {
        query.push_str(&format!("AND address2 = '{}'", address2));
    }
    if let Some(address3) = &query_filter.address3 {
        query.push_str(&format!("AND address3 = '{}'", address3));
    }
    if let Some(city) = &query_filter.city {
        query.push_str(&format!("AND city = '{}'", city));
    }
    if let Some(state) = &query_filter.state {
        query.push_str(&format!("AND state = '{}'", state));
    }
    if let Some(zip) = &query_filter.zip {
        query.push_str(&format!("AND zip = '{}'", zip));
    }
    if let Some(country) = &query_filter.country {
        query.push_str(&format!("AND country = '{}'", country));
    }
    if let Some(phone) = &query_filter.phone {
        query.push_str(&format!("AND phone = '{}'", phone));
    }
    if let Some(status) = query_filter.status {
        query.push_str(&format!("AND status = '{}'", status));
    }
    if let Some(sector) = query_filter.sector {
        query.push_str(&format!("AND sector = '{}'", sector));
    }
    if let Some(participant_type) = query_filter.participant_type {
        query.push_str(&format!("AND participant_type = '{}'", participant_type));
    }
    if let Some(classification) = query_filter.classification {
        query.push_str(&format!("AND classification = '{}'", classification));
    }
    if let Some(sub_classification) = &query_filter.sub_classification {
        query.push_str(&format!(
            "AND sub_classification = '{}'",
            sub_classification
        ));
    }
    if let Some(has_voting_rights) = query_filter.has_voting_rights {
        query.push_str(&format!("AND has_voting_rights = '{}'", has_voting_rights));
    }
    if let Some(termination_date) = query_filter.termination_date {
        query.push_str(&format!("AND termination_date = '{}'", termination_date));
    }
    if let Some(termination_date_gte) = query_filter.termination_date_gte {
        query.push_str(&format!(
            "AND termination_date_gte >= '{}'",
            termination_date_gte
        ));
    }
    if let Some(termination_date_lte) = query_filter.termination_date_lte {
        query.push_str(&format!(
            "AND termination_date_lte <= '{}'",
            termination_date_lte
        ));
    }
    query.push(';');
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
            _ => panic!("Unexpected value type for enum"),
        };
        let status = Status::from_str(&_n11).unwrap();
        let _n12 = match row.get_ref_unwrap(12).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            _ => panic!("Unexpected value type for enum"),
        };
        let sector = Sector::from_str(&_n12).unwrap();
        let _n13 = match row.get_ref_unwrap(13).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            _ => panic!("Unexpected value type for enum"),
        };
        let participant_type = ParticipantType::from_str(&_n13).unwrap();
        let _n14 = match row.get_ref_unwrap(14).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            _ => panic!("Unexpected value type for enum"),
        };
        let classification = Classification::from_str(&_n14).unwrap();
        let sub_classification: Option<String> = row.get::<usize, Option<String>>(15)?;
        let has_voting_rights: Option<bool> = row.get::<usize, Option<bool>>(16)?;
        let termination_date = row
            .get::<usize, Option<i32>>(17)?
            .map(|n| Date::ZERO + (719528 + n).days());
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

#[derive(Default, Deserialize)]
pub struct QueryFilter {
    pub as_of: Option<Date>,
    pub as_of_gte: Option<Date>,
    pub as_of_lte: Option<Date>,
    pub id: Option<i64>,
    pub customer_name: Option<String>,
    pub address1: Option<String>,
    pub address2: Option<String>,
    pub address3: Option<String>,
    pub city: Option<String>,
    pub state: Option<String>,
    pub zip: Option<String>,
    pub country: Option<String>,
    pub phone: Option<String>,
    pub status: Option<Status>,
    pub sector: Option<Sector>,
    pub participant_type: Option<ParticipantType>,
    pub classification: Option<Classification>,
    pub sub_classification: Option<String>,
    pub has_voting_rights: Option<bool>,
    pub termination_date: Option<Date>,
    pub termination_date_gte: Option<Date>,
    pub termination_date_lte: Option<Date>,
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

    pub fn as_of(mut self, value: Date) -> Self {
        self.inner.as_of = Some(value);
        self
    }

    pub fn as_of_gte(mut self, value: Date) -> Self {
        self.inner.as_of_gte = Some(value);
        self
    }

    pub fn as_of_lte(mut self, value: Date) -> Self {
        self.inner.as_of_lte = Some(value);
        self
    }

    pub fn id(mut self, value: i64) -> Self {
        self.inner.id = Some(value);
        self
    }

    pub fn customer_name<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.customer_name = Some(value.into());
        self
    }

    pub fn address1<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.address1 = Some(value.into());
        self
    }

    pub fn address2<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.address2 = Some(value.into());
        self
    }

    pub fn address3<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.address3 = Some(value.into());
        self
    }

    pub fn city<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.city = Some(value.into());
        self
    }

    pub fn state<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.state = Some(value.into());
        self
    }

    pub fn zip<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.zip = Some(value.into());
        self
    }

    pub fn country<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.country = Some(value.into());
        self
    }

    pub fn phone<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.phone = Some(value.into());
        self
    }

    pub fn status(mut self, value: Status) -> Self {
        self.inner.status = Some(value);
        self
    }

    pub fn sector(mut self, value: Sector) -> Self {
        self.inner.sector = Some(value);
        self
    }

    pub fn participant_type(mut self, value: ParticipantType) -> Self {
        self.inner.participant_type = Some(value);
        self
    }

    pub fn classification(mut self, value: Classification) -> Self {
        self.inner.classification = Some(value);
        self
    }

    pub fn sub_classification<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.sub_classification = Some(value.into());
        self
    }

    pub fn has_voting_rights(mut self, value: bool) -> Self {
        self.inner.has_voting_rights = Some(value);
        self
    }

    pub fn termination_date(mut self, value: Date) -> Self {
        self.inner.termination_date = Some(value);
        self
    }

    pub fn termination_date_gte(mut self, value: Date) -> Self {
        self.inner.termination_date_gte = Some(value);
        self
    }

    pub fn termination_date_lte(mut self, value: Date) -> Self {
        self.inner.termination_date_lte = Some(value);
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
        let xs: Vec<Record> = get_data(&conn, &filter).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 590);
        Ok(())
    }
}
