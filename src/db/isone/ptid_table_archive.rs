use std::collections::HashMap;

use serde::{Serialize, Deserialize};
use duckdb::Connection;
use url::form_urlencoded;

use jiff::{civil::Date, ToSpan};
use std::str::FromStr;
use convert_case::{Case, Casing};


#[derive(Clone)]
pub struct IsonePtidTableArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}
// All the insertion and update operations are done with Dart


#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub node_type: NodeType,
    pub ptid: i32,
    pub name: String,
    pub substation_name: Option<String>,
    pub unit_name: Option<String>,
    pub unit_short_name: Option<String>,
    pub zone_id: Option<i32>,
    pub reserve_id: Option<i32>,
    pub rsp_area: Option<String>,
    pub dispatch_zone: Option<String>,
    pub dr_reserve_aggregation_zone_id: Option<i32>,
    pub activated_on: Date,
    pub deactivated_on: Option<Date>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NodeType {
    AggregationZone,
    Hub,
    Interface,
    Load,
    LoadZone,
    Node,
    ReserveZone,
}

impl std::str::FromStr for NodeType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "AGGREGATION_ZONE" => Ok(NodeType::AggregationZone),
            "HUB" => Ok(NodeType::Hub),
            "INTERFACE" => Ok(NodeType::Interface),
            "LOAD" => Ok(NodeType::Load),
            "LOAD_ZONE" => Ok(NodeType::LoadZone),
            "NODE" => Ok(NodeType::Node),
            "RESERVE_ZONE" => Ok(NodeType::ReserveZone),
            _ => Err(format!("Invalid value for NodeType: {}", s)),
        }
    }
}

impl std::fmt::Display for NodeType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NodeType::AggregationZone => write!(f, "aggregation_zone"),
            NodeType::Hub => write!(f, "hub"),
            NodeType::Interface => write!(f, "interface"),
            NodeType::Load => write!(f, "load"),
            NodeType::LoadZone => write!(f, "load_zone"),
            NodeType::Node => write!(f, "node"),
            NodeType::ReserveZone => write!(f, "reserve_zone"),
        }
    }
}

impl serde::Serialize for NodeType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            NodeType::AggregationZone => "aggregation_zone",
            NodeType::Hub => "hub",
            NodeType::Interface => "interface",
            NodeType::Load => "load",
            NodeType::LoadZone => "load_zone",
            NodeType::Node => "node",
            NodeType::ReserveZone => "reserve_zone",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for NodeType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NodeType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

pub fn get_data(conn: &Connection,
query_filter: &QueryFilter,limit: Option<usize>) -> Result<Vec<Record>, Box<dyn std::error::Error>> {   let mut query = String::from(r#"
SELECT
    node_type,
    ptid,
    name,
    substation_name,
    unit_name,
    unit_short_name,
    zone_id,
    reserve_id,
    rsp_area,
    dispatch_zone,
    dr_reserve_aggregation_zone_id,
    activated_on,
    deactivated_on
FROM ptid_table WHERE 1=1"#);
    if let Some(node_type) = &query_filter.node_type {
        query.push_str(&format!("
    AND node_type = '{}'", node_type));
    }
    if let Some(node_type_in) = &query_filter.node_type_in {
        query.push_str(&format!("
    AND node_type IN ('{}')", node_type_in.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("','")));
    }
    if let Some(activated_on) = &query_filter.activated_on {
        query.push_str(&format!("
    AND activated_on = '{}'", activated_on));
    }
    if let Some(activated_on_in) = &query_filter.activated_on_in {
        query.push_str(&format!("
    AND activated_on IN ('{}')", activated_on_in.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("','")));
    }
    if let Some(activated_on_gte) = &query_filter.activated_on_gte {
        query.push_str(&format!("
    AND activated_on >= '{}'", activated_on_gte));
    }
    if let Some(activated_on_lte) = &query_filter.activated_on_lte {
        query.push_str(&format!("
    AND activated_on <= '{}'", activated_on_lte));
    }
    if let Some(deactivated_on) = &query_filter.deactivated_on {
        query.push_str(&format!("
    AND deactivated_on = '{}'", deactivated_on));
    }
    if let Some(deactivated_on_in) = &query_filter.deactivated_on_in {
        query.push_str(&format!("
    AND deactivated_on IN ('{}')", deactivated_on_in.iter().map(|v| v.to_string()).collect::<Vec<_>>().join("','")));
    }
    if let Some(deactivated_on_gte) = &query_filter.deactivated_on_gte {
        query.push_str(&format!("
    AND deactivated_on >= '{}'", deactivated_on_gte));
    }
    if let Some(deactivated_on_lte) = &query_filter.deactivated_on_lte {
        query.push_str(&format!("
    AND deactivated_on <= '{}'", deactivated_on_lte));
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
        let _n0 = match row.get_ref_unwrap(0).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum node_type"),
        };
        let node_type = NodeType::from_str(&_n0).unwrap();
        let ptid: i32 = row.get::<usize, i32>(1)?;
        let name: String = row.get::<usize, String>(2)?;
        let substation_name: Option<String> = row.get::<usize, Option<String>>(3)?;
        let unit_name: Option<String> = row.get::<usize, Option<String>>(4)?;
        let unit_short_name: Option<String> = row.get::<usize, Option<String>>(5)?;
        let zone_id: Option<i32> = row.get::<usize, Option<i32>>(6)?;
        let reserve_id: Option<i32> = row.get::<usize, Option<i32>>(7)?;
        let rsp_area: Option<String> = row.get::<usize, Option<String>>(8)?;
        let dispatch_zone: Option<String> = row.get::<usize, Option<String>>(9)?;
        let dr_reserve_aggregation_zone_id: Option<i32> = row.get::<usize, Option<i32>>(10)?;
        let _n11 = 719528 + row.get::<usize, i32>(11)?;
        let activated_on = Date::ZERO + _n11.days();
        let deactivated_on = row
            .get::<usize, Option<i32>>(12)?
            .map(|n| {Date::ZERO + (719528 + n).days() });
        Ok(Record {
            node_type,
            ptid,
            name,
            substation_name,
            unit_name,
            unit_short_name,
            zone_id,
            reserve_id,
            rsp_area,
            dispatch_zone,
            dr_reserve_aggregation_zone_id,
            activated_on,
            deactivated_on,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub node_type: Option<NodeType>,
    pub node_type_in: Option<Vec<NodeType>>,
    pub activated_on: Option<Date>,
    pub activated_on_in: Option<Vec<Date>>,
    pub activated_on_gte: Option<Date>,
    pub activated_on_lte: Option<Date>,
    pub deactivated_on: Option<Date>,
    pub deactivated_on_in: Option<Vec<Date>>,
    pub deactivated_on_gte: Option<Date>,
    pub deactivated_on_lte: Option<Date>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.node_type {
            params.insert("node_type", value.to_string());
        }
        if let Some(value) = &self.node_type_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("node_type_in", joined);
        }
        if let Some(value) = &self.activated_on {
            params.insert("activated_on", value.to_string());
        }
        if let Some(value) = &self.activated_on_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("activated_on_in", joined);
        }
        if let Some(value) = &self.activated_on_gte {
            params.insert("activated_on_gte", value.to_string());
        }
        if let Some(value) = &self.activated_on_lte {
            params.insert("activated_on_lte", value.to_string());
        }
        if let Some(value) = &self.deactivated_on {
            params.insert("deactivated_on", value.to_string());
        }
        if let Some(value) = &self.deactivated_on_in {
            let joined = value.iter().map(|v| v.to_string()).collect::<Vec<_>>().join(",");
            params.insert("deactivated_on_in", joined);
        }
        if let Some(value) = &self.deactivated_on_gte {
            params.insert("deactivated_on_gte", value.to_string());
        }
        if let Some(value) = &self.deactivated_on_lte {
            params.insert("deactivated_on_lte", value.to_string());
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

    pub fn node_type(mut self, value: NodeType) -> Self {
        self.inner.node_type = Some(value);
        self
    }

    pub fn node_type_in(mut self, values_in: Vec<NodeType>) -> Self {
        self.inner.node_type_in = Some(values_in);
        self
    }

    pub fn activated_on(mut self, value: Date) -> Self {
        self.inner.activated_on = Some(value);
        self
    }

    pub fn activated_on_in(mut self, values_in: Vec<Date>) -> Self {
        self.inner.activated_on_in = Some(values_in);
        self
    }

    pub fn activated_on_gte(mut self, value: Date) -> Self {
        self.inner.activated_on_gte = Some(value);
        self
    }

    pub fn activated_on_lte(mut self, value: Date) -> Self {
        self.inner.activated_on_lte = Some(value);
        self
    }

    pub fn deactivated_on(mut self, value: Date) -> Self {
        self.inner.deactivated_on = Some(value);
        self
    }

    pub fn deactivated_on_in(mut self, values_in: Vec<Date>) -> Self {
        self.inner.deactivated_on_in = Some(values_in);
        self
    }

    pub fn deactivated_on_gte(mut self, value: Date) -> Self {
        self.inner.deactivated_on_gte = Some(value);
        self
    }

    pub fn deactivated_on_lte(mut self, value: Date) -> Self {
        self.inner.deactivated_on_lte = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;
    use duckdb::{AccessMode, Config, Connection};
    use crate::db::prod_db::ProdDb;
    use super::*;

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::scratch().duckdb_path, config).unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }
}
