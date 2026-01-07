use std::collections::HashMap;
use std::time::Duration;

use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{AccessMode, Connection};
use jiff::{civil::Date, ToSpan};
use serde::{Deserialize, Serialize};

use convert_case::{Case, Casing};
use jiff::Timestamp;
use jiff::{tz::TimeZone, Zoned};
use rust_decimal::Decimal;
use std::str::FromStr;
use url::form_urlencoded;

use crate::db::prod_db::ScratchArchive;
use crate::utils::lib_duckdb::open_with_retry;

#[get("/api/data")]
pub async fn get_data_api(
    query: web::Query<ApiQuery>,
    data: web::Data<ScratchArchive>,
) -> impl Responder {
    let conn = open_with_retry(
        &data.duckdb_path,
        8,
        Duration::from_millis(25),
        AccessMode::ReadOnly,
    );
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database at {}: {}",
            &data.duckdb_path,
            conn.err().unwrap(),
        ));
    }
    let conn = conn.unwrap();

    let query_filter = query.to_query_filter();
    println!("query_filter: {:?}", query_filter);
    match get_data(&conn, &query_filter) {
        Ok(records) => {
            if records.len() > 100_000 {
                HttpResponse::BadRequest()
                    .body(format!("Query returned {} records, only a max of 100,000 are allowed.  Please narrow your query.", records.len()))
            } else {
                HttpResponse::Ok().json(records)
            }
        }
        Err(e) => HttpResponse::InternalServerError().body(format!("Error querying data: {}", e)),
    }
}

#[derive(Debug, Deserialize)]
pub struct ApiQuery {
    pub hour_beginning: Option<Zoned>,
    pub hour_beginning_gte: Option<Zoned>,
    pub hour_beginning_lt: Option<Zoned>,
    pub as_of: Option<Date>,
    pub as_of_in: Option<String>,
    pub as_of_gte: Option<Date>,
    pub as_of_lte: Option<Date>,
    pub resource_type: Option<ResourceType>,
    pub resource_type_in: Option<String>,
    pub resource_id: Option<i32>,
    pub resource_id_in: Option<String>,
    pub resource_id_gte: Option<i32>,
    pub resource_id_lte: Option<i32>,
    pub location: Option<String>,
    pub location_like: Option<String>,
    pub location_in: Option<String>,
    pub price: Option<Decimal>,
    pub price_in: Option<String>,
    pub price_gte: Option<Decimal>,
    pub price_lte: Option<Decimal>,
}

impl ApiQuery {
    pub fn to_query_filter(&self) -> QueryFilter {
        QueryFilter {
            hour_beginning: self.hour_beginning.clone(),
            hour_beginning_gte: self.hour_beginning_gte.clone(),
            hour_beginning_lt: self.hour_beginning_lt.clone(),
            as_of: self.as_of,
            as_of_in: self.as_of_in.as_ref().map(|s| {
                s.split(',')
                    .map(|v| v.trim().parse::<Date>().unwrap())
                    .collect()
            }),
            as_of_gte: self.as_of_gte,
            as_of_lte: self.as_of_lte,
            resource_type: self.resource_type,
            resource_type_in: self
                .resource_type_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            resource_id: self.resource_id,
            resource_id_in: self
                .resource_id_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            resource_id_gte: self.resource_id_gte,
            resource_id_lte: self.resource_id_lte,
            location: self.location.clone(),
            location_like: self.location_like.clone(),
            location_in: self
                .location_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            price: self.price,
            price_in: self
                .price_in
                .as_ref()
                .map(|s| s.split(',').map(|v| v.trim().parse().unwrap()).collect()),
            price_gte: self.price_gte,
            price_lte: self.price_lte,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub hour_beginning: Zoned,
    pub as_of: Date,
    pub resource_type: ResourceType,
    pub resource_id: i32,
    pub location: Option<String>,
    pub price: Option<Decimal>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ResourceType {
    Hydro,
    Solar,
    Storage,
    Wind,
}

impl std::str::FromStr for ResourceType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "HYDRO" => Ok(ResourceType::Hydro),
            "SOLAR" => Ok(ResourceType::Solar),
            "STORAGE" => Ok(ResourceType::Storage),
            "WIND" => Ok(ResourceType::Wind),
            _ => Err(format!("Invalid value for ResourceType: {}", s)),
        }
    }
}

impl std::fmt::Display for ResourceType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            ResourceType::Hydro => write!(f, "hydro"),
            ResourceType::Solar => write!(f, "solar"),
            ResourceType::Storage => write!(f, "storage"),
            ResourceType::Wind => write!(f, "wind"),
        }
    }
}

impl serde::Serialize for ResourceType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            ResourceType::Hydro => "hydro",
            ResourceType::Solar => "solar",
            ResourceType::Storage => "storage",
            ResourceType::Wind => "wind",
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

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    hour_beginning,
    as_of,
    resource_type,
    resource_id,
    location,
    price
FROM basic WHERE 1=1"#,
    );
    if let Some(hour_beginning) = &query_filter.hour_beginning {
        query.push_str(&format!(
            "
    AND hour_beginning = '{}'",
            hour_beginning.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(hour_beginning_gte) = &query_filter.hour_beginning_gte {
        query.push_str(&format!(
            "
    AND hour_beginning >= '{}'",
            hour_beginning_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(hour_beginning_lt) = &query_filter.hour_beginning_lt {
        query.push_str(&format!(
            "
    AND hour_beginning < '{}'",
            hour_beginning_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(as_of) = &query_filter.as_of {
        query.push_str(&format!(
            "
    AND as_of = '{}'",
            as_of
        ));
    }
    if let Some(as_of_in) = &query_filter.as_of_in {
        query.push_str(&format!(
            "
    AND as_of IN ('{}')",
            as_of_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(as_of_gte) = &query_filter.as_of_gte {
        query.push_str(&format!(
            "
    AND as_of >= '{}'",
            as_of_gte
        ));
    }
    if let Some(as_of_lte) = &query_filter.as_of_lte {
        query.push_str(&format!(
            "
    AND as_of <= '{}'",
            as_of_lte
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
    if let Some(resource_id) = &query_filter.resource_id {
        query.push_str(&format!(
            "
    AND resource_id = {}",
            resource_id
        ));
    }
    if let Some(resource_id_in) = &query_filter.resource_id_in {
        query.push_str(&format!(
            "
    AND resource_id IN ({})",
            resource_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(resource_id_gte) = &query_filter.resource_id_gte {
        query.push_str(&format!(
            "
    AND resource_id >= {}",
            resource_id_gte
        ));
    }
    if let Some(resource_id_lte) = &query_filter.resource_id_lte {
        query.push_str(&format!(
            "
    AND resource_id <= {}",
            resource_id_lte
        ));
    }
    if let Some(location) = &query_filter.location {
        query.push_str(&format!(
            "
    AND location = '{}'",
            location
        ));
    }
    if let Some(location_like) = &query_filter.location_like {
        query.push_str(&format!(
            "
    AND location LIKE '{}'",
            location_like
        ));
    }
    if let Some(location_in) = &query_filter.location_in {
        query.push_str(&format!(
            "
    AND location IN ('{}')",
            location_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
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
    query.push(';');

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let _micros0: i64 = row.get::<usize, i64>(0)?;
        let hour_beginning = Zoned::new(
            Timestamp::from_microsecond(_micros0).unwrap(),
            TimeZone::get("America/New_York").unwrap(),
        );
        let _n1 = 719528 + row.get::<usize, i32>(1)?;
        let as_of = Date::ZERO + _n1.days();
        let _n2 = match row.get_ref_unwrap(2).to_owned() {
            duckdb::types::Value::Enum(v) => v,
            v => panic!("Unexpected value type {v:?} for enum resource_type"),
        };
        let resource_type = ResourceType::from_str(&_n2).unwrap();
        let resource_id: i32 = row.get::<usize, i32>(3)?;
        let location: Option<String> = row.get::<usize, Option<String>>(4)?;
        let price: Option<Decimal> = match row.get_ref_unwrap(5) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        Ok(Record {
            hour_beginning,
            as_of,
            resource_type,
            resource_id,
            location,
            price,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub hour_beginning: Option<Zoned>,
    pub hour_beginning_gte: Option<Zoned>,
    pub hour_beginning_lt: Option<Zoned>,
    pub as_of: Option<Date>,
    pub as_of_in: Option<Vec<Date>>,
    pub as_of_gte: Option<Date>,
    pub as_of_lte: Option<Date>,
    pub resource_type: Option<ResourceType>,
    pub resource_type_in: Option<Vec<ResourceType>>,
    pub resource_id: Option<i32>,
    pub resource_id_in: Option<Vec<i32>>,
    pub resource_id_gte: Option<i32>,
    pub resource_id_lte: Option<i32>,
    pub location: Option<String>,
    pub location_like: Option<String>,
    pub location_in: Option<Vec<String>>,
    pub price: Option<Decimal>,
    pub price_in: Option<Vec<Decimal>>,
    pub price_gte: Option<Decimal>,
    pub price_lte: Option<Decimal>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.hour_beginning {
            params.insert("hour_beginning", value.to_string());
        }
        if let Some(value) = &self.hour_beginning_gte {
            params.insert("hour_beginning_gte", value.to_string());
        }
        if let Some(value) = &self.hour_beginning_lt {
            params.insert("hour_beginning_lt", value.to_string());
        }
        if let Some(value) = &self.as_of {
            params.insert("as_of", value.to_string());
        }
        if let Some(value) = &self.as_of_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("as_of_in", joined);
        }
        if let Some(value) = &self.as_of_gte {
            params.insert("as_of_gte", value.to_string());
        }
        if let Some(value) = &self.as_of_lte {
            params.insert("as_of_lte", value.to_string());
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
        if let Some(value) = &self.resource_id {
            params.insert("resource_id", value.to_string());
        }
        if let Some(value) = &self.resource_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("resource_id_in", joined);
        }
        if let Some(value) = &self.resource_id_gte {
            params.insert("resource_id_gte", value.to_string());
        }
        if let Some(value) = &self.resource_id_lte {
            params.insert("resource_id_lte", value.to_string());
        }
        if let Some(value) = &self.location {
            params.insert("location", value.to_string());
        }
        if let Some(value) = &self.location_like {
            params.insert("location_like", value.to_string());
        }
        if let Some(value) = &self.location_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("location_in", joined);
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

    pub fn hour_beginning(mut self, value: Zoned) -> Self {
        self.inner.hour_beginning = Some(value);
        self
    }

    pub fn hour_beginning_gte(mut self, value: Zoned) -> Self {
        self.inner.hour_beginning_gte = Some(value);
        self
    }

    pub fn hour_beginning_lt(mut self, value: Zoned) -> Self {
        self.inner.hour_beginning_lt = Some(value);
        self
    }

    pub fn as_of(mut self, value: Date) -> Self {
        self.inner.as_of = Some(value);
        self
    }

    pub fn as_of_in(mut self, values_in: Vec<Date>) -> Self {
        self.inner.as_of_in = Some(values_in);
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

    pub fn resource_type(mut self, value: ResourceType) -> Self {
        self.inner.resource_type = Some(value);
        self
    }

    pub fn resource_type_in(mut self, values_in: Vec<ResourceType>) -> Self {
        self.inner.resource_type_in = Some(values_in);
        self
    }

    pub fn resource_id(mut self, value: i32) -> Self {
        self.inner.resource_id = Some(value);
        self
    }

    pub fn resource_id_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.resource_id_in = Some(values_in);
        self
    }

    pub fn resource_id_gte(mut self, value: i32) -> Self {
        self.inner.resource_id_gte = Some(value);
        self
    }

    pub fn resource_id_lte(mut self, value: i32) -> Self {
        self.inner.resource_id_lte = Some(value);
        self
    }

    pub fn location<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.location = Some(value.into());
        self
    }

    pub fn location_like(mut self, value_like: String) -> Self {
        self.inner.location_like = Some(value_like);
        self
    }

    pub fn location_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.location_in = Some(values_in);
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
    use super::*;
    use crate::db::prod_db::ProdDb;
    use duckdb::{AccessMode, Config, Connection};
    use std::error::Error;

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(ProdDb::scratch().duckdb_path, config).unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 0);
        Ok(())
    }
}

#[cfg(test)]
mod api_tests {
    use std::collections::HashMap;
    use url::form_urlencoded;

    use crate::db::prod_db::ProdDb;

    use super::*;
    use actix_web::{body::MessageBody, test, web, App};
    use jiff::civil::date;

    #[actix_web::test]
    async fn test_get_data_api() {
        let data = web::Data::new(ProdDb::scratch());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;
        let req = test::TestRequest::get().uri("/api/data").to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let data = resp.into_body().try_into_bytes().unwrap();
        let records: Vec<Record> = serde_json::from_slice(&data).unwrap();
        assert_eq!(records.len(), 4);
    }

    #[actix_web::test]
    async fn test_get_data2_api() {
        let data = web::Data::new(ProdDb::scratch());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;

        let filter = QueryFilterBuilder::new()
            .as_of_gte(date(2023, 1, 10))
            .as_of_lte(date(2023, 4, 20))
            .build();
        let uri = format!("/api/data?{}", filter.to_query_url());

        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let data = resp.into_body().try_into_bytes().unwrap();
        let records: Vec<Record> = serde_json::from_slice(&data).unwrap();
        // println!("records: {:?}", records);
        assert_eq!(records.len(), 3);
    }

    #[actix_web::test]
    async fn test_get_data3_api() {
        let data = web::Data::new(ProdDb::scratch());
        let app = test::init_service(App::new().app_data(data.clone()).service(get_data_api)).await;

        let filter = QueryFilterBuilder::new()
            .resource_type_in(vec![ResourceType::Solar, ResourceType::Wind])
            .build();
        let uri = format!("/api/data?{}", filter.to_query_url());

        let req = test::TestRequest::get().uri(&uri).to_request();
        let resp = test::call_service(&app, req).await;
        assert!(resp.status().is_success());
        let data = resp.into_body().try_into_bytes().unwrap();
        let records: Vec<Record> = serde_json::from_slice(&data).unwrap();
        // println!("records: {:?}", records);
        assert_eq!(records.len(), 2);
    }
}
