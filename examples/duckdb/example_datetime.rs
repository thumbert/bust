use std::{error::Error, fmt};

use duckdb::{Connection, Result};
use jiff::{civil::{Date, Time}, tz::TimeZone, Timestamp, ToSpan, Zoned};
use serde::{de::{self, Visitor}, Deserialize, Deserializer, Serialize, Serializer};
use serde_json::json;

fn serialize_zoned_as_offset<S>(z: &Zoned, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&z.strftime("%Y-%m-%d %H:%M:%S%:z").to_string())
}


// Custom deserialization function for the Zoned field
fn deserialize_zoned_assume_ny<'de, D>(deserializer: D) -> Result<Zoned, D::Error>
where
    D: Deserializer<'de>,
{
    struct ZonedVisitor;

    impl Visitor<'_> for ZonedVisitor {
        type Value = Zoned;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a timestamp string with or without a zone name")
        }

        fn visit_str<E>(self, v: &str) -> Result<Zoned, E>
        where
            E: de::Error,
        {
            // If string already contains '[America/New_York]' or any [Zone], parse directly
            if v.contains('[') && v.contains(']') {
                Zoned::strptime("%F %T%:z[%Q]", v).map_err(E::custom)
            } else {
                // Otherwise, append the assumed zone
                let s = format!("{v}[America/New_York]");
                Zoned::strptime("%F %T%:z[%Q]", &s).map_err(E::custom)
            }
        }
    }

    deserializer.deserialize_str(ZonedVisitor)
}


#[derive(Debug, Serialize, Deserialize)]
struct Row {
    date: Date,
    version: Option<Timestamp>,
    #[serde(serialize_with = "serialize_zoned_as_offset")]
    hour_beginning: Zoned,
    time: Time,
}

#[derive(Debug, Deserialize)]
struct Data {
    #[serde(deserialize_with = "deserialize_zoned_assume_ny")]
    #[allow(dead_code)]
    hour_beginning: Zoned,
    #[allow(dead_code)]
    price: f64,
}

/// A short example of how to deal with jiff Dates, Timestamps, Zoned in DuckDB
fn main() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        r#"
CREATE TABLE test (
    date DATE,
    version TIMESTAMP,
    hour_beginning TIMESTAMPTZ,
    time TIME
);
INSERT INTO test VALUES ('2025-01-01', '2025-01-03T05:25:00Z', '2025-01-01T00:00:00-05:00', '1:00:00');
INSERT INTO test VALUES ('2025-01-01', '2025-01-03T05:25:00Z', '2025-01-01T01:00:00-05:00', '00:01:00');
INSERT INTO test VALUES ('2025-01-01', NULL, '2025-01-01T02:00:00-05:00', '16:30:00');
    "#,
    )?;
    let mut stmt = conn.prepare("SELECT * FROM test")?;
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(0).unwrap();
        let version = match row.get_ref_unwrap(1) {
            duckdb::types::ValueRef::Timestamp(_, value) => Some(Timestamp::from_second(value / 1_000_000).unwrap()),
            _ => None,
        };
        let micro2: i64 = row.get(2).unwrap();
        let ts = Timestamp::from_second(micro2 / 1_000_000).unwrap();
        let time = Time::midnight().saturating_add((row.get::<usize, i64>(3)? / 1_000_000).seconds());
        Ok(Row {
            date: Date::ZERO.checked_add(n.days()).unwrap(),
            version,
            hour_beginning: Zoned::new(ts, TimeZone::get("America/New_York").unwrap()),
            time,
        })
    })?;

    let items: Vec<Row> = res_iter.map(|e| e.unwrap()).collect();
    for item in &items {
        println!("Found item: {:?}", item);
    }
    println!("{}", json!(items.first().unwrap())); 
    // {"date":"2025-01-01","version":"2025-01-03T05:25:00Z","hour_beginning":"2025-01-01T00:00:00-05:00[America/New_York]"}

    // How to deserialize it?
    let json_data = "{\"hour_beginning\":\"2025-03-02 00:00:00-05:00[America/New_York]\",\"price\":42.0}";
    let deserialized: Data = serde_json::from_str(json_data)?;
    println!("{:?}", deserialized);

    let json_data_wo = "{\"hour_beginning\":\"2025-03-02 00:00:00-05:00\",\"price\":42.0}";
    let deserialized: Data = serde_json::from_str(json_data_wo)?;
    println!("{:?}", deserialized);

    Ok(())
}
