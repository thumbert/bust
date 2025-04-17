use duckdb::{Connection, Result};
use jiff::{civil::Date, tz::TimeZone, Timestamp, ToSpan, Zoned};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
struct Row {
    date: Date,
    version: Timestamp,
    hour_beginning: Zoned,
}

/// A short example of how to deal with jiff Dates, Timestamps, Zoned in DuckDB
fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        r#"
CREATE TABLE test (
    date DATE,
    version TIMESTAMP,
    hour_beginning TIMESTAMPTZ,
);
INSERT INTO test VALUES ('2025-01-01', '2025-01-03T05:25:00Z', '2025-01-01T00:00:00-05:00');
INSERT INTO test VALUES ('2025-01-01', '2025-01-03T05:25:00Z', '2025-01-01T01:00:00-05:00');
INSERT INTO test VALUES ('2025-01-01', '2025-01-03T05:25:00Z', '2025-01-01T02:00:00-05:00');
    "#,
    )?;
    let mut stmt = conn.prepare("SELECT * FROM test")?;
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(0).unwrap();
        let micro: i64 = row.get(1).unwrap();
        let micro2: i64 = row.get(2).unwrap();
        let ts = Timestamp::from_second(micro2 / 1_000_000).unwrap();
        Ok(Row {
            date: Date::ZERO.checked_add(n.days()).unwrap(),
            version: Timestamp::from_second(micro / 1_000_000).unwrap(),
            hour_beginning: Zoned::new(ts, TimeZone::get("America/New_York").unwrap()),
        })
    })?;

    let items: Vec<Row> = res_iter.map(|e| e.unwrap()).collect();

    for item in &items {
        println!("Found item: {:?}", item);
    }

    println!("{}", json!(items.first().unwrap())); 
    // {"date":"2025-01-01","version":"2025-01-03T05:25:00Z","hour_beginning":"2025-01-01T00:00:00-05:00[America/New_York]"}

    Ok(())
}
