use duckdb::{types::Value, Connection, Result};
use jiff::{civil::Date, ToSpan};
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
struct Row {
    date: Date,
    values: Vec<f64>,
}

/// A short example of how to deal with jiff Dates, Timestamps, Zoned in DuckDB
fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        r#"
CREATE TABLE test (
    date DATE,
    values DOUBLE[],
);
INSERT INTO test VALUES ('2025-01-01', [1, 3.14]);
INSERT INTO test VALUES ('2025-01-02', [2.0, 2.718, 4.35]);
    "#,
    )?;
    let mut stmt = conn.prepare("SELECT * FROM test")?;
    let res_iter = stmt.query_map([], |row| {
        let n = 719528 + row.get::<usize, i32>(0).unwrap();
        let vs = match row.get_ref_unwrap(1).to_owned() {
            duckdb::types::Value::List(xs) => {
                xs.iter().map(|e| match e {
                    Value::Double(v) => v.to_owned(),
                    _ => panic!("Wrong value"),
                }).collect::<Vec<f64>>()
            },
            _ => panic!("Oops"),
        };
        // let vs = vec![1.0];
        Ok(Row {
            date: Date::ZERO.checked_add(n.days()).unwrap(),
            values: vs,
        })
    })?;
    let items: Vec<Row> = res_iter.map(|e| e.unwrap()).collect();

    for item in &items {
        println!("Found item: {:?}", item);
    }

    println!("{}", json!(items.first().unwrap())); // {"date":"2025-01-01","values":[1.0,3.14]}

    Ok(())
}
