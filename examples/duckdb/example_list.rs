use duckdb::{types::Value, Connection, Result};
use jiff::{civil::Date, ToSpan};
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use serde_json::json;

#[derive(Debug, Serialize, Deserialize)]
struct Row {
    mws: Vec<f64>,
    prices: Vec<Decimal>,
}

/// A short example of how to deal with List data structures in DuckDB
fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        r#"
CREATE TABLE test (
    values DOUBLE[],
    prices DECIMAL(9,2)[],
);
INSERT INTO test VALUES ([1, 3.14], [100.1, 120.2]);
INSERT INTO test VALUES ([2.0, 2.718, 4.35], [200.2, 210.51, 220.75]);
    "#,
    )?;
    let mut stmt = conn.prepare("SELECT * FROM test")?;
    let res_iter = stmt.query_map([], |row| {
        let mw = row.get_ref(0).unwrap().to_owned();
        let mws = match mw {
            Value::List(values) => values
                .iter()
                .map(|e| match e {
                    Value::Double(v) => v.to_owned(),
                    _ => panic!("Expected a double"),
                })
                .collect::<Vec<f64>>(),
            _ => panic!("Expected a list of doubles"),
        };
        let price = row.get_ref(1).unwrap().to_owned();
        let prices = match price {
            Value::List(values) => values
                .iter()
                .map(|e| match e {
                    Value::Decimal(v) => v.to_owned(),
                    _ => panic!("Expected a decimal"),
                })
                .collect::<Vec<Decimal>>(),
            _ => panic!("Expected a list of decimals"),
        };
        Ok(Row { mws, prices })
    })?;
    let items: Vec<Row> = res_iter.map(|e| e.unwrap()).collect();

    for item in &items {
        println!("Found item: {:?}", item);
    }

    println!("{}", json!(items.first().unwrap())); // {"values":[1.0,3.14]}

    Ok(())
}
