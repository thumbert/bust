use duckdb::{types::ValueRef, Connection, Result};
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use serde_json::json;

// Serialization to json is done as a string unless the serde-with-float feature is used!
#[derive(Debug, Serialize, Deserialize)]
struct Item {
    name: String,
    #[serde(with = "rust_decimal::serde::float")]
    price: Decimal,
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        r#"
CREATE TABLE test (
    name VARCHAR NOT NULL,
    price DECIMAL(9,4),
);
INSERT INTO test VALUES ('meat', 3.99);
INSERT INTO test VALUES ('hate', NULL);
INSERT INTO test VALUES ('pen', '0.49');
    "#,
    )?;
    let mut stmt = conn.prepare("SELECT name, price FROM test")?;
    let item_iter = stmt.query_map([], |row| {
        let price = match row.get_ref_unwrap(1) {
            ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(Item {
            name: row.get(0)?,
            price,
        })
    })?;

    for item in item_iter {
        println!("Found item: {:?}", item.unwrap());
    }

    let item = Item {
        name: "ham".to_string(), price: dec!(10.99)
    };
    println!("{}", json!(item)); // {"name":"ham","price":10.99}


    Ok(())
}
