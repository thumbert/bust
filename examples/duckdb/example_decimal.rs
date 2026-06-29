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
    #[serde(with = "rust_decimal::serde::float_option")]
    mw: Option<Decimal>,
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        r#"
CREATE TABLE test (
    name VARCHAR NOT NULL,
    price DECIMAL(9,4),
    mw DECIMAL(9,4)
);
INSERT INTO test VALUES ('meat', 3.99, 1.23);
INSERT INTO test VALUES ('hate', NULL, NULL);
INSERT INTO test VALUES ('pen', '0.49', '0.05');
    "#,
    )?;
    let mut stmt = conn.prepare("SELECT name, price, mw FROM test")?;
    let item_iter = stmt.query_map([], |row| {
        let price = match row.get_ref_unwrap(1) {
            ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        let mw = match row.get_ref_unwrap(2) {
            ValueRef::Decimal(v) => Some(v),
            _ => None,
        };
        Ok(Item {
            name: row.get(0)?,
            price,
            mw,
        })
    })?;

    for item in item_iter {
        println!("Found item: {:?}", item.unwrap());
    }

    let item = Item {
        name: "ham".to_string(),
        price: dec!(10.99),
        mw: Some(dec!(2.34)),
    };
    println!("{}", json!(item)); // {"name":"ham","price":10.99,"mw":2.34}

    let item = Item {
        name: "ham".to_string(),
        price: dec!(10.99),
        mw: None,
    };
    println!("{}", json!(item)); // {"name":"ham","price":10.99,"mw":null}

    // For an example of how to read a DuckDB column of type DECIMAL(9,4)[]
    // see file example_list.rs!

    Ok(())
}
