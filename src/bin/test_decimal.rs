use rust_decimal::Decimal;
use serde::{Serialize, Deserialize};

#[derive(Serialize, Deserialize, Debug)]
struct Test {
    #[serde(with = "rust_decimal::serde::float_option")]
    value: Option<Decimal>,
}

fn main() {
    let t = Test { value: Some(Decimal::new(12345, 2)) };
    let json = serde_json::to_string(&t).unwrap();
    println!("{}", json);
}