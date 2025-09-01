use std::str::FromStr;

use quick_xml::de::from_str;
use rust_decimal::Decimal;
use serde::Deserialize;

#[derive(Debug, Deserialize)]
struct Product {
    #[serde(rename = "price")]
    price: String,
}

fn main() {
    let xml = r#"<product><price>123.45</price></product>"#;
    let product: Product = from_str(xml).unwrap();
    println!("{:?}", product); // Product { price: 123.45 }
    println!("{:?}", Decimal::from_str(&product.price));
}