pub mod buckets;
pub mod holiday;
pub mod interval;


mod tests;

use chrono::prelude::*;
use chrono::Datelike;

fn is_weekend<T: Datelike + Copy>(date: T) -> bool {
    matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

#[test]
fn test_is_weekend() {
    assert!(is_weekend(NaiveDate::from_ymd_opt(2022, 12, 3).unwrap()));
}
