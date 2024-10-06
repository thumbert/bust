pub mod bucket;
pub mod elec;
pub mod holiday;
pub mod interval;
pub mod timeseries;



// pub mod db; //capacity/bids_offers/monthly_auction; //monthly_capacity_auction_archive;
#[path = "db/isone/mod.rs"]
pub mod isone;

pub mod api;

// use crate::


pub mod tests;



use chrono::prelude::*;
use chrono::Datelike;

fn is_weekend<T: Datelike + Copy>(date: T) -> bool {
    matches!(date.weekday(), Weekday::Sat | Weekday::Sun)
}

#[test]
fn test_is_weekend() {
    assert!(is_weekend(NaiveDate::from_ymd_opt(2022, 12, 3).unwrap()));
}
