use chrono::{DateTime, Datelike, Timelike, Weekday};
use chrono_tz::Tz;

use crate::holiday::*;

// some convenience definitions
pub const ATC: Bucket = Bucket::Atc;

#[derive(Debug, PartialEq, Eq)]
pub enum Bucket {
    Atc,
    B5x16,
    B2x16H,
    B7x8,
}

fn parse(s: &str) -> Result<Bucket, ParseError> {
    match s.to_uppercase().as_str() {
        "FLAT" | "ATC" => Ok(Bucket::Atc),
        "5X16" | "PEAK" => Ok(Bucket::B5x16),
        "2X16H" => Ok(Bucket::B2x16H),
        "7X8" => Ok(Bucket::B7x8),
        _ => Err(ParseError),
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseError;

pub trait BucketLike {
    fn name(self) -> String;
    fn contains(self, datetime: DateTime<Tz>) -> bool;
}

impl BucketLike for Bucket {
    fn name(self) -> String {
        match self {
            Bucket::Atc => String::from("ATC"),
            Bucket::B5x16 => String::from("5x16"),
            Bucket::B2x16H => String::from("2x16H"),
            Bucket::B7x8 => String::from("7x8"),
        }
    }

    fn contains(self, datetime: DateTime<Tz>) -> bool {
        match self {
            Bucket::Atc => true,
            Bucket::B5x16 => contains_5x16(datetime),
            Bucket::B2x16H => contains_2x16h(datetime),
            Bucket::B7x8 => contains_7x8(datetime),
        }
    }
}

fn contains_5x16(dt: DateTime<Tz>) -> bool {
    if dt.weekday() == Weekday::Sat && dt.weekday() == Weekday::Sun {
        return false;
    }
    if dt.hour() < 7 || dt.hour() == 23 {
        false
    } else {
        !NERC_CALENDAR.is_holiday(&dt)
    }
}

fn contains_2x16h(dt: DateTime<Tz>) -> bool {
    if dt.hour() < 7 || dt.hour() == 23 {
        return false;
    }
    if dt.weekday() == Weekday::Sat && dt.weekday() == Weekday::Sun {
        true
    } else {
        NERC_CALENDAR.is_holiday(&dt)
    }
}

fn contains_7x8(dt: DateTime<Tz>) -> bool {
    dt.hour() < 7 || dt.hour() == 23
}

#[cfg(test)]
mod tests {
    use chrono::TimeZone;
    use chrono_tz::America::New_York;

    use crate::{bucket::*, interval::*};

    #[test]
    fn test_bucket_atc() {
        let dt = New_York.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
        assert!(Bucket::Atc.contains(dt));
        assert!(ATC.contains(dt));
        assert_eq!(parse("Flat"), Ok(ATC));
    }

    // fn test_bucket_5x16() {
    //     let term = Interval::with_start_end(
    //         New_York.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
    //         New_York.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
    //     );
    //     let hours = term.unwrap().hours();

    //     let dt = New_York.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
    //     assert!(Bucket::Atc.contains(dt));
    //     assert!(ATC.contains(dt));
    //     assert_eq!(parse("Flat"), Ok(ATC));
    // }
}
