use std::{marker, str::FromStr};

use chrono::{DateTime, Datelike, Timelike, Weekday};
use chrono_tz::Tz;

use crate::holiday::*;

pub const ATC: Bucket<Atc> = Bucket {
    state: std::marker::PhantomData::<Atc>,
};
pub const B5X16: Bucket<_B5x16> = Bucket {
    state: std::marker::PhantomData::<_B5x16>,
};
pub const B2X16H: _B2x16H = _B2x16H {};
pub const B7X8: _B7x8 = _B7x8 {};

/// I don't know how to parse a string into the correct bucket
/// I can parse an individual bucket...

pub trait BucketLike {
    fn name(self) -> String;
    fn contains(self, datetime: DateTime<Tz>) -> bool;
}

#[derive(Debug, PartialEq, Eq)]
pub struct Bucket<B> {
    state: std::marker::PhantomData<B>,
}

impl FromStr for Bucket<Atc> {
    type Err = ParseBucketError;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_uppercase().as_str() {
            "ATC" | "FLAT" => Ok(ATC),
            _ => Err(ParseBucketError),
        }
    }
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseBucketError;

#[derive(Debug, PartialEq, Eq)]
pub struct Atc {}
impl BucketLike for Bucket<Atc> {
    fn name(self) -> String {
        String::from("ATC")
    }

    fn contains(self, _: DateTime<Tz>) -> bool {
        true
    }
}

pub struct _B5x16 {}
impl BucketLike for _B5x16 {
    fn name(self) -> String {
        String::from("5x16")
    }

    fn contains(self, dt: DateTime<Tz>) -> bool {
        contains_5x16(dt)
    }
}

pub struct _B2x16H {}
impl BucketLike for _B2x16H {
    fn name(self) -> String {
        String::from("2x16H")
    }

    fn contains(self, dt: DateTime<Tz>) -> bool {
        contains_2x16h(dt)
    }
}

pub struct _B7x8 {}
impl BucketLike for _B7x8 {
    fn name(self) -> String {
        String::from("7x8")
    }

    fn contains(self, dt: DateTime<Tz>) -> bool {
        contains_7x8(dt)
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

    use crate::bucket::*;

    #[test]
    fn test_bucket_atc() {
        assert_eq!(ATC.name(), "ATC");
        assert!(ATC.contains(New_York.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()));
        let b ="Flat".parse::<Bucket<Atc>>().unwrap(); 
        assert_eq!(b, ATC);
    }
}
