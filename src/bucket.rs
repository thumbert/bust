use std::str::FromStr;

use jiff::{civil::Weekday, Zoned};

use crate::{
    holiday::*,
    interval::{hour_tz::HourTz, interval::IntervalTzLike, term_tz::TermTz},
};

pub trait BucketLike {
    fn name(&self) -> String;
    fn contains(&self, datetime: &Zoned) -> bool;
    fn count_hours(&self, term: &TermTz) -> i32;
}

#[derive(Debug, PartialEq, Eq)]
pub enum Bucket {
    Atc,
    B5x16,
    B2x16H,
    B7x8,
    Offpeak,
}

#[derive(Debug, PartialEq, Eq)]
pub struct ParseError;

impl FromStr for Bucket {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parse_bucket(s) {
            Ok(bucket) => Ok(bucket),
            Err(_) => Err(format!("Failed parsing {} as an bucket", s)),
        }
    }
}

fn parse_bucket(s: &str) -> Result<Bucket, ParseError> {
    match s.to_uppercase().as_str() {
        "FLAT" | "ATC" => Ok(Bucket::Atc),
        "5X16" | "PEAK" => Ok(Bucket::B5x16),
        "2X16H" => Ok(Bucket::B2x16H),
        "7X8" => Ok(Bucket::B7x8),
        "OFFPEAK" => Ok(Bucket::Offpeak),
        _ => Err(ParseError),
    }
}

impl BucketLike for Bucket {
    fn name(&self) -> String {
        match self {
            Bucket::Atc => String::from("ATC"),
            Bucket::B5x16 => String::from("5x16"),
            Bucket::B2x16H => String::from("2x16H"),
            Bucket::B7x8 => String::from("7x8"),
            Bucket::Offpeak => String::from("Offpeak"),
        }
    }

    fn contains(&self, datetime: &Zoned) -> bool {
        match self {
            Bucket::Atc => true,
            Bucket::B5x16 => contains_5x16(datetime),
            Bucket::B2x16H => contains_2x16h(datetime),
            Bucket::B7x8 => contains_7x8(datetime),
            Bucket::Offpeak => !contains_5x16(datetime),
        }
    }

    fn count_hours(&self, term: &TermTz) -> i32 {
        let mut hour = HourTz::containing(term.start());
        let last = HourTz::containing(term.end());
        let mut count: i32 = 0;
        while hour < last {
            if self.contains(&hour.start()) {
                count += 1;
            }
            hour = hour.next();
        }
        count
    }
}

fn contains_5x16(dt: &Zoned) -> bool {
    if dt.weekday() == Weekday::Saturday || dt.weekday() == Weekday::Sunday {
        return false;
    }
    if dt.hour() < 7 || dt.hour() == 23 {
        false
    } else {
        !NERC_CALENDAR.is_holiday(&dt.date())
    }
}

fn contains_2x16h(dt: &Zoned) -> bool {
    if dt.hour() < 7 || dt.hour() == 23 {
        return false;
    }
    if dt.weekday() == Weekday::Saturday || dt.weekday() == Weekday::Sunday {
        true
    } else {
        NERC_CALENDAR.is_holiday(&dt.date())
    }
}

fn contains_7x8(dt: &Zoned) -> bool {
    dt.hour() < 7 || dt.hour() == 23
}

#[cfg(test)]
mod tests {

    use jiff::civil::date;

    use crate::{bucket::*, elec::iso::ISONE, interval::{month, term::Term}};

    #[test]
    fn test_bucket_atc() {
        let dt = date(2022, 1, 1)
            .at(0, 0, 0, 0)
            .in_tz("America/New_York")
            .unwrap();
        assert!(Bucket::Atc.contains(&dt));
        assert!(Bucket::Atc.name() == "ATC");
        assert_eq!(parse_bucket("Flat"), Ok(Bucket::Atc));
        assert_eq!("ATC".parse::<Bucket>(), Ok(Bucket::Atc));
    }

    #[test]
    fn test_bucket_5x16() {
        let term = Term::new(date(2025, 1, 1), date(2025, 1, 31))
            .unwrap()
            .with_tz(&ISONE.tz);
        let hours = Bucket::B5x16.count_hours(&term);
        assert_eq!(hours, 352);
    }

    #[test]
    fn test_bucket_5x16_a() {
        let term = "Cal 25".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let months = term.months();
        for month in months {
            println!("Month: {:?}", month);
            // let hours = Bucket::B5x16.count_hours(&month);
            // assert_eq!(hours, 352);
        }
        let hours = Bucket::B5x16.count_hours(&term);
        assert_eq!(hours, 4080);
    }
}
