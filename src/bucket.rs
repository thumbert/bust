use core::fmt;
use std::str::FromStr;

use jiff::{civil::Weekday, Zoned};
use serde::{Deserialize, Deserializer, Serialize};

use crate::{
    holiday::*,
    interval::{hour_tz::HourTz, interval_base::IntervalTzLike},
};

pub trait BucketLike {
    fn name(&self) -> String;
    fn contains(&self, datetime: &Zoned) -> bool;
    fn count_hours<K: IntervalTzLike>(&self, term: &K) -> i32;
}

#[derive(Copy, Clone, Debug, PartialEq, Eq, Serialize)]
pub enum Bucket {
    #[serde(rename = "ATC")]
    Atc,
    #[serde(rename = "5x16")]
    B5x16,
    #[serde(rename = "2x16H")]
    B2x16H,
    #[serde(rename = "7x8")]
    B7x8,
    #[serde(rename = "7x16")]
    B7x16,
    #[serde(rename = "Caiso1x16H")]
    Caiso1x16H,
    #[serde(rename = "Caiso6x16")]
    Caiso6x16,
    #[serde(rename = "Caiso7x8")]
    Caiso7x8,
    #[serde(rename = "CaisoOffpeak")]
    CaisoOffpeak,
    #[serde(rename = "Offpeak")]
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

impl fmt::Display for Bucket {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.name())
    }
}

// Custom deserializer using FromStr so that Actix path path can parse different casing, e.g.
// "ATC" and "atc", not only the canonical one "Atc".
impl<'de> Deserialize<'de> for Bucket {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Bucket::from_str(&s).map_err(serde::de::Error::custom)
    }
}

fn parse_bucket(s: &str) -> Result<Bucket, ParseError> {
    match s.to_uppercase().replace("_", "").as_str() {
        "FLAT" | "ATC" => Ok(Bucket::Atc),
        "5X16" | "PEAK" => Ok(Bucket::B5x16),
        "2X16H" => Ok(Bucket::B2x16H),
        "7X8" => Ok(Bucket::B7x8),
        "7X16" => Ok(Bucket::B7x16),
        "CAISO1X16H" => Ok(Bucket::Caiso1x16H),
        "CAISO6X16" | "CAISOPEAK" => Ok(Bucket::Caiso6x16),
        "CAISO7X8" => Ok(Bucket::Caiso7x8),
        "CAISOOFFPEAK" => Ok(Bucket::CaisoOffpeak),
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
            Bucket::B7x16 => String::from("7x16"),
            Bucket::Offpeak => String::from("Offpeak"),
            Bucket::Caiso1x16H => String::from("Caiso_1x16H"),
            Bucket::Caiso6x16 => String::from("Caiso_6x16"),
            Bucket::Caiso7x8 => String::from("Caiso_7x8"),
            Bucket::CaisoOffpeak => String::from("Caiso_Offpeak"),
        }
    }

    fn contains(&self, zoned: &Zoned) -> bool {
        match self {
            Bucket::Atc => true,
            Bucket::B5x16 => contains_5x16(zoned),
            Bucket::B2x16H => contains_2x16h(zoned),
            Bucket::B7x8 => contains_7x8(zoned),
            Bucket::B7x16 => contains_7x16(zoned),
            Bucket::Offpeak => !contains_5x16(zoned),
            Bucket::Caiso1x16H => contains_caiso_1x16h(zoned),
            Bucket::Caiso6x16 => contains_caiso_6x16(zoned),
            Bucket::Caiso7x8 => contains_caiso_7x8(zoned),
            Bucket::CaisoOffpeak => !contains_caiso_6x16(zoned),
        }
    }

    fn count_hours<K: IntervalTzLike>(&self, term: &K) -> i32 {
        let mut hour = HourTz::containing(&term.start());
        let last = HourTz::containing(&term.end());
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

fn contains_7x16(dt: &Zoned) -> bool {
    dt.hour() >= 7 && dt.hour() < 23
}

/// Caiso Peak bucket.  Weekdays 6am-10pm, excluding NERC holidays.
fn contains_caiso_6x16(dt: &Zoned) -> bool {
    if dt.weekday() == Weekday::Sunday {
        return false;
    }
    if dt.hour() < 6 || dt.hour() > 21 {
        false
    } else {
        !NERC_CALENDAR.is_holiday(&dt.date())
    }
}

fn contains_caiso_1x16h(dt: &Zoned) -> bool {
    if dt.hour() < 6 || dt.hour() > 21 {
        return false;
    }
    if dt.weekday() == Weekday::Sunday {
        true
    } else {
        NERC_CALENDAR.is_holiday(&dt.date())
    }
}

fn contains_caiso_7x8(dt: &Zoned) -> bool {
    dt.hour() < 6 || dt.hour() > 21
}



#[cfg(test)]
mod tests {
    use jiff::civil::date;

    use crate::{
        bucket::*,
        elec::iso::ISONE,
        interval::{term::Term, term_tz::TermTz},
    };

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

        let years = ["Cal 12", "Cal 13", "Cal 14", "Cal 15"]
            .iter()
            .map(|y| y.parse::<Term>().unwrap().with_tz(&ISONE.tz))
            .collect::<Vec<TermTz>>();
        let hours: Vec<i32> = years
            .iter()
            .map(|term| Bucket::Atc.count_hours(term))
            .collect();
        assert_eq!(hours, vec![8784, 8760, 8760, 8760]);
    }

    #[test]
    fn test_bucket_5x16() {
        let term = "Cal 12".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::B5x16.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![336, 336, 352, 336, 352, 336, 336, 368, 304, 368, 336, 320]
        );

        let term = "Cal 14".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::B5x16.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![352, 320, 336, 352, 336, 336, 352, 336, 336, 368, 304, 352]
        );

        let term = "Cal 15".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::B5x16.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![336, 320, 352, 352, 320, 352, 368, 336, 336, 352, 320, 352]
        );
    }

    #[test]
    fn test_bucket_2x16h() {
        let term = "Cal 12".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::B2x16H.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![160, 128, 144, 144, 144, 144, 160, 128, 176, 128, 144, 176]
        );

        let term = "Cal 13".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::B2x16H.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![144, 128, 160, 128, 144, 160, 144, 144, 160, 128, 160, 160]
        );
    }

    #[test]
    fn test_bucket_7x8() {
        let term = "Cal 12".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::B7x8.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![248, 232, 247, 240, 248, 240, 248, 248, 240, 248, 241, 248]
        );

        let term = "Cal 13".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::B7x8.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![248, 224, 247, 240, 248, 240, 248, 248, 240, 248, 241, 248]
        );
    }

    #[test]
    fn test_bucket_7x16() {
        let term = "Cal 12".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::B7x16.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![496, 464, 496, 480, 496, 480, 496, 496, 480, 496, 480, 496]
        );
    }

    #[test]
    fn test_bucket_offpeak() {
        let term = "Cal 12".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::Offpeak.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![408, 360, 391, 384, 392, 384, 408, 376, 416, 376, 385, 424]
        );

        let term = "Cal 13".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        let hours: Vec<i32> = term
            .months()
            .iter()
            .map(|m| Bucket::Offpeak.count_hours(m))
            .collect();
        assert_eq!(
            hours,
            vec![392, 352, 407, 368, 392, 400, 392, 392, 400, 376, 401, 408]
        );
    }
}
