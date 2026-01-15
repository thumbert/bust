use std::{error::Error, str::FromStr};

use jiff::{
    civil::{date, Date},
    tz::TimeZone,
    ToSpan, Zoned,
};
use serde::{Deserialize, Serialize};

use crate::interval::interval_base::{DateExt, IntervalTzLike};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DateTz(Zoned);

impl PartialOrd for DateTz {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        self.0.partial_cmp(&other.0)
    }
}

impl DateTz {
    pub fn containing(zoned: &Zoned) -> Self {
        DateTz(zoned.start_of_day().unwrap())
    }

    pub fn year(&self) -> i16 {
        self.0.year()
    }

    pub fn month(&self) -> i8 {
        self.0.month()
    }

    pub fn day(&self) -> i8 {
        self.0.day()
    }

    pub fn next(&self) -> DateTz {
        DateTz(self.0.saturating_add(1.day()))
    }

    pub fn previous(&self) -> DateTz {
        DateTz(self.0.saturating_sub(1.day()))
    }

    pub fn to_date(&self) -> Date {
        date(self.year(), self.month(), self.day())
    }

    /// Inclusive of the end date.
    pub fn up_to(&self, end: DateTz) -> Result<Vec<DateTz>, Box<dyn Error>> {
        let mut res: Vec<DateTz> = Vec::new();
        if self > &end {
            return Err("input date is before self".into());
        }
        let mut current = self.clone();
        while current != end {
            res.push(current.clone());
            current = current.next();
        }
        res.push(current);
        Ok(res)
    }
}

impl IntervalTzLike for DateTz {
    fn start(&self) -> Zoned {
        self.0.clone()
    }
    fn end(&self) -> Zoned {
        self.0.saturating_add(1.day())
    }
}

impl FromStr for DateTz {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ps = s.split('[').collect::<Vec<&str>>();
        let date = ps[0].parse::<Date>();
        if date.is_err() {
            return Err(format!("Failed parsing {} as a Date", s));
        }
        let date = date.unwrap();
        let tz_str = if ps.len() > 1 {
            ps[1].trim_end_matches(']')
        } else {
            return Err(format!("No time zone found in DateTz string {}", s));
        };
        let tz = TimeZone::get(tz_str);
        if tz.is_err() {
            return Err(format!(
                "Failed getting time zone {} in DateTz string {}",
                tz_str, s
            ));
        }
        Ok(date.with_tz(&tz.unwrap()))
    }
}

#[cfg(test)]
mod tests {
    use jiff::{civil::date, tz::TimeZone};

    use crate::interval::interval_base::*;

    #[test]
    fn test_date() {
        let tz = TimeZone::get("America/New_York").unwrap();
        let dt = date(2022, 1, 1).with_tz(&tz);
        assert_eq!(dt.next(), date(2022, 1, 2).with_tz(&tz));
        assert_eq!(dt.previous(), date(2021, 12, 31).with_tz(&tz));
    }
}
