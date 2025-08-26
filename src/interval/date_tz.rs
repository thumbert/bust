use std::error::Error;

use jiff::{civil::Date, ToSpan, Zoned};

use crate::interval::interval::IntervalTzLike;

#[derive(Clone, Debug, PartialEq)]
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

#[cfg(test)]
mod tests {
    use jiff::{civil::{date, Time}, tz::TimeZone};

    use crate::interval::interval::*;

    #[test]
    fn test_date() {
        let tz = TimeZone::get("America/New_York").unwrap();
        let dt = date(2022, 1, 1).with_tz(&tz);
        assert_eq!(dt.next(), date(2022, 1, 2).with_tz(&tz));
        assert_eq!(
            dt.previous(),
            date(2021, 12, 31).with_tz(&tz)
        );
    }
}
