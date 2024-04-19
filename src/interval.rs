pub mod hour;
pub mod month;

use chrono::{DateTime, Datelike, Duration, NaiveTime, TimeZone, Timelike};
use std::cmp;
use std::fmt;
use std::fmt::{Debug, Formatter, Write};
use std::ops::Add;

use chrono_tz::Tz;

use crate::interval::hour::Hour;

pub trait IntervalLike {
    fn start(&self) -> DateTime<Tz>;
    fn end(&self) -> DateTime<Tz>;
    fn contains(&self, dt: DateTime<Tz>) -> bool {
        dt >= self.start() && dt < self.end()
    }

    /// Split this interval into whole hours
    fn hours(&self) -> Vec<Hour> {
        let mut out: Vec<Hour> = Vec::new();
        let trunc_start = self
            .start()
            .with_time(NaiveTime::from_hms_opt(self.start().hour(), 0, 0).unwrap())
            .unwrap();
        let mut dt = self.start();
        if dt > trunc_start {
            dt = trunc_start + Duration::hours(1);
        }
        let end = self.end();
        while dt < end {
            out.push(Hour::containing(dt));
            dt += Duration::hours(1);
        }
        // check if you overshot
        if out.last().unwrap().end() > self.end() {
            out.pop().unwrap();
        }
        out
    }

    // Return the timezone of this interval
    fn timezone(&self) -> Tz {
        self.start().timezone()
    }
}

impl cmp::PartialEq for dyn IntervalLike {
    fn eq(&self, other: &Self) -> bool {
        self.start() == other.start() && self.end() == other.end()
    }
}

#[derive(PartialEq, Debug)]
pub struct Interval {
    start: DateTime<Tz>,
    end: DateTime<Tz>,
}

impl Interval {
    pub fn with_start_end(start: DateTime<Tz>, end: DateTime<Tz>) -> Option<Interval> {
        if start.timezone() != end.timezone() {
            return None;
        }
        if end < start {
            return None;
        }
        Some(Interval { start, end })
    }

    fn with_start(start: DateTime<Tz>, duration: Duration) -> Interval {
        let end = start.checked_add_signed(duration).unwrap();
        Interval { start, end }
    }
}

impl IntervalLike for Interval {
    fn start(&self) -> DateTime<Tz> {
        self.start
    }
    fn end(&self) -> DateTime<Tz> {
        self.end
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use crate::interval::month::Month;
    use crate::interval::*;
    use chrono::TimeDelta;
    use chrono_tz::America::New_York;
    use itertools::Itertools;

    #[test]
    fn test_interval() {
        let start = New_York.with_ymd_and_hms(2022, 1, 1, 3, 15, 20).unwrap();
        let end = Tz::UTC.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(Interval::with_start_end(start, end), None); // can't be different timezones
        let end = New_York.with_ymd_and_hms(2021, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(Interval::with_start_end(start, end), None); // can't be negative interval
        let end = New_York.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        assert_eq!(
            Interval::with_start_end(start, end).unwrap(),
            Interval { start, end }
        ); // works
    }

    #[test]
    fn split_hours() {
        let interval = Interval::with_start(
            New_York.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap(),
            TimeDelta::days(366),
        );
        let hours = interval.hours();
        let mut count: HashMap<Month, usize> = HashMap::new();
        for (key, value) in &hours.into_iter().group_by(|e| Month::containing(e.start())) {
            count.insert(key, value.count());
        }
        println!("{:#?}", count);

        // let month = Month::new(2022, 11, New_York).unwrap();
        // let hours = month.hours();
        // assert_eq!(hours.len(), 744);
    }

    // #[test]
    // fn visiblity_hour() {
    //     let hour = Hour::new(2024, 7, 1, 16, New_York);
    //     // let hour = Hour {start: New_York.with_ymd_and_hms(2024, 7, 14, 16, 0, 0).unwrap()};

    // }
}
