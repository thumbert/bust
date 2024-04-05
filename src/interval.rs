pub mod hour;
pub mod month;

use chrono::{DateTime, Datelike, Duration, NaiveTime, TimeZone, Timelike};
use std::fmt;
use std::fmt::{Debug, Formatter, Write};
use std::ops::Add;

use chrono_tz::Tz;

use crate::interval::hour::Hour;
//use self::hour::Hour;

// pub trait IntervalLike<T: TimeZone> {
//     fn start(self: &Self) -> DateTime<T>;
//     fn end(self: &Self) -> DateTime<T>;
//     // fn contains(&self, dt: DateTime<Tz>) -> bool {
//     //     dt >= self.start() && dt < self.end()
//     // }

//     // /// Split this interval into whole hours
//     // fn hours(&self) -> Vec<Hour> {
//     //     let mut out: Vec<Hour> = Vec::new();
//     //     let mut dt = Hour::containing(self.start()).end();
//     //     while dt < self.end() {
//     //         out.push(Hour{start: dt});
//     //         dt = dt + Duration::hours(1);
//     //     }
//     //     out
//     // }
// }

pub trait IntervalLike {
    fn start(self: &Self) -> DateTime<Tz>;
    fn end(self: &Self) -> DateTime<Tz>;
    fn contains(&self, dt: DateTime<Tz>) -> bool {
        dt >= self.start() && dt < self.end()
    }

    // /// Split this interval into whole hours
    fn hours(&self) -> Vec<Hour> {
        let mut out: Vec<Hour> = Vec::new();
        let trunc_start = self.start()
            .with_time(NaiveTime::from_hms_opt(self.start().hour(), 0, 0).unwrap()).unwrap();
        let mut dt = self.start();
        if dt > trunc_start {
            dt = trunc_start + Duration::hours(1);
        }
        let end = self.end();
        while dt < end {
            out.push(Hour { start: dt });
            dt = dt + Duration::hours(1);
        }
        // check if you overshot
        if out.last().unwrap().end() > self.end() {
            out.pop().unwrap();
        }
        out
    }
}

/// How to make sure the same tz value is used?

#[derive(PartialEq, Debug)]
pub struct Interval {
    start: DateTime<Tz>,
    end: DateTime<Tz>,
}

// #[derive(PartialEq, Debug)]
// pub struct Interval2<T: TimeZone> {
//     start: DateTime<T>,
//     end: DateTime<T>,
// }

// impl<T: TimeZone> Interval2<T> {
//     fn with_start(start: DateTime<T>, duration: Duration) -> Interval2<T> {
//         let start2 = start.clone();
//         let end = start2.checked_add_signed(duration).unwrap();
//         Interval2{start: start.clone(), end: end}
//     }
// }

impl Interval {
    fn with_start(start: DateTime<Tz>, duration: Duration) -> Interval {
        let start2 = start.clone();
        let end = start2.checked_add_signed(duration).unwrap();
        Interval {
            start: start.clone(),
            end: end,
        }
    }
}

impl IntervalLike for Interval {
    fn start(&self) -> DateTime<Tz> {
        self.start.clone()
    }
    fn end(&self) -> DateTime<Tz> {
        self.end.clone()
    }
}

// impl<T: TimeZone> IntervalLike<T> for Interval2<T> {
//     fn start(&self) -> DateTime<T> {
//         self.start.clone()
//     }
//     fn end(&self) -> DateTime<T> {
//         self.end.clone()
//     }
// }

#[cfg(test)]
mod tests {
    // use tests::month::Month;

    use chrono_tz::America::New_York;
    use tests::month::Month;

    // use chrono::{Timelike, Utc};
    use crate::interval::*;
    // use crate::interval::Interval::Hour;

    #[test]
    fn test_interval() {
        let start = New_York.with_ymd_and_hms(2022, 1, 1, 3, 15, 20).unwrap();
        let end = Tz::UTC.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let interval = Interval {
            start: start,
            end: end,
        };
        // let i2 = Interval2::with_start(start, Duration::days(365));
        println!("{:?}", interval);
    }

    #[test]
    fn split_hours() {
        let month = Month::new(2024, 1, New_York);
        let hours = month.hours();
        // println!("{:?}", hours);
        assert_eq!(hours.len(), 744);
    }
}
