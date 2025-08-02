
use jiff::{civil::{Date, DateTime}, tz::TimeZone, ToSpan, Zoned};

use crate::interval::date_tz::DateTz;


pub trait DateExt {
    fn with_tz(&self, tz: &TimeZone) -> DateTz;

    fn up_to(&self, end: Self) -> Vec<Self>
    where
        Self: Sized;
}

impl DateExt for Date {
    fn with_tz(&self, tz: &TimeZone) -> DateTz {
        let dt = self.at(0, 0, 0, 0);
        DateTz::containing(dt.to_zoned(tz.clone()).unwrap())
    }

    fn up_to(&self, end: Self) -> Vec<Self> {
        let mut dates = Vec::new();
        let mut current = *self;
        while current <= end {
            dates.push(current);
            current = current.saturating_add(1.days());
        }
        dates
    }
}

impl IntervalLike for Date {
    fn start(&self) -> DateTime {
        self.at(0, 0, 0, 0)
    }
    fn end(&self) -> DateTime {
        self.at(0, 0, 0, 0) + 1.days()
    }
}

// use std::cmp;
// use std::fmt::Debug;


// use super::hour::Hour;
// use super::month_tz::MonthTz;

// // use crate::interval::hour::Hour;

// // use self::month_tz::MonthTz;

pub trait IntervalLike {
    // fn tz(&self) -> Tz {
    //     self.start().timezone()
    // }
    fn start(&self) -> DateTime;
    fn end(&self) -> DateTime;
    // fn contains(&self, dt: DateTime<Tz>) -> bool {
    //     dt >= self.start() && dt < self.end()
    // }

    // /// Split this interval into whole hours
    // fn hours(&self) -> Vec<Hour> {
    //     let mut out: Vec<Hour> = Vec::new();
    //     let trunc_start = self
    //         .start()
    //         .with_time(NaiveTime::from_hms_opt(self.start().hour(), 0, 0).unwrap())
    //         .unwrap();
    //     let mut dt = self.start();
    //     if dt > trunc_start {
    //         dt = trunc_start + Duration::hours(1);
    //     }
    //     let end = self.end();
    //     while dt < end {
    //         out.push(Hour::containing(dt));
    //         dt += Duration::hours(1);
    //     }
    //     // check if you overshot
    //     if out.last().unwrap().end() > self.end() {
    //         out.pop().unwrap();
    //     }
    //     out
    // }

    // // Return the timezone of this interval
    // fn timezone(&self) -> Tz {
    //     self.start().timezone()
    // }
}

pub trait IntervalTzLike {
    fn start(&self) -> Zoned;
    fn end(&self) -> Zoned;
    fn contains(&self, dt: Zoned) -> bool {
        dt >= self.start() && dt < self.end()
    }
}

pub struct IntervalTz {
    pub start: Zoned,
    pub end: Zoned,
}   

impl IntervalTz {
    pub fn new(start: Zoned, end: Zoned) -> Option<Self> {
        if start.time_zone() != end.time_zone() || end < start {
            return None;
        }
        Some(IntervalTz { start, end })
    }
}

impl IntervalTzLike for IntervalTz {
    fn start(&self) -> Zoned {
        self.start.clone()
    }
    fn end(&self) -> Zoned {
        self.end.clone()
    }
}

// impl cmp::PartialEq for dyn IntervalLike {
//     fn eq(&self, other: &Self) -> bool {
//         self.start() == other.start() && self.end() == other.end()
//     }
// }

// #[derive(PartialEq, Debug)]
// pub struct Interval {
//     pub(crate) start: DateTime<Tz>,
//     pub(crate) end: DateTime<Tz>,
// }

// impl Interval {
//     pub fn with_start_end(start: DateTime<Tz>, end: DateTime<Tz>) -> Option<Interval> {
//         if start.timezone() != end.timezone() {
//             return None;
//         }
//         if end < start {
//             return None;
//         }
//         Some(Interval { start, end })
//     }

//     pub fn with_start(start: DateTime<Tz>, duration: Duration) -> Interval {
//         let end = start.checked_add_signed(duration).unwrap();
//         Interval { start, end }
//     }

//     /// Make an interval that spans years, e.g. [2023-2026)
//     pub fn with_y(start: i32, end: i32, tz: Tz) -> Option<Interval> {
//         if start > end {
//             return None;
//         }
//         let start_dt = tz.with_ymd_and_hms(start, 1, 1, 0, 0, 0).unwrap();
//         let end_dt = tz.with_ymd_and_hms(end + 1, 1, 1, 0, 0, 0).unwrap();
//         Some(Interval {
//             start: start_dt,
//             end: end_dt,
//         })
//     }

//     /// Make an interval that spans months, e.g. [Feb23-Mar26)
//     pub fn with_ym(start: (i32, u32), end: (i32, u32), tz: Tz) -> Option<Interval> {
//         let start_m = MonthTz::new(start.0, start.1, tz).unwrap();
//         let end_m = MonthTz::new(end.0, end.1, tz).unwrap().next();
//         if start_m > end_m {
//             None
//         } else {
//             Some(Interval {
//                 start: start_m.start(),
//                 end: end_m.start(),
//             })
//         }
//     }
// }

// impl IntervalLike for Interval {
//     fn start(&self) -> DateTime<Tz> {
//         self.start
//     }
//     fn end(&self) -> DateTime<Tz> {
//         self.end
//     }
// }

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use jiff::civil::date;

//     use crate::interval::month_tz::MonthTz;
//     use crate::interval::*;
//     use chrono::TimeDelta;
//     use chrono_tz::America::New_York;
//     use chrono_tz::Tz;
//     use interval::{Interval, IntervalLike};
//     use itertools::Itertools;

    // #[test]
    // fn test_date_ext() {
    //     let date = date(2025, 1, 1);
    //     assert_eq!(
    //         Interval::with_start_end(start, end).unwrap(),
    //         Interval { start, end }
    //     ); // works
    // }

//     #[test]
//     fn test_special_constructors() {
//         // with_y
//         let term = Interval::with_y(2022, 2024, New_York).unwrap();
//         assert_eq!(
//             term.start,
//             New_York.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()
//         );
//         assert_eq!(
//             term.end,
//             New_York.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap()
//         );
//         // with_ym
//         let term = Interval::with_ym((2023, 2), (2026, 3), New_York).unwrap();
//         assert_eq!(
//             term.start,
//             New_York.with_ymd_and_hms(2023, 2, 1, 0, 0, 0).unwrap()
//         );
//         assert_eq!(
//             term.end,
//             New_York.with_ymd_and_hms(2026, 4, 1, 0, 0, 0).unwrap()
//         );
//     }

//     #[test]
//     fn split_hours() {
//         let interval = Interval::with_start(
//             New_York.with_ymd_and_hms(2024, 2, 1, 0, 0, 0).unwrap(),
//             TimeDelta::days(366),
//         );
//         let hours = interval.hours();
//         let mut count: HashMap<MonthTz, usize> = HashMap::new();
//         for (key, value) in &hours
//             .into_iter()
//             .group_by(|e| MonthTz::containing(e.start()))
//         {
//             count.insert(key, value.count());
//         }
//         println!("{:#?}", count);

//         // let month = Month::new(2022, 11, New_York).unwrap();
//         // let hours = month.hours();
//         // assert_eq!(hours.len(), 744);
//     }

//     // #[test]
//     // fn visiblity_hour() {
//     //     let hour = Hour::new(2024, 7, 1, 16, New_York);
//     //     // let hour = Hour {start: New_York.with_ymd_and_hms(2024, 7, 14, 16, 0, 0).unwrap()};

//     // }
}
