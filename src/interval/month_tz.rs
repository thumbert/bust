// use std::fmt;
// use std::fmt::{Debug, Formatter};

// use super::interval::IntervalLike;

use jiff::{
    civil::{date, Date},
    ToSpan, Zoned,
};

use crate::interval::{date_tz::DateTz, interval::{DateExt, IntervalTzLike}};

#[derive(PartialEq, Debug, Clone, Hash, Eq, PartialOrd, Ord)]
pub struct MonthTz(Zoned);

pub fn month_tz(year: i16, month: i8, tz: &str) -> MonthTz {
    MonthTz::new(year, month, tz)
}

impl MonthTz {
    pub fn new(year: i16, month: i8, tz: &str) -> MonthTz {
        let start = date(year, month, 1).at(0, 0, 0, 0).in_tz(tz).unwrap();
        MonthTz(start)
    }

    pub fn containing(zoned: Zoned) -> Self {
        MonthTz(zoned.with().day(1).hour(0).minute(0).second(0).nanosecond(0).build().unwrap())
    }

    pub fn start_date(&self) -> DateTz {
        DateTz::containing(&self.start())
    }


    pub fn end_date(&self) -> DateTz {
        DateTz::containing(&self.end().checked_sub(1.day()).unwrap())    
    }

    // pub fn days(&self) -> Vec<DateTz> {
    //     let end = self.end_date();
    //     self.start_date()
    //         .series(1.day())
    //         .take_while(|e| e <= &end)
    //         .collect()
    // }

    pub fn next(&self) -> MonthTz {
        MonthTz(self.start().saturating_add(1.month()))
    }
}

impl IntervalTzLike for MonthTz {
    fn start(&self) -> Zoned {
        self.0.clone()
    }
    fn end(&self) -> Zoned {
        self.0.saturating_add(1.month())
    }
}

//     pub fn from_int(yyyymm: u32, tz: Tz) -> Option<MonthTz> {
//         let year = i32::try_from(yyyymm / 100).unwrap();
//         let month = yyyymm % 100;
//         let start = tz.with_ymd_and_hms(year, month, 1, 0, 0, 0);
//         match start {
//             LocalResult::Single(start) => Some(MonthTz { start }),
//             LocalResult::Ambiguous(_, _) => panic!("Wrong inputs!"),
//             LocalResult::None => None,
//         }
//     }

//     /// Return the hour that contains this datetime.
//     pub fn containing(dt: DateTime<Tz>) -> MonthTz {
//         let start = dt
//             .with_day(1)
//             .unwrap()
//             .with_hour(0)
//             .unwrap()
//             .with_minute(0)
//             .unwrap()
//             .with_second(0)
//             .unwrap();
//         MonthTz { start }
//     }

//     pub fn year(&self) -> i32 {
//         self.start.year()
//     }

//     pub fn month(&self) -> u32 {
//         self.start.month()
//     }

//     pub fn next(&self) -> MonthTz {
//         MonthTz { start: self.end() }
//     }

//     pub fn day_count(&self) -> usize {
//         usize::try_from(
//             self.next()
//                 .start
//                 .signed_duration_since(self.start)
//                 .num_days(),
//         )
//         .unwrap()
//     }

//     pub fn hour_count(&self) -> usize {
//         usize::try_from(
//             self.next()
//                 .start
//                 .signed_duration_since(self.start)
//                 .num_hours(),
//         )
//         .unwrap()
//     }
// }

// impl IntervalLike for MonthTz {
//     fn start(&self) -> DateTime<Tz> {
//         self.start
//     }

//     fn end(&self) -> DateTime<Tz> {
//         let month = self.start.month();
//         if month < 12 {
//             self.start.with_month(month + 1).unwrap()
//         } else {
//             self.start
//                 .with_year(self.start.year() + 1)
//                 .unwrap()
//                 .with_month(1)
//                 .unwrap()
//         }
//     }
// }

// impl fmt::Display for MonthTz {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         f.write_str(&self.start.format("%Y-%m").to_string())
//     }
// }

// #[cfg(test)]
// mod tests {
//     use crate::interval::{interval::IntervalLike, month_tz::MonthTz};
//     use chrono::{Datelike, Duration, TimeZone, Timelike};
//     use chrono_tz::{America::New_York, Tz};

//     #[test]
//     fn test_month_utc() {
//         let dt = Tz::UTC.with_ymd_and_hms(2022, 4, 15, 3, 15, 20).unwrap();
//         let month = MonthTz::containing(dt);
//         assert_eq!(month.start.hour(), 0);
//         assert_eq!(month.start.day(), 1);
//         assert_eq!(month.start.month(), 4);
//         // println!("{:?}", month.next());
//         assert_eq!(
//             month.next(),
//             MonthTz {
//                 start: Tz::UTC.with_ymd_and_hms(2022, 5, 1, 0, 0, 0).unwrap()
//             }
//         );
//         assert!(month.contains(dt));
//         assert!(!month.contains(dt + Duration::days(31)));
//         // assert_eq!(format!("{}", month), "2022-04Z");
//     }

//     #[test]
//     fn test_month_ny() {
//         let month = MonthTz::new(2024, 3, New_York).unwrap();
//         assert_eq!(month.year(), 2024);
//         assert_eq!(month.month(), 3);
//         assert_eq!(format!("{}", month), "2024-03");
//         let month = month.next();
//         assert_eq!(format!("{}", month), "2024-04");
//         assert_eq!(month.timezone(), New_York);
//         assert_eq!(MonthTz::from_int(202404, New_York).unwrap(), month);
//     }

//     #[test]
//     fn test_month_eq() {
//         let m1 = MonthTz::new(2024, 3, New_York).unwrap();
//         let m2 = MonthTz::new(2024, 4, New_York).unwrap();
//         let m3 = MonthTz::new(2024, 3, Tz::UTC).unwrap();
//         let m4 = m1.clone();
//         assert!(m1 != m2);
//         assert!(m1 != m3);
//         assert_eq!(m1, m4);
//     }

//     #[test]
//     fn test_count() {
//         let m = MonthTz::new(2024, 1, New_York).unwrap();
//         assert_eq!(m.hour_count(), 744);
//         let m = MonthTz::new(2024, 2, New_York).unwrap();
//         assert_eq!(m.hour_count(), 696);
//         let m = MonthTz::new(2024, 3, New_York).unwrap();
//         assert_eq!(m.hour_count(), 743); // DST
//         let m = MonthTz::new(2024, 11, New_York).unwrap();
//         assert_eq!(m.hour_count(), 721); // DST
//     }
// }
