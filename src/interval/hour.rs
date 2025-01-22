// use chrono::{DateTime, Duration, LocalResult, TimeZone};
// use chrono_tz::Tz;
// use std::fmt::{Debug, Formatter};
// use std::{fmt, panic};

// use super::interval::IntervalLike;


// #[derive(Debug, Clone, Eq, PartialEq, Hash)]
// pub struct Hour {
//     start: DateTime<Tz>,
// }

// impl Hour {
//     // Note that not all hours are representable this way (DST fall hour)
//     pub fn new(year: i32, month: u32, day: u32, hour: u32, tz: Tz) -> Option<Hour> {
//         let start = tz.with_ymd_and_hms(year, month, day, hour, 0, 0);
//         match start {
//             LocalResult::Single(start) => Some(Hour { start }),
//             LocalResult::Ambiguous(_, _) => panic!("Ambiguous combo of inputs!"),
//             LocalResult::None => None,
//         }
//     }

//     /// Return the hour that contains this datetime.
//     pub fn containing(dt: DateTime<Tz>) -> Hour {
//         let tz = dt.timezone();
//         let secs = dt.timestamp();
//         let start = tz.timestamp_opt(3600 * (secs / 3600), 0).unwrap();
//         Hour { start }
//     }

//     pub fn next(&self) -> Hour {
//         Hour { start: self.end() }
//     }
// }

// impl IntervalLike for Hour {
//     fn start(&self) -> DateTime<Tz> {
//         self.start
//     }
//     fn end(&self) -> DateTime<Tz> {
//         self.start + Duration::hours(1)
//     }
// }

// impl fmt::Display for Hour {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         f.write_str(&self.start.format("%Y-%m-%d %H %:z").to_string())
//     }
// }
// // }

// #[cfg(test)]
// mod tests {
//     use chrono::{Datelike, Duration, TimeZone, Timelike};
//     use chrono_tz::{America::New_York, Tz};

//     use crate::interval::{hour::Hour, interval::IntervalLike};

//     #[test]
//     fn test_hour_utc() {
//         let dt = Tz::UTC.with_ymd_and_hms(2022, 4, 15, 3, 15, 20).unwrap();
//         let hour = Hour::containing(dt);
//         assert_eq!(hour.start.hour(), 3);
//         assert_eq!(hour.start.day(), 15);
//         assert_eq!(hour.start.month(), 4);
//         assert_eq!(
//             hour.next(),
//             Hour {
//                 start: Tz::UTC.with_ymd_and_hms(2022, 4, 15, 4, 0, 0).unwrap()
//             }
//         );
//     }

//     #[test]
//     fn hour_ny() {
//         let hour = Hour::new(2023, 3, 1, 0, New_York).unwrap();
//         assert_eq!(format!("{}", hour), "2023-03-01 00 -05:00");
//         let hour = hour.next();
//         assert_eq!(format!("{}", hour), "2023-03-01 01 -05:00");
//         assert!(hour.contains(New_York.with_ymd_and_hms(2023, 3, 1, 1, 17, 24).unwrap()));
//     }

//     #[test]
//     fn hour_dst() {
//         let hour0 = Hour::new(2022, 11, 6, 0, New_York).unwrap();
//         assert_eq!(format!("{}", hour0), "2022-11-06 00 -04:00");
//         let hour1 = hour0.next();
//         assert_eq!(format!("{}", hour1), "2022-11-06 01 -04:00");
//         let hour2 = hour1.next();
//         assert_eq!(format!("{}", hour2), "2022-11-06 01 -05:00");
//         //
//         let dt = New_York.timestamp_opt(1667710800, 0).unwrap(); // "2022-11-06T01:16:40-04:00"
//         let hour = Hour::containing(dt);
//         assert_eq!(format!("{}", hour), "2022-11-06 01 -04:00");
//         let dt = dt + Duration::hours(1);
//         let hour = Hour::containing(dt);
//         assert_eq!(format!("{}", hour), "2022-11-06 01 -05:00");
//     }
// }
