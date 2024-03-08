mod hour;
mod month;

use std::fmt;
use std::fmt::{Debug, Formatter, Write};
use std::ops::Add;
use chrono::{Datelike, DateTime, Duration, Timelike, TimeZone};

// pub enum Interval<T: TimeZone> {
//     Instant {start: DateTime<T>},
//     Hour(Hour<T>),
//     Date {start: DateTime<T>},
//     // Month {start: DateTime<T>},
//     Year {start: DateTime<T>},
// }


#[derive(PartialEq)]
#[derive(Debug)]
pub struct Interval<T: TimeZone> {
    start: DateTime<T>,
    end: DateTime<T>,
}

impl<T: TimeZone> Interval<T> {
    
    /// Return the hour that contains this datetime.
    fn from(start: &DateTime<T>, duration: Duration) -> Interval<T> {
        let end = start.checked_add_signed(duration).unwrap();
        Interval{start: start.clone(), end: end}
    }

    fn contains(&self, dt: DateTime<T>) -> bool {
        dt >= self.start && dt < self.end
    }
}



// #[derive(PartialEq)]
// #[derive(Debug)]
// pub struct Hour<T: TimeZone> {
//     start: DateTime<T>,
// }

// impl<T: TimeZone> Hour<T> {
//     /// Return the hour that contains this datetime.
//     fn from(dt: DateTime<T>) -> Hour<T> {
//         let start = dt.with_minute(0).unwrap().with_second(0).unwrap();
//         Hour{start}
//     }

//     fn end(&self) -> DateTime<T> {
//         self.start.clone() + Duration::hours(1)
//     }

//     fn next(&self) -> Hour<T> {
//         Hour{start: self.end()}
//     }

//     fn contains(&self, dt: DateTime<T>) -> bool {
//         dt >= self.start && dt < self.end()
//     }
// }


#[cfg(test)]
mod tests {
    use chrono::{Timelike, Utc};
    use crate::interval::*;
    // use crate::interval::Interval::Hour;

    #[test]
    fn test_interval() {
        let start = Utc.with_ymd_and_hms(2022, 1, 1, 3, 15, 20).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let interval = Interval{start: start, end: end};
        println!("{:?}", interval);



        // let hour = Hour::from(dt);
        // println!("{:?}", hour);
        // assert_eq!(hour.start.hour(), 3);
        // assert_eq!(hour.start.minute(), 0);
        // assert_eq!(hour.end().hour(), 4);
        // println!("{:?}", hour.next());
        // assert_eq!(hour.next(),
        //            Hour{start: Utc.with_ymd_and_hms(2022, 1, 1, 4, 0, 0).unwrap()});
        // assert!(hour.contains(dt));
        // assert!(!hour.contains(dt + Duration::hours(1)));
    }

    // #[test]
    // fn test_interval() {
    //     let start = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
    //     let end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
    //     let ival = Interval::new(start, end);
    //     println!("{}", ival);
    //
    //     assert_eq!(true, true);
    // }
}



// #[derive(Debug)]
// pub struct Interval<T: TimeZone> {
//     start: DateTime<T>,
//     end: DateTime<T>,
// }
//
// impl<T: TimeZone> Interval<T> {
//     fn new(start: DateTime<T>, end: DateTime<T>) -> Interval<T> {
//         if end >= start {
//             Interval {start, end}
//         } else {
//             panic!("Can't initiate Interval because end is before start!");
//         }
//     }
// }
//
// impl<T: TimeZone> fmt::Display for Interval<T> {
//     fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
//         f.write_char('[')?;
//         std::fmt::Display::fmt(&self.start.naive_local(), f)?;
//         self.start.offset().fmt(f)?;
//         f.write_str(" -> ")?;
//         std::fmt::Display::fmt(&self.end.naive_local(), f)?;
//         self.end.offset().fmt(f)?;
//         f.write_char(')')
//     }
// }
