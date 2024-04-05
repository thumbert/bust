use std::fmt;
use std::fmt::{Debug, Formatter};
use chrono::{DateTime, Duration, TimeZone, Timelike};
use chrono_tz::Tz;

use super::IntervalLike;


// #[derive(Debug, Clone, PartialEq)]
// pub struct Hour<T: TimeZone> {
//     pub start: DateTime<T>,
// }

#[derive(Debug, Clone, PartialEq)]
pub struct Hour {
    pub start: DateTime<Tz>,
}


impl Hour {
    /// Return the hour that contains this datetime.
    pub fn containing(dt: DateTime<Tz>) -> Hour {
        let start = dt.with_minute(0).unwrap().with_second(0).unwrap();
        Hour {start}
    }

    pub fn next(&self) -> Hour {
        Hour {start: self.end()}
    }
}

impl IntervalLike for Hour {
    fn start(&self) -> DateTime<Tz> {
        self.start.clone()
    }

    fn end(&self) -> DateTime<Tz> {
        self.start.clone() + Duration::hours(1)    
    }
}


// impl<T: TimeZone> Hour<T> {
//     /// Return the hour that contains this datetime.
//     pub fn containing(dt: DateTime<T>) -> Hour<T> {
//         let start = dt.with_minute(0).unwrap().with_second(0).unwrap();
//         Hour {start}
//     }

//     pub fn next(&self) -> Hour<T> {
//         Hour {start: self.end()}
//     }
// }

// impl<T: TimeZone> IntervalLike<T> for Hour<T> {
//     fn start(&self) -> DateTime<T> {
//         self.start.clone()
//     }

//     fn end(&self) -> DateTime<T> {
//         self.start.clone() + Duration::hours(1)    
//     }
// }


impl fmt::Display for Hour {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // write!(f, "{}-{:02}", self.year(), self.month()).unwrap();
        // std::fmt::Display::fmt(&self.start.naive_local(), f)?;
        // self.start.offset().fmt(f)
        f.write_str(&self.start.format("%Y-%m-%d %H %:z").to_string())
    }
}




#[cfg(test)]
mod tests {
    use super::*;
    use chrono::{Datelike, Timelike, TimeZone};
    // use chrono_tz::America::New_York;
    // use crate::interval::*;
    // use crate::interval::Interval::Hour;

    #[test]
    fn test_hour_utc() {
        let dt = Tz::UTC.with_ymd_and_hms(2022, 4, 15, 3, 15, 20).unwrap();
        let hour = Hour::containing(dt);
        // println!("{:?}", hour);
        // println!("{}", hour);
        assert_eq!(hour.start.hour(), 3);
        assert_eq!(hour.start.day(), 15);
        assert_eq!(hour.start.month(), 4);
        // println!("{:?}", hour.next());
        assert_eq!(hour.next(),
                   Hour{start: Tz::UTC.with_ymd_and_hms(2022, 4, 15, 4, 0, 0).unwrap()});
        // assert!(hour.contains(dt));
    }

}

