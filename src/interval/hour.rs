use std::fmt;
use std::fmt::{Debug, Formatter, Write};
use chrono::{Datelike, DateTime, Duration, Timelike, TimeZone};


#[derive(PartialEq)]
#[derive(Debug)]
pub struct Hour<T: TimeZone> {
    start: DateTime<T>,
}

impl<T: TimeZone> Hour<T> {
    /// Return the hour that contains this datetime.
    fn from(dt: DateTime<T>) -> Hour<T> {
        let start = dt.with_minute(0).unwrap().with_second(0).unwrap();
        Hour {start}
    }

    fn end(&self) -> DateTime<T> {
        self.start.clone() + Duration::hours(1)    
    }

    fn next(&self) -> Hour<T> {
        Hour {start: self.end()}
    }

    fn contains(&self, dt: DateTime<T>) -> bool {
        dt >= self.start && dt < self.end()
    }

}

impl<T: TimeZone> fmt::Display for Hour<T> where
    T::Offset: std::fmt::Display,{
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
    use chrono::{Datelike, Timelike, TimeZone, Utc};
    use chrono_tz::America::New_York;
    // use crate::interval::*;
    // use crate::interval::Interval::Hour;

    #[test]
    fn test_hour_utc() {
        let dt = Utc.with_ymd_and_hms(2022, 4, 15, 3, 15, 20).unwrap();
        let hour = Hour::from(dt);
        // println!("{:?}", hour);
        // println!("{}", hour);
        assert_eq!(hour.start.hour(), 3);
        assert_eq!(hour.start.day(), 15);
        assert_eq!(hour.start.month(), 4);
        // println!("{:?}", hour.next());
        assert_eq!(hour.next(),
                   Hour{start: Utc.with_ymd_and_hms(2022, 4, 15, 4, 0, 0).unwrap()});
        assert!(hour.contains(dt));
        // assert!(!month.contains(dt + Duration::days(31)));
        // assert_eq!(format!("{}", month), "2022-04Z");
    }

    // #[test]
    // fn test_month_ny() {
    //     let month = Month::from(New_York.with_ymd_and_hms(2022, 4, 15, 3, 15, 20).unwrap());
    //     println!("{}", month);
    // }

}

