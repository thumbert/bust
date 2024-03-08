use std::fmt;
use std::fmt::{Debug, Formatter, Write};
use chrono::{Datelike, DateTime, Duration, Timelike, TimeZone};


#[derive(PartialEq)]
#[derive(Debug)]
pub struct Month<T: TimeZone> {
    start: DateTime<T>,
}

impl<T: TimeZone> Month<T> {
    /// Return the hour that contains this datetime.
    fn from(dt: DateTime<T>) -> Month<T> {
        let start = dt.with_day(1).unwrap().with_hour(0).unwrap().with_minute(0).unwrap().with_second(0).unwrap();
        Month {start}
    }

    fn year(&self) -> i32 {
        self.start.year()
    }

    fn month(&self) -> u32 {
        self.start.month()
    }

    fn end(&self) -> DateTime<T> {
        let month = self.start.month();
        if month < 12 {
            self.start.with_month(month + 1).unwrap()
        } else {
            self.start.with_year(self.start.year()+1).unwrap().with_month(1).unwrap()
        }
    }

    fn next(&self) -> Month<T> {
        Month {start: self.end()}
    }

    fn contains(&self, dt: DateTime<T>) -> bool {
        dt >= self.start && dt < self.end()
    }

}

impl<T: TimeZone> fmt::Display for Month<T> where
    T::Offset: std::fmt::Display,{
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        // write!(f, "{}-{:02}", self.year(), self.month()).unwrap();
        // std::fmt::Display::fmt(&self.start.naive_local(), f)?;
        // self.start.offset().fmt(f)
        f.write_str(&self.start.format("%Y-%m %:z").to_string())

        // f.write_str(" -> ")?;
        // std::fmt::Display::fmt(&self.end.naive_local(), f)?;
        // self.end.offset().fmt(f)?;
        // f.write_char(')')
    }
}




#[cfg(test)]
mod tests {
    use chrono::{Datelike, Duration, Timelike, TimeZone, Utc};
    use chrono_tz::America::New_York;
    use crate::interval::month::Month;
    // use crate::interval::*;
    // use crate::interval::Interval::Hour;

    #[test]
    fn test_month_utc() {
        let dt = Utc.with_ymd_and_hms(2022, 4, 15, 3, 15, 20).unwrap();
        let month = Month::from(dt);
        // println!("{:?}", hour);
        assert_eq!(month.start.hour(), 0);
        assert_eq!(month.start.day(), 1);
        assert_eq!(month.start.month(), 4);
        // println!("{:?}", month.next());
        assert_eq!(month.next(),
                   Month{start: Utc.with_ymd_and_hms(2022, 5, 1, 0, 0, 0).unwrap()});
        assert!(month.contains(dt));
        assert!(!month.contains(dt + Duration::days(31)));
            // assert_eq!(format!("{}", month), "2022-04Z");
    }

    #[test]
    fn test_month_ny() {
        let month = Month::from(New_York.with_ymd_and_hms(2022, 4, 15, 3, 15, 20).unwrap());
        // println!("{}", month);
    }

}

