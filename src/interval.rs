use std::fmt;
use std::fmt::{Debug, Formatter, write, Write};
use chrono::{Datelike, DateTime, TimeZone};

#[derive(Debug)]
pub struct Interval<T: TimeZone> {
    start: DateTime<T>,
    end: DateTime<T>,
}

impl<T: TimeZone> Interval<T> {
    fn new(start: DateTime<T>, end: DateTime<T>) -> Interval<T> {
        if end >= start {
            return Interval {start, end};
        } else {
            panic!("Can't initiate Interval because end is before start!");
        }
    }
}

impl<T: TimeZone> fmt::Display for Interval<T> {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_char('[')?;
        std::fmt::Display::fmt(&self.start.naive_local(), f)?;
        self.start.offset().fmt(f)?;
        f.write_str(" -> ")?;
        std::fmt::Display::fmt(&self.end.naive_local(), f)?;
        &self.end.offset().fmt(f)?;
        f.write_char(')')
    }
}

#[cfg(test)]
mod tests {
    use chrono::Utc;
    use crate::interval::*;

    #[test]
    fn test_interval() {
        let start = Utc.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap();
        let end = Utc.with_ymd_and_hms(2023, 1, 1, 0, 0, 0).unwrap();
        let ival = Interval::new(start, end);
        println!("{}", ival);

        assert_eq!(true, true);
    }
}