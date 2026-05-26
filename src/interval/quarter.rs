use jiff::{
    civil::{self as jc, Date},
    ToSpan,
};
use pest::{iterators::Pair, Parser};
use serde::{Deserialize, Deserializer, Serialize, Serializer};
use std::fmt::Formatter;

use std::{error::Error, fmt, str::FromStr};

use crate::interval::{interval_base::IntervalLike, term::Term};

use super::term::{ParseError, Rule, TermParser};

#[inline]
pub const fn quarter(year: i16, quarter: i8) -> Quarter {
    Quarter::constant(year, quarter)
}

/// A civil Month structure (not timezone aware)
#[derive(Clone, Copy, PartialEq, PartialOrd, Eq)]
pub struct Quarter {
    start_date: jc::Date,
}

impl Quarter {
    /// Creates a new `Quarter` value in a `const` context.
    ///
    /// # Panics
    ///
    /// This routine panics when the given year-quarter-01 does not correspond
    /// to a valid date.  Namely, all of the following must be true:
    ///
    /// * The year must be in the range `-9999..=9999`.
    /// * The quarter must be in the range `1..=12`.
    ///
    #[inline]
    pub const fn constant(year: i16, quarter: i8) -> Quarter {
        let start_month = (quarter - 1) * 3 + 1;
        let start = Date::constant(year, start_month, 1);
        Quarter { start_date: start }
    }

    
    pub fn containing(datetime: jc::DateTime) -> Quarter {
        let month = datetime.month();
        let quarter = (month - 1) / 3 + 1;
        Quarter {
            start_date: jc::date(datetime.year(), (quarter - 1) * 3 + 1, 1),
        }
    }
    
    pub fn year(&self) -> i16 {
        self.start_date.year()
    }

    pub fn quarter(&self) -> i8 {
        (self.start_date.month() - 1) / 3 + 1
    }
    
    pub fn start(&self) -> jc::DateTime {
        self.start_date.at(0, 0, 0, 0)
    }

    pub fn end(&self) -> jc::DateTime {
        self.start_date.saturating_add(3.month()).at(0, 0, 0, 0)
    }

    pub fn start_date(&self) -> jc::Date {
        self.start_date
    }

    pub fn end_date(&self) -> jc::Date {
        self.start_date.saturating_add(2.month()).last_of_month()
    }

    pub fn days(&self) -> Vec<jc::Date> {
        let end = self.end_date();
        self.start_date()
            .series(1.day())
            .take_while(|e| e <= &end)
            .collect()
    }

    pub fn term(&self) -> Term {
        Term::new(self.start_date, self.end_date()).unwrap()
    }

    pub fn next(&self) -> Quarter {
        Quarter {
            start_date: self.start_date.saturating_add(3.month()),
        }
    }

    pub fn previous(&self) -> Quarter {
        Quarter {
            start_date: self.start_date.saturating_sub(3.month()),
        }
    }

    /// Inclusive of the end quarter.
    pub fn up_to(&self, end: Quarter) -> Result<Vec<Quarter>, Box<dyn Error>> {
        let mut res: Vec<Quarter> = Vec::new();
        if self > &end {
            return Err("input quarter is before self".into());
        }
        let mut current = *self;
        while current != end {
            res.push(current);
            current = current.next();
        }
        res.push(current);
        Ok(res)
    }

    /// Jump forward (or backwards) a number of quarters.
    pub fn add(&self, n: i32) -> Result<Quarter, Box<dyn Error>> {
        Ok(Quarter {
            start_date: self.start_date.checked_add((n * 3).months())?,
        })
    }

    #[inline]
    pub fn strftime<'f, F: 'f + ?Sized + AsRef<[u8]>>(
        &self,
        format: &'f F,
    ) -> jiff::fmt::strtime::Display<'f> {
        self.start_date.strftime(format)
    }

    // pub fn with_tz(&self, tz: &str) -> MonthTz {
    //     MonthTz::containing(self.start().in_tz(tz).unwrap())
    // }
}

impl From<Quarter> for Term {
    fn from(q: Quarter) -> Self {
        let start = q.start_date;
        let end = q.end_date();
        Term::new(start, end).unwrap()
    }
}

impl fmt::Display for Quarter {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.start_date.strftime("%Y-Q%q").to_string())
    }
}

impl fmt::Debug for Quarter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.start_date.strftime("%Y-Q%q"))
    }
}

impl FromStr for Quarter {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parse_quarter(s) {
            Ok(quarter) => Ok(quarter),
            Err(_) => Err(ParseError(format!("Failed parsing {} as a quarter", s))),
        }
    }
}

impl IntervalLike for Quarter {
    fn start(&self) -> jc::DateTime {
        self.start_date.at(0, 0, 0, 0)
    }

    fn end(&self) -> jc::DateTime {
        self.start_date.saturating_add(3.month()).at(0, 0, 0, 0)
    }
}

impl Serialize for Quarter {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: Serializer,
    {
        let s = self.start_date.strftime("%Y-Q%q").to_string();
        serializer.serialize_str(&s)
    }
}

// Custom deserializer using FromStr so that Actix path path can parse different formats, e.g.
// "2025-03", "Mar25", etc.
impl<'de> Deserialize<'de> for Quarter {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Quarter::from_str(&s).map_err(serde::de::Error::custom)
    }
}

/// Parse various formats for a quarter:
/// "Q123", "Q1-23", "Q1-2023", "2023-Q1"
fn parse_quarter(input: &str) -> Result<Quarter, ParseError> {
    let token = TermParser::parse(Rule::quarter, input)
        .unwrap()
        .next()
        .unwrap();
    process_quarter(token)
}

pub fn process_quarter(token: Pair<'_, Rule>) -> Result<Quarter, ParseError> {
    let record = token.into_inner().next().unwrap();
    match record.as_rule() {
        Rule::quarter => process_quarter_iso(record), // "2023-Q1"
        // Rule::quarter_txt => process_quarter_txt(record), // "Q123", "Q1-23", "Q1-2023"
        _ => unreachable!(),
    }
}

/// Parse "2023-Q1" like strings.    
pub fn process_quarter_iso(token: Pair<'_, Rule>) -> Result<Quarter, ParseError> {
    let v: Vec<_> = token.as_str().split('-').collect();
    // println!("v={:?}", v);
    let year = v[0].parse::<i16>().unwrap();
    let m = v[1].parse::<i8>().unwrap();
    let dt = jc::Date::new(year, m, 1);
    match dt {
        Ok(dt) => Ok(Quarter { start_date: dt }),
        Err(e) => Err(ParseError(format!("{}", e))),
    }
}



#[cfg(test)]
mod tests {
    use std::error::Error;

    use jiff::civil::{date, DateTime};

    use super::*;

    #[test]
    fn test_basic() -> Result<(), Box<dyn Error>> {
        let q = Quarter::containing("2024-03-15".parse::<DateTime>()?);
        assert_eq!(q.start_date(), date(2024, 1, 1));
        assert_eq!(q.end_date(), date(2024, 3, 31));
        assert_eq!(quarter(2024, 1).days().len(), 91);
        Ok(())
    }

    #[test]
    fn test_parsing() -> Result<(), Box<dyn Error>> {
        assert_eq!("2024-Q3".parse::<Quarter>()?, quarter(2024, 3));
        Ok(())
    }

    #[test]
    fn test_formatting() -> Result<(), Box<dyn Error>> {
        assert_eq!(quarter(2024, 3).strftime("Q%q,%Y").to_string(), "Q3,2024");
        Ok(())
    }

    #[test]
    fn test_ordering() -> Result<(), Box<dyn Error>> {
        let start = quarter(2024, 3);
        assert!(start > quarter(2023, 3));
        assert!(start >= quarter(2023, 3));
        assert!(start < quarter(2025, 3));
        assert!(start <= quarter(2025, 3));
        assert!(start == quarter(2024, 3));
        Ok(())
    }
}
