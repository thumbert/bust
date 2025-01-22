use jiff::{
    civil::{self as jc, date, Date, DateTime},
    ToSpan,
};
// use pest::error::Error;
use pest::{iterators::Pair, Parser};
use std::fmt::Formatter;

use std::{error::Error, fmt, str::FromStr};

use super::term::{ParseError, Rule, TermParser};

#[inline]
pub const fn month(year: i16, month: i8) -> Month {
    Month::constant(year, month)
}

/// A civil Month structure (not timezone aware)
#[derive(Clone, Copy, PartialEq)]
pub struct Month {
    // start_datetime: jc::DateTime,
    start_date: jc::Date,
}

impl Month {
    /// Creates a new `Month` value in a `const` context.
    ///
    /// # Panics
    ///
    /// This routine panics when the given year-month-01 does not correspond
    /// to a valid date.  Namely, all of the following must be true:
    ///
    /// * The year must be in the range `-9999..=9999`.
    /// * The month must be in the range `1..=12`.
    ///
    #[inline]
    pub const fn constant(year: i16, month: i8) -> Month {
        let start = Date::constant(year, month, 1);
        Month { start_date: start }
    }

    pub fn containing(datetime: jc::DateTime) -> Month {
        Month {
            start_date: jc::date(datetime.year(), datetime.month(), 1),
        }
    }

    pub fn start(&self) -> jc::DateTime {
        self.start_date.at(0, 0, 0, 0)
    }

    pub fn end(&self) -> jc::DateTime {
        self.start_date.saturating_add(1.month()).at(0, 0, 0, 0)
    }

    pub fn start_date(&self) -> jc::Date {
        self.start_date
    }

    pub fn end_date(&self) -> jc::Date {
        self.start_date.last_of_month()
    }

    pub fn days(&self) -> Vec<jc::Date> {
        let end = self.end_date();
        self.start_date()
            .series(1.day())
            .take_while(|e| e < &end)
            .collect()
    }

    pub fn next(&self) -> Month {
        Month {
            start_date: self.start_date.saturating_add(1.month()),
        }
    }

    pub fn previous(&self) -> Month {
        Month {
            start_date: self.start_date.saturating_sub(1.month()),
        }
    }

    pub fn is_after(&self, start: Month) -> Result<bool, Box<dyn Error>> {
        let span = start.start_date.until(self.start_date)?;
        // println!("{}", span.get_days());
        Ok(span.get_days() > 0)
    }

    pub fn up_to(&self, end: Month) -> Result<Vec<Month>, Box<dyn Error>> {
        let mut res: Vec<Month> = Vec::new();
        if self.is_after(end)? {
            return Err("input month is before self".into());
        }
        let mut current = *self;
        while current != end {
            res.push(current);
            current = current.next();
        }
        res.push(current);
        Ok(res)
    }

    /// Jump forward (or backwards) a number of months.
    pub fn add(&self, n: i32) -> Result<Month, Box<dyn Error>> {
        Ok(Month {
            start_date: self.start_date.checked_add(n.months())?,
        })
    }
}

impl fmt::Display for Month {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.write_str(&self.start_date.strftime("%Y-%m").to_string())
    }
}

impl fmt::Debug for Month {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.start_date.strftime("%Y-%m"))
    }
}

impl FromStr for Month {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parse_month(s) {
            Ok(month) => Ok(month),
            Err(_) => Err(ParseError(format!("Failed parsing {} as a month", s))),
        }
    }
}

/// Parse various formats for a month:
/// "Apr23", "J23", "April2023", "4/2023", "4/23", "2023-04"
fn parse_month(input: &str) -> Result<Month, Box<dyn Error>> {
    let token = TermParser::parse(Rule::month, input)?.next().unwrap();
    let record = token.into_inner().next().unwrap();
    match record.as_rule() {
        Rule::month_iso => process_month_iso(record), // "2023-04"
        // Rule::month_txt => process_month_txt(record),  // "Apr23", "APR23", "April2023"
        // Rule::month_abb => process_month_abb(record),  // "J23"
        // Rule::month_us => process_month_us(record),    // "4/2023", "4/23"
        _ => unreachable!(),
    }
}

/// Parse "2023-03" like strings.    
fn process_month_iso(token: Pair<'_, Rule>) -> Result<Month, Box<dyn Error>> {
    let v: Vec<_> = token.as_str().split('-').collect();
    // println!("v={:?}", v);
    let year = v[0].parse::<i16>().unwrap();
    let m = v[1].parse::<i8>().unwrap();
    let dt = jc::Date::new(year, m, 1)?;
    Ok(Month { start_date: dt })
}

/// Parse "Apr23" or "August2024" like strings.
/// - month > month_txt
///   - mon > feb: "Feb"
///   - yy: "23"
// fn process_month_txt(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
//     let mut record = token.into_inner().into_iter();
//     let m = match record
//         .next()
//         .unwrap()
//         .into_inner()
//         .next()
//         .unwrap()
//         .as_rule()
//     {
//         Rule::jan => 1,
//         Rule::feb => 2,
//         Rule::mar => 3,
//         Rule::apr => 4,
//         Rule::may => 5,
//         Rule::jun => 6,
//         Rule::jul => 7,
//         Rule::aug => 8,
//         Rule::sep => 9,
//         Rule::oct => 10,
//         Rule::nov => 11,
//         Rule::dec => 12,
//         _ => unreachable!(),
//     };
//     let year = match record.next() {
//         Some(y) => match y.as_rule() {
//             Rule::year => y.as_str().parse::<i32>().unwrap(),
//             Rule::yy => y.as_str().parse::<i32>().unwrap() + 2000, // no more 1900!
//             _ => unreachable!(),
//         },
//         None => unreachable!(),
//     };
//     Ok(TermType::Month(year, m))
// }

/// Parse "Z23" like strings.
/// - month > month_abb
///   - abb: "Z"
///   - yy: "23"
// fn process_month_abb(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
//     if token.as_rule() != Rule::month_abb {
//         panic!("Expecting Rule::abb, got {:?}", token.as_rule());
//     }
//     let m = match token.as_str().chars().next().unwrap() {
//         'F' => 1,
//         'G' => 2,
//         'H' => 3,
//         'J' => 4,
//         'K' => 5,
//         'M' => 6,
//         'N' => 7,
//         'Q' => 8,
//         'U' => 9,
//         'V' => 10,
//         'X' => 11,
//         'Z' => 12,
//         _ => unreachable!(),
//     };
//     let mut record = token.into_inner();

//     let year = match record.next() {
//         Some(y) => match y.as_rule() {
//             Rule::year => y.as_str().parse::<i32>().unwrap(),
//             Rule::yy => y.as_str().parse::<i32>().unwrap() + 2000, // no more 1900!
//             _ => unreachable!(),
//         },
//         None => unreachable!(),
//     };
//     Ok(TermType::Month(year, m))
// }

/// Parse "4/28", "04/2028", etc.  This parser will fail on incorrect months, e.g. "15/2028".
// fn process_month_us(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
//     if token.as_rule() != Rule::month_us {
//         panic!("Expecting Rule::month_us, got {:?}", token.as_rule());
//     }
//     let v: Vec<_> = token.as_str().split('/').collect();
//     // println!("v={:?}", v);
//     let m = v[0].parse::<u32>().unwrap();
//     let mut record = token.into_inner();

//     let year = match record.next() {
//         Some(y) => match y.as_rule() {
//             Rule::year => y.as_str().parse::<i32>().unwrap(),
//             Rule::yy => y.as_str().parse::<i32>().unwrap() + 2000, // no more 1900!
//             _ => unreachable!(),
//         },
//         None => unreachable!(),
//     };
//     Ok(TermType::Month(year, m))
// }

#[cfg(test)]
mod tests {
    use std::error::Error;

    use jiff::civil::DateTime;

    use super::{month, Month};

    #[test]
    fn test_month() -> Result<(), Box<dyn Error>> {
        let month = Month::containing("2024-03-15".parse::<DateTime>()?);
        assert_eq!(month.start(), "2024-03-01".parse::<DateTime>()?);
        assert_eq!(month.end(), "2024-04-01".parse::<DateTime>()?);
        assert_eq!(format!("{}", month), "2024-03");
        // let month = "2024-07".parse::<Month>()?;
        // println!("{}",month);
        // println!("{:?}", month.days());
        Ok(())
    }

    #[test]
    fn test_up_to() -> Result<(), Box<dyn Error>> {
        let start = month(2024, 9);
        let end = month(2025, 2);
        assert!(end.is_after(start)?);

        let months = start.up_to(end)?;
        assert_eq!(months.len(), 6);
        assert_eq!(months.first(), Some(&month(2024, 9)));
        assert_eq!(months.last(), Some(&month(2025, 2)));

        let months = start.up_to(start)?;
        assert_eq!(months.len(), 1);
        assert_eq!(months.first(), Some(&month(2024, 9)));

        Ok(())
    }

    #[test]
    fn test_add() -> Result<(), Box<dyn Error>> {
        let start = month(2024, 9);
        assert_eq!(start.add(12)?, month(2025, 9));
        assert_eq!(start.add(-12)?, month(2023, 9));
        assert_eq!(start.add(0)?, month(2024, 9));

        Ok(())
    }
}
