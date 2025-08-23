use std::error::Error;
use std::str::FromStr;

use jiff::{
    civil::{date, Date, DateTime},
    tz::TimeZone,
    SpanCompare, ToSpan,
};
// use super::interval::Interval;
// use pest::error::Error;
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;
use thiserror::Error;

use crate::interval::{
    interval::{DateExt, IntervalLike},
    month::{process_month, process_month_abb, process_month_txt, process_month_us},
    term_tz::TermTz,
};

#[derive(Parser)]
#[grammar = "grammars/term.pest"]
pub struct TermParser;

#[derive(Error, Debug)]
#[error("{0}")]
pub struct ParseError(pub String);

#[derive(Clone, Copy, Debug, PartialEq, PartialOrd, Eq)]
pub struct Term {
    start: Date,
    end: Date,
}

impl Term {
    pub fn new(start: Date, end: Date) -> Option<Term> {
        if end < start {
            return None;
        }
        Some(Term { start, end })
    }

    pub fn with_tz(&self, tz: &TimeZone) -> TermTz {
        TermTz {
            start_date: self.start.with_tz(tz),
            end_date: self.end.with_tz(tz),
        }
    }

    /// Determine if this term is a single day.
    pub fn is_day(&self) -> bool {
        self.start.day() == self.end.day()
            && self.start.month() == self.end.month()
            && self.start.year() == self.end.year()
    }

    /// Determine if this term is a full calendar month.
    pub fn is_month(&self) -> bool {
        self.start.day() == 1
            && self.end.day() == self.end.last_of_month().day()
            && self.start.month() == self.end.month()
            && self.start.year() == self.end.year()
    }

    /// Determine if this term is a month range.
    pub fn is_month_range(&self) -> bool {
        if self.start.day() != 1 {
            return false;
        }
        if self.end.day() != self.end.last_of_month().day() {
            return false;
        }
        if self.start == self.end.first_of_month() {
            return false;
        }
        true
    }

    /// Determine if this term is a full calendar year.
    pub fn is_year(&self) -> bool {
        self.start.day() == 1
            && self.end.day() == 31
            && self.start.month() == 1
            && self.end.month() == 12
            && self.start.year() == self.end.year()
    }

    /// Determine if this term is a year range.
    pub fn is_year_range(&self) -> bool {
        self.start.day() == 1
            && self.end.day() == 31
            && self.start.month() == 1
            && self.end.month() == 12
            && self.start.year() < self.end.year()
    }

    /// Determining a TermType is a pretty expensive operation.  Use accordingly.
    /// FIXME
    pub fn term_type(&self) -> TermType {
        let mut res = TermType::DayRange;
        // if self.start().first_of_year().unwrap() == self.start()
        //     && self.start().last_of_year().unwrap() == self.end()
        // {
        //     if self.start.year() == self.end.year() {
        //         return TermType::Year;
        //     } else {
        //         return TermType::YearRange;
        //     }
        // }

        // if self.start().day() == 1 && self.end() == self.start().last_of_month()

        if self.start.start().day() == 1 && self.end.end().day() == 1 {
            if self.start.start().month() == self.end.end().month() {
                TermType::Month
            } else {
                TermType::MonthRange
            }
        } else {
            TermType::DayRange
        }
    }
}

impl IntervalLike for Term {
    fn start(&self) -> DateTime {
        self.start.start()
    }
    fn end(&self) -> DateTime {
        self.end.end()
    }
}

#[derive(Debug, PartialEq)]
pub enum TermType {
    Day,
    DayRange,
    Month,
    MonthRange,
    Quarter,
    QuarterRange,
    Year,
    YearRange,
}

impl FromStr for Term {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parse_term(s) {
            Ok(term) => Ok(term),
            Err(e) => Err(e),
        }
    }
}

impl From<Date> for Term {
    fn from(day: Date) -> Self {
        Term::new(day, day).unwrap()
    }
}

// pub enum TokenType {
//     MonthOfYear(u32),
//     DayOfMonth(u32),
//     Year(i32),
//     Month((i32, u32)),
// }

// impl FromStr for MonthTz {
//     type Err = ParseError;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         let (year, month) = match parse_month(s) {
//             Ok(TermType::Month(year, month)) => (year, month),
//             Ok(_) => unreachable!(),
//             Err(_) => return Err(ParseError(format!("Failed parsing {} as a month", s))),
//         };
//         if month > 12 {
//             return Err(ParseError(format!("Month of year {} > 12", month)));
//         }
//         Ok(MonthTz::new(year, month, Tz::UTC).unwrap())
//     }
// }

// impl FromStr for Interval {
//     type Err = ParseError;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         if let Ok(term) = parse_term(s) {
//             let interval = match term {
//                 TermType::Day(_, _, _) => todo!(),
//                 TermType::DayRange => todo!(),
//                 TermType::Month(year, month) => {
//                     let start = Tz::UTC.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap();
//                     let month = MonthTz::containing(start);
//                     Interval {
//                         start,
//                         end: month.end(),
//                     }
//                 }
//                 TermType::MonthRange(p1, p2) => {
//                     let start = Tz::UTC.with_ymd_and_hms(p1.0, p1.1, 1, 0, 0, 0).unwrap();
//                     let dt2 = Tz::UTC.with_ymd_and_hms(p2.0, p2.1, 1, 0, 0, 0).unwrap();
//                     let month = MonthTz::containing(dt2);
//                     let end = month.end();
//                     // if end < start {
//                     //     return
//                     // }
//                     Interval { start, end }
//                 }
//                 TermType::Quarter(year, quarter) => {
//                     let start = Tz::UTC
//                         .with_ymd_and_hms(year, 3 * (quarter - 1) + 1, 1, 0, 0, 0)
//                         .unwrap();
//                     let end = if quarter < 4 {
//                         Tz::UTC
//                             .with_ymd_and_hms(year, 3 * quarter + 1, 1, 0, 0, 0)
//                             .unwrap()
//                     } else {
//                         Tz::UTC.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).unwrap()
//                     };
//                     Interval { start, end }
//                 }
//                 TermType::QuarterRange => todo!(),
//                 TermType::Year(year) => {
//                     let start = Tz::UTC.with_ymd_and_hms(year, 1, 1, 0, 0, 0).unwrap();
//                     let end = Tz::UTC.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).unwrap();
//                     Interval { start, end }
//                 }
//                 TermType::YearRange(_, _) => todo!(),
//             };
//             Ok(interval)
//         } else {
//             Err(ParseError(format!("Failed parsing {} as a term", s)))
//         }
//     }
// }

pub fn parse_term(input: &str) -> Result<Term, ParseError> {
    let token = TermParser::parse(Rule::term, input);
    let term = match token {
        Ok(mut token) => token.next().unwrap(),
        Err(_) => return Err(ParseError(format!("failed to parse {}", input))),
    };
    // println!("term as rule ={:?}", term.as_rule());
    let record = term.into_inner().next().unwrap();
    // println!("record as rule ={:?}", record.as_rule());
    match record.as_rule() {
        Rule::EOI => Err(ParseError(format!("failed to parse {}", input))),
        Rule::simple => process_simple(record),
        Rule::range => process_range(record),
        _ => unreachable!(),
    }
}

fn process_simple(pair: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = pair.into_inner().next().unwrap();
    match record.as_rule() {
        Rule::cal => process_cal(record),
        Rule::month => match process_month(record) {
            Ok(month) => Ok(Term::from(month)),
            Err(e) => Err(ParseError(format!("failed to parse month: {}", e))),
        },
        Rule::quarter => process_quarter(record),
        Rule::day => match process_day(record) {
            Ok(day) => Ok(Term::from(day)),
            Err(e) => Err(ParseError(format!("failed to parse day: {}", e))),
        },
        _ => unreachable!(),
    }
}

fn process_day(token: Pair<'_, Rule>) -> Result<Date, ParseError> {
    let record = token.into_inner().next().unwrap();
    match record.as_rule() {
        Rule::day_iso => process_day_iso(record), // "2023-04-15"
        Rule::day_txt => process_day_txt(record), // "15Apr23", "15APR23", "15April2023"
        Rule::day_us => process_day_us(record),   // "4/15/2023", "4/15/23"
        _ => unreachable!(),
    }
}

fn process_day_iso(token: Pair<'_, Rule>) -> Result<Date, ParseError> {
    let date_str = token.as_str();
    let parts: Vec<&str> = date_str.split('-').collect();
    if parts.len() != 3 {
        return Err(ParseError(format!("invalid ISO date format: {}", date_str)));
    }
    let year = parts[0]
        .parse::<i16>()
        .map_err(|_| ParseError(format!("invalid year in ISO date: {}", date_str)))?;
    let month = parts[1]
        .parse::<i8>()
        .map_err(|_| ParseError(format!("invalid month in ISO date: {}", date_str)))?;
    let day = parts[2]
        .parse::<i8>()
        .map_err(|_| ParseError(format!("invalid day in ISO date: {}", date_str)))?;
    Ok(date(year, month, day))
}

fn process_day_txt(token: Pair<'_, Rule>) -> Result<Date, ParseError> {
    let mut record = token.into_inner();
    let day = match record.next() {
        Some(dd) => match dd.as_rule() {
            Rule::dd => dd
                .as_str()
                .parse::<i8>()
                .map_err(|_| ParseError(format!("invalid day number: {}", dd.as_str())))?,
            _ => unreachable!()
        },
        _ => return Err(ParseError("expected day number".to_string())),
    };
    let m = match record
        .next()
        .unwrap()
        .into_inner()
        .next()
        .unwrap()
        .as_rule()
    {
        Rule::jan => 1,
        Rule::feb => 2,
        Rule::mar => 3,
        Rule::apr => 4,
        Rule::may => 5,
        Rule::jun => 6,
        Rule::jul => 7,
        Rule::aug => 8,
        Rule::sep => 9,
        Rule::oct => 10,
        Rule::nov => 11,
        Rule::dec => 12,
        _ => unreachable!(),
    };
    let year = match record.next() {
        Some(y) => match y.as_rule() {
            Rule::year => y.as_str().parse::<i16>().unwrap(),
            Rule::yy => y.as_str().parse::<i16>().unwrap() + 2000, // no more 1900!
            _ => unreachable!(),
        },
        None => unreachable!(),
    };
    Date::new(year, m, day)
        .map_err(|_| ParseError(format!("invalid date: {}-{:02}-{:02}", year, m, day)))
}

#[allow(dead_code)]
fn parse_day_txt(input: &str) -> Result<Date, ParseError> {
    let token = TermParser::parse(Rule::day_txt, input)
        .unwrap()
        .next()
        .unwrap();
    process_day_txt(token)
}

fn process_day_us(token: Pair<'_, Rule>) -> Result<Date, ParseError> {
    let date_str = token.as_str();
    let parts: Vec<&str> = date_str.split('/').collect();
    if parts.len() != 3 {
        return Err(ParseError(format!(
            "invalid US date format: {}.  Expected mm/dd/yyyy.",
            date_str
        )));
    }
    let month = parts[0]
        .parse::<i8>()
        .map_err(|_| ParseError(format!("invalid month in US date: {}", date_str)))?;
    let day = parts[1]
        .parse::<i8>()
        .map_err(|_| ParseError(format!("invalid day in US date: {}", date_str)))?;
    let mut year = parts[2]
        .parse::<i16>()
        .map_err(|_| ParseError(format!("invalid year in US date: {}", date_str)))?;
    if year < 100 {
        year += 2000; // assume 21st century for two-digit years
    } 
    Ok(date(year, month, day))
}

fn process_cal(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let next = token.into_inner().next().unwrap();
    let year = match next.as_rule() {
        Rule::year => next.as_str().parse::<i16>().unwrap(),
        Rule::yy => next.as_str().parse::<i16>().unwrap() + 2000,
        _ => unreachable!(),
    };
    let start = date(year, 1, 1);
    let end = date(year, 12, 31);
    Ok(Term { start, end })
}

/// Parse "Q2, 24", "Q3 24", "Q2, 2024", "Q3 2024" strings
fn process_quarter(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let q = token
        .as_str()
        .chars()
        .nth(1)
        .unwrap()
        .to_string()
        .parse::<i8>()
        .unwrap();
    let next = token.into_inner().next().unwrap();
    let year = match next.as_rule() {
        Rule::year => next.as_str().parse::<i16>().unwrap(),
        Rule::yy => next.as_str().parse::<i16>().unwrap() + 2000,
        _ => unreachable!(),
    };
    let start = date(year, (q - 1) * 3 + 1, 1);
    let end = date(year, q * 3 + 1, 1).yesterday().unwrap();
    Ok(Term { start, end })
}

fn process_range(pair: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = pair.into_inner().next().unwrap();
    match record.as_rule() {
        Rule::range_month => process_range_month(record),
        Rule::range_day => process_range_day(record),
        _ => unreachable!(),
    }
}

pub fn process_range_day(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = token.into_inner().next().unwrap();
    match record.as_rule() {
        Rule::range_day_txt => process_range_day_txt(record), // "15Apr23-20Aug27"
        Rule::range_day_us => process_range_day_us(record),   // "4/15/2023-8/20/27"
        _ => unreachable!(),
    }
}

fn process_range_day_txt(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = token.into_inner().collect::<Vec<_>>();
    let r1 = record[0].as_rule();
    let r2 = record[1].as_rule();
    match (r1, r2) {
        (Rule::day_txt, Rule::day_txt) => {
            let day1 = process_day_txt(record[0].clone())?;
            let day2 = process_day_txt(record[1].clone())?;
            if day1 >= day2 {
                return Err(ParseError(format!(
                    "invalid day range: {} - {}.  Start day should be before end date!",
                    day1, day2
                )));
            }
            Ok(Term::new(day1, day2).unwrap())
        }
        _ => unreachable!(),
    }
}

fn process_range_day_us(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = token.into_inner().collect::<Vec<_>>();
    let r1 = record[0].as_rule();
    let r2 = record[1].as_rule();
    match (r1, r2) {
        (Rule::day_us, Rule::day_us) => {
            let day1 = process_day_us(record[0].clone())?;
            let day2 = process_day_us(record[1].clone())?;
            if day1 >= day2 {
                return Err(ParseError(format!(
                    "invalid day range: {} - {}.  Start day should be before end date!",
                    day1, day2
                )));
            }
            Ok(Term::new(day1, day2).unwrap())
        }
        _ => unreachable!(),
    }
}


pub fn process_range_month(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = token.into_inner().next().unwrap();
    match record.as_rule() {
        Rule::range_month_abb => process_range_month_abb(record), // "J23"
        Rule::range_month_txt => process_range_month_txt(record), // "Apr23", "APR23", "April2023"
        Rule::range_month_us => process_range_month_us(record),   // "4/2023", "4/23"
        _ => unreachable!(),
    }
}

#[allow(dead_code)]
fn parse_range_month_abb(input: &str) -> Result<Term, ParseError> {
    let token = TermParser::parse(Rule::range_month_abb, input)
        .unwrap()
        .next()
        .unwrap();
    process_range_month_abb(token)
}

#[allow(dead_code)]
fn parse_range_month_us(input: &str) -> Result<Term, ParseError> {
    let token = TermParser::parse(Rule::range_month_us, input)
        .unwrap()
        .next()
        .unwrap();
    process_range_month_us(token)
}

#[allow(dead_code)]
fn parse_range_month_txt(input: &str) -> Result<Term, ParseError> {
    let token = TermParser::parse(Rule::range_month_txt, input)
        .unwrap()
        .next()
        .unwrap();
    process_range_month_txt(token)
}

fn process_range_month_abb(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = token.into_inner().collect::<Vec<_>>();
    let r1 = record[0].as_rule();
    let r2 = record[1].as_rule();

    match (r1, r2) {
        (Rule::month_abb, Rule::month_abb) => {
            let month1 = process_month_abb(record[0].clone())?;
            let month2 = process_month_abb(record[1].clone())?;
            if month1.start_date() > month2.end_date() {
                return Err(ParseError(format!(
                    "Invalid month range: {} - {}.  Start month is after end month!",
                    month1, month2
                )));
            }
            Ok(Term::new(month1.start_date(), month2.end_date()).unwrap())
        }
        _ => unreachable!(),
    }
}

fn process_range_month_txt(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = token.into_inner().collect::<Vec<_>>();
    let r1 = record[0].as_rule();
    let r2 = record[1].as_rule();

    match (r1, r2) {
        (Rule::month_txt, Rule::month_txt) => {
            let month1 = process_month_txt(record[0].clone())?;
            let month2 = process_month_txt(record[1].clone())?;
            if month1.start_date() > month2.end_date() {
                return Err(ParseError(format!(
                    "Invalid month range: {} - {}.  Start month is after end month!",
                    month1, month2
                )));
            }
            Ok(Term::new(month1.start_date(), month2.end_date()).unwrap())
        }
        _ => unreachable!(),
    }
}

fn process_range_month_us(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = token.into_inner().collect::<Vec<_>>();
    let r1 = record[0].as_rule();
    let r2 = record[1].as_rule();

    match (r1, r2) {
        (Rule::month_us, Rule::month_us) => {
            let month1 = process_month_us(record[0].clone())?;
            let month2 = process_month_us(record[1].clone())?;
            if month1.start_date() > month2.end_date() {
                return Err(ParseError(format!(
                    "Invalid month range: {} - {}.  Start month is after end month!",
                    month1, month2
                )));
            }
            Ok(Term::new(month1.start_date(), month2.end_date()).unwrap())
        }
        _ => unreachable!(),
    }
}

#[cfg(test)]
mod tests {

    use crate::interval::term::*;
    use pest::Parser;

    #[test]
    fn test_term_type() {
        assert!(parse_term("12Feb24").unwrap().is_day());
        assert!(parse_term("Feb24").unwrap().is_month());
        assert!(parse_term("Feb24-Aug26").unwrap().is_month_range());
        assert!(parse_term("2024").unwrap().is_year());
        assert!(parse_term("2024").unwrap().is_month_range());
        // assert!(parse_term("Cal24-Cal26").unwrap().is_year_range());
    }

    #[test]
    fn test_parse_term() {
        let vs = [
            (
                "15Apr24",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2024, 4, 15),
                },
            ),
            (
                "2024-04-15",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2024, 4, 15),
                },
            ),
            (
                "4/15/2024",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2024, 4, 15),
                },
            ),
            (
                "04/15/2024",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2024, 4, 15),
                },
            ),
            (
                "Apr24",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
            ),
            (
                "Apr 24",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
            ),
            (
                "Apr 2024",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
            ),
            (
                "April2024",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
            ),
            (
                "April 2024",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
            ),
            (
                "J24",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
            ),
            (
                "4/24",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
            ),
            (
                "4/2024",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
            ),
            (
                "2024-04",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
            ),
            (
                "Q3, 2024",
                Term {
                    start: date(2024, 7, 1),
                    end: date(2024, 9, 30),
                },
            ),
            (
                "Q3, 24",
                Term {
                    start: date(2024, 7, 1),
                    end: date(2024, 9, 30),
                },
            ),
            (
                "Q3 24",
                Term {
                    start: date(2024, 7, 1),
                    end: date(2024, 9, 30),
                },
            ),
            //
            (
                "Cal 24",
                Term {
                    start: date(2024, 1, 1),
                    end: date(2024, 12, 31),
                },
            ),
            (
                "Cal 2024",
                Term {
                    start: date(2024, 1, 1),
                    end: date(2024, 12, 31),
                },
            ),
            (
                "2024",
                Term {
                    start: date(2024, 1, 1),
                    end: date(2024, 12, 31),
                },
            ),
            //
            (
                "15Apr24-20Aug28",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2028, 8, 20),
                },
            ),
            (
                "4/15/24-8/20/28",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2028, 8, 20),
                },
            ),
            (
                "4/15/24 - 8/20/28",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2028, 8, 20),
                },
            ),
            //
            (
                "Apr24-Aug28",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2028, 8, 31),
                },
            ),
            (
                "Apr24 - May 25",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2025, 5, 31),
                },
            ),
            (
                "J24-Q28",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2028, 8, 31),
                },
            ),
            (
                "J24 - Q28",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2028, 8, 31),
                },
            ),
            (
                "4/24-8/28",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2028, 8, 31),
                },
            ),
        ];
        for e in vs {
            // println!("{:?}", e);
            assert_eq!(parse_term(e.0).ok().unwrap(), e.1);
        }
    }

    #[test]
    fn test_parse_fails() {
        assert!(parse_term("2024-18").is_err()); // wrong month
        assert!(parse_term("J26-Q24").is_err()); // end date before start date
    }

    #[test]
    fn test_parse_day() {
        // let token = TermParser::parse(Rule::day_us, "4/15/2024")
        //     .unwrap()
        //     .next()
        //     .unwrap();
        // let term = process_day_us(token);
        // println!("{:?}", term); 
        assert_eq!(parse_day_txt("15Apr24").unwrap(), date(2024, 4, 15));
    }

    #[test]
    fn test_parse_range_month() {
        assert_eq!(
            parse_range_month_txt("Apr24-Aug26").unwrap(),
            Term {
                start: date(2024, 4, 1),
                end: date(2026, 8, 31),
            }
        );
        assert_eq!(
            parse_range_month_abb("J24-Q26").unwrap(),
            Term {
                start: date(2024, 4, 1),
                end: date(2026, 8, 31),
            }
        );
        assert_eq!(
            parse_range_month_us("4/24-8/26").unwrap(),
            Term {
                start: date(2024, 4, 1),
                end: date(2026, 8, 31),
            }
        );
    }

    #[test]
    fn test_grammar_term() {
        // month
        assert!(TermParser::parse(Rule::term, "Jan23").is_ok());
        assert!(TermParser::parse(Rule::term, "JAN23").is_ok());
        assert!(TermParser::parse(Rule::term, "JaN23").is_ok());
        assert!(TermParser::parse(Rule::term, "January23").is_ok());
        assert!(TermParser::parse(Rule::term, "Jan2023").is_ok());
        assert!(TermParser::parse(Rule::term, "Janu2023").is_err());
        // year
        assert!(TermParser::parse(Rule::term, "2023").is_ok());
        assert!(TermParser::parse(Rule::term, "23").is_err());
    }

    #[test]
    fn test_parse_simple() {
        let token = TermParser::parse(Rule::simple, "Cal 24")
            .unwrap()
            .next()
            .unwrap();
        let term = process_simple(token).unwrap();
        assert_eq!(
            term,
            Term {
                start: date(2024, 1, 1),
                end: date(2024, 12, 31),
            },
        )
    }

    #[test]
    fn test_parse_cal() {
        let token = TermParser::parse(Rule::cal, "Cal 24")
            .unwrap()
            .next()
            .unwrap();
        let term = process_cal(token).unwrap();
        assert_eq!(
            term,
            Term {
                start: date(2024, 1, 1),
                end: date(2024, 12, 31),
            },
        )
    }
}
