use std::fmt;
use std::str::FromStr;

use jiff::{
    civil::{date, Date, DateTime},
    tz::TimeZone,
    ToSpan,
};
// use super::interval::Interval;
// use pest::error::Error;
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;
use thiserror::Error;

use crate::interval::{
    interval::{DateExt, IntervalLike},
    month::{month, process_month, process_month_abb, process_month_txt, process_month_us, Month},
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

    /// Return the days in the term
    pub fn days(&self) -> Vec<Date> {
        let mut days = Vec::new();
        let mut current = self.start;
        while current <= self.end {
            days.push(current);
            current = current.saturating_add(1.days());
        }
        days
    }

    /// Returns the months in this term.  If the term is not an exact month or
    /// month range, return the minimal vector of months that cover the term.   
    pub fn months(&self) -> Vec<Month> {
        let mut months = Vec::new();
        let mut current = month(self.start.year(), self.start.month());
        let end_month = month(self.end.year(), self.end.month());
        while current <= end_month {
            months.push(Month::containing(current.start()));
            let next =
                date(current.start().year(), current.start().month(), 1).saturating_add(1.months());
            current = Month::containing(next.start());
        }
        months
    }

    /// Returns the years in this term.  If the term is not an exact year or
    /// year range, return the minimal vector of years that cover the term.
    pub fn years(&self) -> Vec<i16> {
        let mut years = Vec::new();
        let mut current = self.start.year();
        let end = self.end.year();
        while current <= end {
            years.push(current);
            current += 1;
        }
        years
    }

    /// Determine the term type, pretty expensive operation.
    /// Go from the most specific to the most general.
    pub fn term_type(&self) -> TermType {
        if self.is_day() {
            return TermType::Day;
        }
        if self.is_month() {
            return TermType::Month;
        }
        if self.is_year() {
            return TermType::Year;
        }
        if self.is_year_range() {
            return TermType::YearRange;
        }
        if self.is_month_range() {
            return TermType::MonthRange;
        }
        TermType::DayRange
    }

    /// Determine if this term is a single day.
    pub fn is_day(&self) -> bool {
        self.start == self.end
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

    /// Determine if this term is a quarter.
    pub fn is_quarter(&self) -> bool {
        self.start.day() == 1
            && self.start.month() % 3 == 1
            && self.end.month() == self.start.month() + 2
            && self.end.day() == self.end.last_of_month().day()
    }

    /// Determine if this term is a quarter range.
    pub fn is_quarter_range(&self) -> bool {
        self.start.day() == 1
            && self.start.month() % 3 == 1
            && self.end.month() % 3 == 0
            && self.end.day() == self.end.last_of_month().day()
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
}

impl IntervalLike for Term {
    fn start(&self) -> DateTime {
        self.start.start()
    }
    fn end(&self) -> DateTime {
        self.end.end()
    }
}

impl fmt::Display for Term {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.is_day() {
            return write!(f, "{}", self.start);
        }
        if self.is_month() {
            return write!(f, "{}", self.start.strftime("%b%y"));
        }
        if self.is_month() {
            return write!(f, "{}", self.start.strftime("%b%y"));
        }
        if self.is_quarter() {
            return write!(
                f,
                "Q{},{}",
                (self.start.month() - 1) / 3 + 1,
                self.start.strftime("%y")
            );
        }
        if self.is_year() {
            return write!(f, "Cal{}", self.start.strftime("%y"));
        }
        if self.is_year_range() {
            return write!(
                f,
                "Cal{}-Cal{}",
                self.start.strftime("%y"),
                self.end.strftime("%y")
            );
        }
        if self.is_quarter_range() {
            return write!(
                f,
                "Q{},{}-Q{},{}",
                (self.start.month() - 1) / 3 + 1,
                self.start.strftime("%y"),
                (self.end.month() - 1) / 3 + 1,
                self.end.strftime("%y")
            );
        }
        if self.is_month_range() {
            return write!(
                f,
                "{}-{}",
                self.start.strftime("%b%y"),
                self.end.strftime("%b%y")
            );
        }
        write!(
            f,
            "{}{}-{}{}",
            self.start.day(),
            self.start.strftime("%b%y"),
            self.end.day(),
            self.end.strftime("%b%y")
        )
    }
}

#[derive(Debug, PartialEq)]
pub enum TermType {
    Day,
    DayRange,
    Month,
    MonthRange,
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
            _ => unreachable!(),
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
        Rule::range_day => process_range_day(record),
        Rule::range_month => process_range_month(record),
        Rule::range_quarter => process_range_quarter(record),
        Rule::range_cal => process_range_cal(record),
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

fn process_range_quarter(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = token.into_inner().collect::<Vec<_>>();
    let r1 = record[0].as_rule();
    let r2 = record[1].as_rule();
    match (r1, r2) {
        (Rule::quarter, Rule::quarter) => {
            let q1 = process_quarter(record[0].clone())?;
            let q2 = process_quarter(record[1].clone())?;
            if q1.start >= q2.end {
                return Err(ParseError(format!(
                    "invalid quarter range: {} - {}.  End needs to be after start!",
                    q1, q2
                )));
            }
            Ok(Term::new(q1.start, q2.end).unwrap())
        }
        _ => unreachable!(),
    }
}

fn process_range_cal(token: Pair<'_, Rule>) -> Result<Term, ParseError> {
    let record = token.into_inner().collect::<Vec<_>>();
    let r1 = record[0].as_rule();
    let r2 = record[1].as_rule();

    match (r1, r2) {
        (Rule::cal, Rule::cal) => {
            let cal1 = process_cal(record[0].clone())?;
            let cal2 = process_cal(record[1].clone())?;
            if cal1.start >= cal2.end {
                return Err(ParseError(format!(
                    "invalid year range: {} - {}.  End needs to be after start!",
                    cal1, cal2
                )));
            }
            Ok(Term::new(cal1.start, cal2.end).unwrap())
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
        assert!(parse_term("15Apr24").unwrap().is_day());
        assert!(parse_term("12Feb24").unwrap().is_day());
        assert!(parse_term("Feb24").unwrap().is_month());
        assert!(parse_term("Feb24-Aug26").unwrap().is_month_range());
        assert!(parse_term("2024").unwrap().is_year());
        assert!(parse_term("2024").unwrap().is_month_range());
        assert!(parse_term("Cal24").unwrap().is_year());
        assert!(parse_term("Cal24-Cal26").unwrap().is_year_range());
        assert!(parse_term("Q1,24").unwrap().is_quarter());
        assert!(parse_term("1Apr24-30Jun24").unwrap().is_quarter());
        assert!(!parse_term("1Feb24-31May24").unwrap().is_quarter());
        assert!(parse_term("Q2,24-Q3,24").unwrap().is_quarter_range());
        assert!(parse_term("1Apr24-30Sep24").unwrap().is_quarter_range());
    }

    #[test]
    fn test_fmt() {
        assert_eq!(
            "2024-04-15".parse::<Term>().unwrap().to_string(),
            "2024-04-15"
        );
        assert_eq!("2024-04".parse::<Term>().unwrap().to_string(), "Apr24");
        assert_eq!("Q2,24".parse::<Term>().unwrap().to_string(), "Q2,24");
        assert_eq!("Cal24".parse::<Term>().unwrap().to_string(), "Cal24");
        assert_eq!(
            "Cal24-Cal26".parse::<Term>().unwrap().to_string(),
            "Cal24-Cal26"
        );
        assert_eq!(
            "10Jan24-2Feb25".parse::<Term>().unwrap().to_string(),
            "10Jan24-2Feb25"
        );
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
                TermType::Day,
            ),
            (
                "2024-04-15",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2024, 4, 15),
                },
                TermType::Day,
            ),
            (
                "4/15/2024",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2024, 4, 15),
                },
                TermType::Day,
            ),
            (
                "04/15/2024",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2024, 4, 15),
                },
                TermType::Day,
            ),
            (
                "Apr24",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
                TermType::Month,
            ),
            (
                "Apr 24",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
                TermType::Month,
            ),
            (
                "Apr 2024",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
                TermType::Month,
            ),
            (
                "April2024",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
                TermType::Month,
            ),
            (
                "April 2024",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
                TermType::Month,
            ),
            (
                "J24",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
                TermType::Month,
            ),
            (
                "4/24",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
                TermType::Month,
            ),
            (
                "4/2024",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
                TermType::Month,
            ),
            (
                "2024-04",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2024, 4, 30),
                },
                TermType::Month,
            ),
            (
                "Q3, 2024",
                Term {
                    start: date(2024, 7, 1),
                    end: date(2024, 9, 30),
                },
                TermType::MonthRange,
            ),
            (
                "Q3, 24",
                Term {
                    start: date(2024, 7, 1),
                    end: date(2024, 9, 30),
                },
                TermType::MonthRange,
            ),
            (
                "Q3 24",
                Term {
                    start: date(2024, 7, 1),
                    end: date(2024, 9, 30),
                },
                TermType::MonthRange,
            ),
            //
            (
                "Cal 24",
                Term {
                    start: date(2024, 1, 1),
                    end: date(2024, 12, 31),
                },
                TermType::Year,
            ),
            (
                "Cal 2024",
                Term {
                    start: date(2024, 1, 1),
                    end: date(2024, 12, 31),
                },
                TermType::Year,
            ),
            (
                "2024",
                Term {
                    start: date(2024, 1, 1),
                    end: date(2024, 12, 31),
                },
                TermType::Year,
            ),
            //
            (
                "15Apr24-20Aug28",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2028, 8, 20),
                },
                TermType::DayRange,
            ),
            (
                "4/15/24-8/20/28",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2028, 8, 20),
                },
                TermType::DayRange,
            ),
            (
                "4/15/24 - 8/20/28",
                Term {
                    start: date(2024, 4, 15),
                    end: date(2028, 8, 20),
                },
                TermType::DayRange,
            ),
            //
            (
                "Apr24-Aug28",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2028, 8, 31),
                },
                TermType::MonthRange,
            ),
            (
                "Apr24 - May 25",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2025, 5, 31),
                },
                TermType::MonthRange,
            ),
            (
                "J24-Q28",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2028, 8, 31),
                },
                TermType::MonthRange,
            ),
            (
                "J24 - Q28",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2028, 8, 31),
                },
                TermType::MonthRange,
            ),
            (
                "4/24-8/28",
                Term {
                    start: date(2024, 4, 1),
                    end: date(2028, 8, 31),
                },
                TermType::MonthRange,
            ),
            (
                "Q1,24-Q3,25",
                Term {
                    start: date(2024, 1, 1),
                    end: date(2025, 9, 30),
                },
                TermType::MonthRange,
            ),
            (
                "Cal24-Cal25",
                Term {
                    start: date(2024, 1, 1),
                    end: date(2025, 12, 31),
                },
                TermType::YearRange,
            ),
        ];
        for e in vs {
            // println!("{:?}", e);
            assert_eq!(parse_term(e.0).ok().unwrap(), e.1);
            assert_eq!(e.1.term_type(), e.2);
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

    #[test]
    fn test_months() {
        let term = "Feb24-Aug24".parse::<Term>().unwrap();
        assert_eq!(term.months().len(), 7);
        let term = "14Feb24-3Aug24".parse::<Term>().unwrap();
        assert_eq!(term.months().len(), 7);
    }

    #[test]
    fn test_years() {
        let term = "Cal24".parse::<Term>().unwrap();
        assert_eq!(term.years(), vec![2024]);
        let term = "Cal24-Cal26".parse::<Term>().unwrap();
        assert_eq!(term.years(), vec![2024, 2025, 2026]);
        let term = "13Jan24-3Aug24".parse::<Term>().unwrap();
        assert_eq!(term.years(), vec![2024]);
    }
}
