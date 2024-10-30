use std::str::FromStr;

use super::IntervalLike;
use super::{month_tz::MonthTz, Interval};
use chrono::TimeZone;
use chrono_tz::Tz;
use pest::error::Error;
use pest::iterators::Pair;
use pest::Parser;
use pest_derive::Parser;
use thiserror::Error;

#[derive(Parser)]
#[grammar = "grammars/term.pest"]
pub struct TermParser;

#[derive(Error, Debug)]
#[error("{0}")]
pub struct ParseError(pub String);

#[derive(Debug, PartialEq)]
pub enum TermType {
    Day(i32, u32, u32),
    DayRange,
    Month(i32, u32),
    MonthRange((i32, u32), (i32, u32)),
    Quarter(i32, u32),
    QuarterRange,
    Year(i32),
    YearRange(i32, i32),
}

// pub struct Term {
//     pub interval: Interval,
//     pub kind: TermType,
// }

// impl FromStr for Term {
//     type Err = ParseError;

//     fn from_str(s: &str) -> Result<Self, Self::Err> {
//         let (year, month) = match parse_month(s) {
//             Ok(TermType::Month((year, month))) => (year, month),
//             Ok(_) => unreachable!(),
//             Err(_) => return Err(ParseError(format!("Failed parsing {} as a month", s))),
//         };
//         if month > 12 {
//             return Err(ParseError(format!("Month of year {} > 12", month)));
//         }
//         Ok(Month::new(year, month, Tz::UTC).unwrap())
//     }
// }

pub enum TokenType {
    MonthOfYear(u32),
    DayOfMonth(u32),
    Year(i32),
    Month((i32, u32)),
}

impl FromStr for MonthTz {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let (year, month) = match parse_month(s) {
            Ok(TermType::Month(year, month)) => (year, month),
            Ok(_) => unreachable!(),
            Err(_) => return Err(ParseError(format!("Failed parsing {} as a month", s))),
        };
        if month > 12 {
            return Err(ParseError(format!("Month of year {} > 12", month)));
        }
        Ok(MonthTz::new(year, month, Tz::UTC).unwrap())
    }
}

impl FromStr for Interval {
    type Err = ParseError;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        if let Ok(term) = parse_term(s) {
            let interval = match term {
                TermType::Day(_, _, _) => todo!(),
                TermType::DayRange => todo!(),
                TermType::Month(year, month) => {
                    let start = Tz::UTC.with_ymd_and_hms(year, month, 1, 0, 0, 0).unwrap();
                    let month = MonthTz::containing(start);
                    Interval {
                        start,
                        end: month.end(),
                    }
                }
                TermType::MonthRange(p1, p2) => {
                    let start = Tz::UTC.with_ymd_and_hms(p1.0, p1.1, 1, 0, 0, 0).unwrap();
                    let dt2 = Tz::UTC.with_ymd_and_hms(p2.0, p2.1, 1, 0, 0, 0).unwrap();
                    let month = MonthTz::containing(dt2);
                    let end = month.end();
                    // if end < start {
                    //     return
                    // }
                    Interval { start, end }
                }
                TermType::Quarter(year, quarter) => {
                    let start = Tz::UTC
                        .with_ymd_and_hms(year, 3 * (quarter - 1) + 1, 1, 0, 0, 0)
                        .unwrap();
                    let end = if quarter < 4 {
                        Tz::UTC
                            .with_ymd_and_hms(year, 3 * quarter + 1, 1, 0, 0, 0)
                            .unwrap()
                    } else {
                        Tz::UTC.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).unwrap()
                    };
                    Interval { start, end }
                }
                TermType::QuarterRange => todo!(),
                TermType::Year(year) => {
                    let start = Tz::UTC.with_ymd_and_hms(year, 1, 1, 0, 0, 0).unwrap();
                    let end = Tz::UTC.with_ymd_and_hms(year + 1, 1, 1, 0, 0, 0).unwrap();
                    Interval { start, end }
                }
                TermType::YearRange(_, _) => todo!(),
            };
            Ok(interval)
        } else {
            Err(ParseError(format!("Failed parsing {} as a term", s)))
        }
    }
}

/// Parse a variety of inputs into corresponding terms.
///  
/// Although the code below seems like it takes care of a large number of problems,
/// it still doesn't prevent all problems.  
///
///
///  
pub fn parse_term(input: &str) -> Result<TermType, Error<Rule>> {
    let token = TermParser::parse(Rule::term, input);
    let term = match token {
        Ok(mut token) => token.next().unwrap(),
        Err(e) => return Err(e),
    };

    fn parse_inner(pair: Pair<Rule>) -> TermType {
        match pair.as_rule() {
            Rule::EOI => todo!(),
            Rule::month | Rule::simple | Rule::range | Rule::range_month | Rule::term => {
                parse_inner(pair.into_inner().next().unwrap())
            }
            Rule::range_cal => todo!(),
            Rule::range_month_abb => process_range_month_abb(pair).unwrap(),
            Rule::range_month_txt => process_range_month_txt(pair).unwrap(),
            Rule::range_month_us => process_range_month_us(pair).unwrap(),
            Rule::cal => process_cal(pair).unwrap(),
            Rule::month_abb => process_month_abb(pair).unwrap(),
            Rule::month_iso => process_month_iso(pair).unwrap(),
            Rule::month_txt => process_month_txt(pair).unwrap(),
            Rule::month_us => process_month_us(pair).unwrap(),
            Rule::quarter => process_quarter(pair).unwrap(),
            _ => unreachable!(),
        }
    }

    Ok(parse_inner(term))
}

fn process_range_month_abb(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
    let mut record = token.into_inner();
    // println!("{:?}", record);
    // println!("{:?}", record.len());
    // println!("{:?}", record.next().unwrap().as_str());

    let (start_year, start_month) = match process_month_abb(record.next().unwrap()) {
        Ok(TermType::Month(y, m)) => (y, m),
        _ => unreachable!(),
    };
    let (end_year, end_month) = match process_month_abb(record.next().unwrap()) {
        Ok(TermType::Month(y, m)) => (y, m),
        _ => unreachable!(),
    };
    Ok(TermType::MonthRange(
        (start_year, start_month),
        (end_year, end_month),
    ))
}

fn process_range_month_txt(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
    let mut record = token.into_inner();
    // println!("{:?}", record);
    let (start_year, start_month) = match process_month_txt(record.next().unwrap()) {
        Ok(TermType::Month(y, m)) => (y, m),
        _ => unreachable!(),
    };
    let (end_year, end_month) = match process_month_txt(record.next().unwrap()) {
        Ok(TermType::Month(y, m)) => (y, m),
        _ => unreachable!(),
    };
    Ok(TermType::MonthRange(
        (start_year, start_month),
        (end_year, end_month),
    ))
}

fn process_range_month_us(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
    let mut record = token.into_inner();
    let (start_year, start_month) = match process_month_us(record.next().unwrap()) {
        Ok(TermType::Month(y, m)) => (y, m),
        _ => unreachable!(),
    };
    let (end_year, end_month) = match process_month_us(record.next().unwrap()) {
        Ok(TermType::Month(y, m)) => (y, m),
        _ => unreachable!(),
    };
    Ok(TermType::MonthRange(
        (start_year, start_month),
        (end_year, end_month),
    ))
}

/// Parse "Apr23", "J23" or "April2023" like strings.
fn parse_month(input: &str) -> Result<TermType, Error<Rule>> {
    let token = TermParser::parse(Rule::month, input)?.next().unwrap();
    let record = token.into_inner().next().unwrap();
    // println!("{:?}", record);
    match record.as_rule() {
        Rule::month_txt => process_month_txt(record),
        Rule::month_abb => process_month_abb(record),
        Rule::month_us => process_month_us(record),
        _ => unreachable!(),
    }
}

/// Parse "2023-03" like strings.  It will parse Ok() even strings
/// that are not real months, e.g. "2023-15".  
fn process_month_iso(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
    let v: Vec<_> = token.as_str().split('-').collect();
    // println!("v={:?}", v);
    let year = v[0].parse::<i32>().unwrap();
    let m = v[1].parse::<u32>().unwrap();
    Ok(TermType::Month(year, m))
}

/// Parse "Apr23" or "August2024" like strings.
/// - month > month_txt
///   - mon > feb: "Feb"
///   - yy: "23"
fn process_month_txt(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
    // fn process_month_txt(input: Pairs<'_, Rule>) -> Result<TermType, Error<Rule>> {
    //     if token.as_rule() != Rule::month_txt {
    //     panic!("Expecting Rule::quarter, got {:?}", token.as_rule());
    // }

    let mut record = token.into_inner().into_iter();
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
            Rule::year => y.as_str().parse::<i32>().unwrap(),
            Rule::yy => y.as_str().parse::<i32>().unwrap() + 2000, // no more 1900!
            _ => unreachable!(),
        },
        None => unreachable!(),
    };
    Ok(TermType::Month(year, m))
}

/// Parse "Z23" like strings.
/// - month > month_abb
///   - abb: "Z"
///   - yy: "23"
fn process_month_abb(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
    if token.as_rule() != Rule::month_abb {
        panic!("Expecting Rule::abb, got {:?}", token.as_rule());
    }
    let m = match token.as_str().chars().next().unwrap() {
        'F' => 1,
        'G' => 2,
        'H' => 3,
        'J' => 4,
        'K' => 5,
        'M' => 6,
        'N' => 7,
        'Q' => 8,
        'U' => 9,
        'V' => 10,
        'X' => 11,
        'Z' => 12,
        _ => unreachable!(),
    };
    let mut record = token.into_inner();

    // println!("{:?}", record.next().as_str());
    // println!("m={:?}", m);
    // println!("{:?}", record.next());

    let year = match record.next() {
        Some(y) => match y.as_rule() {
            Rule::year => y.as_str().parse::<i32>().unwrap(),
            Rule::yy => y.as_str().parse::<i32>().unwrap() + 2000, // no more 1900!
            _ => unreachable!(),
        },
        None => unreachable!(),
    };
    Ok(TermType::Month(year, m))
}

/// Parse "4/28", "04/2028", etc.  This parser will fail on incorrect months, e.g. "15/2028".
fn process_month_us(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
    if token.as_rule() != Rule::month_us {
        panic!("Expecting Rule::month_us, got {:?}", token.as_rule());
    }
    let v: Vec<_> = token.as_str().split('/').collect();
    // println!("v={:?}", v);
    let m = v[0].parse::<u32>().unwrap();
    let mut record = token.into_inner();

    let year = match record.next() {
        Some(y) => match y.as_rule() {
            Rule::year => y.as_str().parse::<i32>().unwrap(),
            Rule::yy => y.as_str().parse::<i32>().unwrap() + 2000, // no more 1900!
            _ => unreachable!(),
        },
        None => unreachable!(),
    };
    Ok(TermType::Month(year, m))
}

fn process_cal(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
    if token.as_rule() != Rule::cal {
        panic!("Expecting Rule::cal, got {:?}", token.as_rule());
    }
    let next = token.into_inner().next().unwrap();
    let year = match next.as_rule() {
        Rule::year => next.as_str().parse::<i32>().unwrap(),
        Rule::yy => next.as_str().parse::<i32>().unwrap() + 2000,
        _ => unreachable!(),
    };
    Ok(TermType::Year(year))
}

/// Parse "Q2, 24", "Q3 24", "Q2, 2024", "Q3 2024" strings
fn process_quarter(token: Pair<'_, Rule>) -> Result<TermType, Error<Rule>> {
    if token.as_rule() != Rule::quarter {
        panic!("Expecting Rule::quarter, got {:?}", token.as_rule());
    }
    let q = token
        .as_str()
        .chars()
        .nth(1)
        .unwrap()
        .to_string()
        .parse::<u32>()
        .unwrap();
    let next = token.into_inner().next().unwrap();
    let year = match next.as_rule() {
        Rule::year => next.as_str().parse::<i32>().unwrap(),
        Rule::yy => next.as_str().parse::<i32>().unwrap() + 2000,
        _ => unreachable!(),
    };
    Ok(TermType::Quarter(year, q))
}

#[cfg(test)]
mod tests {

    use chrono_tz::Tz;
    use pest::Parser;

    use crate::interval::{month_tz::MonthTz, term::*};

    #[test]
    fn test_interval_from_str() {
        // a month
        let left = "May25".parse::<Interval>().unwrap();
        let right = Interval {
            start: Tz::UTC.with_ymd_and_hms(2025, 5, 1, 0, 0, 0).unwrap(),
            end: Tz::UTC.with_ymd_and_hms(2025, 6, 1, 0, 0, 0).unwrap(),
        };
        assert_eq!(left, right);
        // a month range
        let left = "May25-Sep25".parse::<Interval>().unwrap();
        let right = Interval {
            start: Tz::UTC.with_ymd_and_hms(2025, 5, 1, 0, 0, 0).unwrap(),
            end: Tz::UTC.with_ymd_and_hms(2025, 10, 1, 0, 0, 0).unwrap(),
        };
        assert_eq!(left, right);
        // a quarter
        let left = "Q4,25".parse::<Interval>().unwrap();
        let right = Interval {
            start: Tz::UTC.with_ymd_and_hms(2025, 10, 1, 0, 0, 0).unwrap(),
            end: Tz::UTC.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        };
        assert_eq!(left, right);
        // a year
        let left = "2025".parse::<Interval>().unwrap();
        let right = Interval {
            start: Tz::UTC.with_ymd_and_hms(2025, 1, 1, 0, 0, 0).unwrap(),
            end: Tz::UTC.with_ymd_and_hms(2026, 1, 1, 0, 0, 0).unwrap(),
        };
        assert_eq!(left, right);
    }

    #[test]
    fn test_parse_term() {
        let vs = [
            ("Apr24", TermType::Month(2024, 4)),
            ("Apr 24", TermType::Month(2024, 4)),
            ("Apr 2024", TermType::Month(2024, 4)),
            ("April2024", TermType::Month(2024, 4)),
            ("April 2024", TermType::Month(2024, 4)),
            ("J24", TermType::Month(2024, 4)),
            ("4/24", TermType::Month(2024, 4)),
            ("4/2024", TermType::Month(2024, 4)),
            ("2024-04", TermType::Month(2024, 4)),
            ("2024-18", TermType::Month(2024, 18)), // !
            //
            ("Q3, 2024", TermType::Quarter(2024, 3)),
            ("Q3, 24", TermType::Quarter(2024, 3)),
            ("Q3 24", TermType::Quarter(2024, 3)),
            //
            ("Cal 24", TermType::Year(2024)),
            ("Cal 2024", TermType::Year(2024)),
            ("2024", TermType::Year(2024)),
            //
            ("Apr24-Aug28", TermType::MonthRange((2024, 4), (2028, 8))),
            ("Apr24-Aug21", TermType::MonthRange((2024, 4), (2021, 8))), // !
            ("Apr24 - May 25", TermType::MonthRange((2024, 4), (2025, 5))),
            ("J24-Q28", TermType::MonthRange((2024, 4), (2028, 8))),
            ("J24 - Q28", TermType::MonthRange((2024, 4), (2028, 8))),
            ("4/24 - 08/2028", TermType::MonthRange((2024, 4), (2028, 8))),
            //
        ];
        for e in vs {
            println!("{:?}", e);
            assert_eq!(parse_term(e.0).ok().unwrap(), e.1);
        }
    }

    // #[test]
    // fn test_parse_range_month() {
    //     // assert_eq!(
    //     //     parse_range_month_txt("Apr24-Aug26").unwrap(),
    //     //     TermType::MonthRange((2024, 4), (2026, 8))
    //     // );
    //     // assert_eq!(
    //     //     parse_range_month_abb("J24-Q26").unwrap(),
    //     //     TermType::MonthRange((2024, 4), (2026, 8))
    //     // );
    //     // assert_eq!(
    //     //     parse_range_month_us("4/24-8/26").unwrap(),
    //     //     TermType::MonthRange((2024, 4), (2026, 8))
    //     // );
    // }

    #[test]
    fn test_parse_month() {
        // assert_eq!(
        //     parse_month("Apr24").ok().unwrap(), (2024, 4)
        // );
        assert_eq!(
            "Apr24".parse::<MonthTz>().unwrap(),
            MonthTz::new(2024, 4, Tz::UTC).unwrap()
        );
        // assert_eq!(
        //     parse_month("J24", Tz::UTC).unwrap(),
        //     Month::new(2024, 4, Tz::UTC).unwrap()
        // );
        // assert!(parse_month("Apx24", Tz::UTC).is_err());
    }

    // #[test]
    // fn test_parse_month_iso() {
    //     assert_eq!(
    //         parse_month_iso("2024-04").unwrap(),
    //         TermType::Month(2024, 4)
    //     );
    //     assert_eq!(
    //         parse_month_iso("2028-15").unwrap(),
    //         TermType::Month(2028, 15)
    //     );
    // }
    // #[test]
    // fn test_parse_month_txt() {
    //     assert_eq!(
    //         parse_month_txt("Apr24").unwrap(),
    //         TermType::Month(2024, 4)
    //     );
    //     assert_eq!(
    //         parse_month_txt("April24").unwrap(),
    //         TermType::Month(2024, 4)
    //     );
    //     assert_eq!(
    //         parse_month_txt("April2024").unwrap(),
    //         TermType::Month(2024, 4)
    //     );
    //     assert!(parse_month_txt("Apx24").is_err());
    // }
    // #[test]
    // fn test_parse_month_abb() {
    //     assert_eq!(parse_month_abb("J24").unwrap(), TermType::Month(2024, 4));
    //     assert_eq!(parse_month_abb("N24").unwrap(), TermType::Month(2024, 7));
    //     assert!(parse_month_abb("R24").is_err());
    // }
    // #[test]
    // fn test_parse_month_us() {
    //     assert_eq!(parse_month_us("4/24").unwrap(), TermType::Month(2024, 4));
    //     assert_eq!(parse_month_us("4/2024").unwrap(), TermType::Month(2024, 4));
    //     assert_eq!(parse_month_us("04/24").unwrap(), TermType::Month(2024, 4));
    //     assert_eq!(parse_month_us("04/2024").unwrap(), TermType::Month(2024, 4));
    // }

    // #[test]
    // fn test_parse_quarter() {
    //     assert_eq!(parse_quarter("Q3,24").unwrap(), TermType::Quarter(2024, 3));
    //     assert_eq!(
    //         parse_quarter("Q4, 2024").unwrap(),
    //         TermType::Quarter(2024, 4)
    //     );
    //     assert_eq!(parse_quarter("Q4 24").unwrap(), TermType::Quarter(2024, 4));
    //     assert_eq!(
    //         parse_quarter("Q4 2024").unwrap(),
    //         TermType::Quarter(2024, 4)
    //     );
    //     assert!(parse_quarter("Q5, 2024").is_err());
    // }

    // #[test]
    // fn test_parse_cal() {
    //     assert_eq!(parse_cal("Cal2024").unwrap(), TermType::Year(2024));
    //     assert_eq!(parse_cal("Cal24").unwrap(), TermType::Year(2024));
    //     assert_eq!(parse_cal("Cal 24").unwrap(), TermType::Year(2024));
    //     assert_eq!(parse_cal("2024").unwrap(), TermType::Year(2024));
    // }

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
}
