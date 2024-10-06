// use std::str::FromStr;

// use chrono_tz::Tz;
// use winnow::ascii::{digit0, digit1};
// use winnow::error::{ContextError, ErrMode, InputError, ErrorKind};
// use winnow::token::one_of;
// use winnow::{prelude::*, Parser};
// use winnow::combinator::{repeat, separated_pair};

// use super::month::Month;

// /// Parse "2021" -> 2021
// fn parse_year<'s>(input: &mut &'s str) -> PResult<u32> {
//     let y0 = one_of('1'..'9').parse_next(input)?;
//     let y = digit1.parse_next(input)?;
//     let v = format!("{}{}", y0, y).parse::<u32>();
//     if v.is_err() {
//         return Err(ErrMode::Backtrack(ContextError::new()));
//     }
//     Ok(v.unwrap())
// }

// /// Parse a month value, e.g. "06" -> 6
// // fn parse_mm<'s>(input: &mut &'s str) -> PResult<u32,  InputError<&'s str>> {
// //     let mm =  digit1.try_map(FromStr::from_str).parse_next(input);

// //     // let mm = repeat(0..2, digit0).parse_next(input)?;
// //     // let v = format!("{}", mm).parse::<u32>();
// //     // if v.is_err() || v.unwrap() > 12 {
// //     //     return Err(ErrMode::Backtrack(ContextError::new()));
// //     // }
// //     // Ok(v.unwrap())
// //     Ok(6)
// // }

// // fn parse_mm<'s>(input: &mut &'s str) -> PResult<u32, InputError<&'s str>> {
// //     let mm = digit1.try_map(str::parse).parse_next(input)?;
// //     if (mm > 12) {
// //         return Err(InputError::new(input.into(), ErrorKind::Fail));
// //     }
// //     Ok(mm)
// //   }
  

// /// 2022-06
// fn parse_month_iso<'s>(input: &mut &'s str) -> PResult<Month> {
//     // let year = parse_year(input);
    

//     // let val = separated_pair(parse_year, "-", repeat(1..3, digit0)).parse_next(input)?;

//     // let year =  one_of('1'..'9').parse_next(input);
//     // digit0.parse_next(input)?.parse::<i32>().unwrap();
//     Ok(Month::new(2024, 3, Tz::UTC).unwrap())
// }

// #[cfg(test)]
// mod tests {
//     use chrono_tz::Tz;

//     use crate::interval::{month::Month};

//     use super::parse_year;

//     #[test]
//     fn test_parse_year() {
//         assert_eq!(parse_year(&mut "2024").unwrap(), 2024);
//     }

//     // #[test]
//     // fn test_parse_mm() {
//     //     assert_eq!(parse_mm(&mut "11").unwrap(), 11);
//     // }


//     // Month::new(2024, 3, Tz::UTC).unwrap()
// }
