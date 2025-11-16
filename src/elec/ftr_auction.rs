// use jiff::{civil::Date, Span};

// struct FtrAuction {
//     start_date: Date,
//     month_count: u8,
//     round: Option<u8>,
// }

// impl FtrAuction {
//     pub fn new(start_date: Date, month_count: u8, round: Option<u8>) -> FtrAuction {
//         FtrAuction {
//             start_date,
//             month_count,
//             round,
//         }
//     }

//     pub fn end_date(self) -> Date {
//         self.start_date
//             .saturating_add(Span::new().months(self.month_count))
//             .last_of_month()
//     }
// }

// #[cfg(test)]
// mod tests {
//     use std::error::Error;

//     use crate::elec::ftr_auction::*;
//     use jiff::{civil::Date, Span};

//     #[test]
//     fn one_month() -> Result<(), Box<dyn Error>> {
//         let auction = FtrAuction::new("2024-01-01".parse::<Date>()?, 1, None);
//         assert_eq!(auction.month_count, 1);
//         Ok(())
//     }
// }
