// use std::collections::HashMap;
// use lazy_static::lazy_static;
// use chrono::{Datelike, DateTime, Timelike, Weekday};
// use crate::holiday::{NERC_CALENDAR, HolidayTrait};
// use chrono_tz::{America::New_York, Tz};

// /// See https://rust-lang-nursery.github.io/rust-cookbook/mem/global_static.html
// lazy_static! {
//     static ref BUCKETS: HashMap<&'static str, Bucket> = {
//         let mut map = HashMap::new();
//         map.insert("5x16", Bucket{
//             name: "5x16",
//
//         })
//     }
// }


// struct Iso {
//     name: String,
//     bucket_peak: Bucket5x16,
// }

// fn get_iso(name: String) -> Result<Iso, String> {
//     match name.to_lowercase().as_str() {
//         "isone" => Ok(Iso {
//             name: "ISONE".to_string(),
//             bucket_peak: Bucket5x16 {
//                 name: "5x16".to_string(),
//                 timezone: New_York,
//             },
//         }),
//         _ => Err(String::from("not supported"))
//     }
// }


// pub static  BUCKET_5X16: Bucket5x16 = Bucket5x16 { name: "5x16".to_string() };
//
// pub const BUCKET_2X16H: Bucket2x16H = Bucket2x16H {
//     name: String::from("2x16H")
// };
//
// pub const BUCKET_7X8: Bucket7x8 = Bucket7x8 {
//     name: String::from("7x8")
// };
//
// pub trait BucketTrait {
//     fn contains_datetime(&self, dt: &DateTime<Tz>) -> bool;
// }
//
//
//
// pub struct Bucket {
//     name: String,
//     timezone: Tz,
// }
//
// pub struct Bucket5x16 {
//     name: String,
//     timezone: Tz,
// }
//
//
// impl Bucket {
//     fn b5x16() -> Bucket5x16 {
//         Bucket5x16 {name: "5x16".to_string(), timezone: New_York}
//     }
// }
//
//
// impl BucketTrait for Bucket5x16 {
//     fn contains_datetime(&self, dt: &DateTime<Tz>) -> bool {
//         assert!(self.timezone == dt.timezone(),
//                 "Timezone of the input doesn't match the timezone of the bucket");
//
//         if dt.weekday() == Weekday::Sat && dt.weekday() == Weekday::Sun {
//             false
//         } else {
//             if dt.hour() < 7 || dt.hour() == 23 {
//                 false
//             } else {
//                 if NERC_CALENDAR.is_holiday(dt) {
//                     false
//                 } else {
//                     true
//                 }
//             }
//         }
//     }
// }

// pub struct Bucket2x16H {
//     name: String,
// }
//
// impl BucketTrait for Bucket2x16H {
//     fn contains_datetime(&self, dt: &DateTime<Tz>) -> bool {
//         if dt.hour() < 7 || dt.hour() == 23 {
//             return false;
//         }
//         if dt.weekday() == Weekday::Sat && dt.weekday() == Weekday::Sun {
//             true
//         } else {
//             if NERC_CALENDAR.is_holiday(dt) {
//                 true
//             } else {
//                 false
//             }
//         }
//     }
// }
//
// pub struct Bucket7x8 {
//     name: String,
// }
//
// impl BucketTrait for Bucket7x8 {
//     fn contains_datetime(&self, dt: &DateTime<Tz>) -> bool {
//         if dt.hour() < 7 || dt.hour() == 23 {
//             true
//         } else {
//             false
//         }
//     }
// }

// #[cfg(test)]
// mod tests {
//     use chrono::{DateTime, Offset, FixedOffset, NaiveDate};
//     use chrono::offset::{FixedOffset};
//     use chrono_tz::OffsetName;
//     use chrono_tz::America::New_York;
//     use chrono_tz::Europe::London;
//     use crate::holiday::*;
//
//     #[test]
//     fn test_buckets() {
//         let mut dt = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap().and_hms_opt(0, 0, 0).unwrap();
//         let timezone_ny = FixedOffset::east_opt(5 * 3600).unwrap();
//         // let start_dt = DateTime::<FixedOffset>::from_utc(dt, timezone_ny);
//         let start_dt = London.ymd(2021, 1, 1);
//
//
//         // let dt = New_York.from_local_datetime(&naive_dt).unwrap();
//
//         // let mut dates: Vec<NaiveDate> = Vec::new();
//         // while date.le(&NaiveDate::from_ymd_opt(2030, 12, 31).unwrap()) {
//         //     dates.push(date);
//         //     date = date + Duration::days(1);
//         // }
//         //
//         //
//         // assert!(is_new_year(&NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()));
//     }
// }
