use std::time::Instant;
use chrono::prelude::*;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use chrono_tz::Tz;
use chrono_tz::America::New_York;

use plotly::{Plot, Scatter};


mod holiday;

use crate::holiday::{HolidayTrait, NercCalendar};


/// See https://github.com/felipenoris/bdays/blob/master/src/tests.rs

// fn utils() {
//     let dt = NaiveDate::from_ymd_opt(2022, 12, 28).unwrap();
//     assert_eq!(is_weekend(dt), false);
//
//     //NERC_CALENDAR.is_holiday(&dt);
// }

fn examples_datetimes() {
    let date = NaiveDate::from_ymd_opt(2023, 3, 12).unwrap();
    let naive_dt = NaiveDateTime::new(date, NaiveTime::from_num_seconds_from_midnight_opt(0,0).unwrap());
    let mut dt = New_York.from_local_datetime(&naive_dt).unwrap();
    assert_eq!(dt.to_rfc3339(), "2023-03-12T00:00:00-05:00");

    println!("Show the DST Spring forward transition from -05:00 offset to -04:00 offset");
    for _i in 0..5 {
        println!("{}", dt.to_rfc3339());
        dt += Duration::hours(1);
    }

    // Use of a timezone string to get a timezone
    let tz = "America/New_York".parse::<Tz>().unwrap();
    let mut fall = tz.with_ymd_and_hms(2023, 11, 5, 0, 0, 0).unwrap();
    assert_eq!(fall.to_rfc3339(), "2023-11-05T00:00:00-04:00");
    println!("\nShow the DST Fall back transition from -04:00 offset to -05:00 offset");
    for _i in 0..5 {
        println!("{}", fall.to_rfc3339());
        fall += Duration::hours(1);
    }

    println!("{:?}", New_York);
}

// fn speed_test_datetime() {
//     let mut dt = New_York.
// }
//



fn main() {
    examples_datetimes();
}
