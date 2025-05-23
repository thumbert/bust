extern crate chrono;
extern crate chrono_tz;

use std::time::Instant;

use bust::holiday::{HolidayTrait, NercCalendar};
use chrono::{Duration, NaiveDate};

fn main() {
    let calendar = NercCalendar {};
    let mut date = NaiveDate::from_ymd_opt(2021, 1, 1).unwrap();
    // let naive_dt = NaiveDateTime::new(date, NaiveTime::from_num_seconds_from_midnight_opt(0,0).unwrap());
    // let dt = New_York.from_local_datetime(&naive_dt).unwrap();

    let mut dates: Vec<NaiveDate> = Vec::new();
    while date.le(&NaiveDate::from_ymd_opt(2030, 12, 31).unwrap()) {
        dates.push(date);
        date += Duration::days(1);
    }
    // println!("{}", dates.len());

    let mut count = 0;
    let start = Instant::now();
    for _i in 0..25 {
        for date in dates.iter() {
            // println!("{}", date);
            if calendar.is_holiday(date) {
                // println!("{date}");
                count += 1;
            }
        }
    }
    let duration = start.elapsed();
    println!("Count of holidays: {}", count);
    assert_eq!(count, 1500);
    println!("Time elapsed: {:?}", duration);
}
