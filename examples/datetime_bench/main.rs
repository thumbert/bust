extern crate chrono;
extern crate chrono_tz;

use std::time::{Instant};

use chrono::prelude::*;
use chrono::{Datelike, Duration, NaiveDate, NaiveDateTime, NaiveTime};
use chrono_tz::{Tz, America::New_York};


fn main() {
    let mut dt = New_York.ymd(2000, 1, 1).and_hms(0,0,0);
    let mut count = 0;

    let start = Instant::now();
    for _i in 0..201624 {
        dt = dt + Duration::hours(1);
        count += 1;
    }
    let duration = start.elapsed();

    println!("Time elapsed: {:?}", duration);
    println!("{}", dt);
    assert_eq!(count, 201624);
}