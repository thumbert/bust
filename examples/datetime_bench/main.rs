extern crate chrono;
extern crate chrono_tz;

use std::time::Instant;
use chrono::prelude::*;
use chrono::Duration;
use chrono_tz::America::New_York;


fn main() {
    // let mut dt = New_York.ymd_opt(2000, 1, 1).and_hms_opt(0,0,0).unwrap();
    let mut dt = New_York.with_ymd_and_hms(2000, 1, 1, 0, 0, 0).unwrap();
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