use std::time::Instant;

use bust::{holiday::{HolidayTrait, NercCalendar}, interval::interval::DateExt};
use jiff::civil::date;

fn main() {
    let calendar = NercCalendar {};
    let start = date(2021, 1, 1);
    let dates = start.up_to(date(2030, 12, 31));
    // println!("{}", dates.len());

    let mut count = 0;
    let start = Instant::now();
    for _i in 0..25 {
        for date in dates.iter() {
            if calendar.is_holiday(date) {
                count += 1;
            }
        }
    }
    let duration = start.elapsed();
    println!("Count of holidays: {}", count);
    assert_eq!(count, 1500);
    println!("Time elapsed: {:?}", duration);
}
