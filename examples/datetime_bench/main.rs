use std::time::Instant;

use jiff::{ToSpan, Zoned};

fn main() {
    let mut dt = "2000-01-01T00:00:00-05:00[America/New_York]".parse::<Zoned>().unwrap();
    let mut count = 0;

    let start = Instant::now();
    for _i in 0..201624 {
        dt += 1.hour();
        count += 1;
    }
    let duration = start.elapsed();

    println!("Time elapsed: {:?}", duration);
    println!("{}", dt);
    assert_eq!(count, 201624);
}
