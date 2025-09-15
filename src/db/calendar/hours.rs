use jiff::Zoned;

use crate::{
    bucket::{Bucket, BucketLike},
    elec::iso::ISONE,
    interval::{interval::IntervalTzLike, term::Term},
};

struct Row {
    hour_beginning: Zoned,
    b5x16: bool,
    b2x16h: bool,
    b7x8: bool,
    offpeak: bool,
}

pub fn generate_csv(term: Term, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let term_tz = term.with_tz(&ISONE.tz);

    let rows = term_tz.hours().into_iter().map(|h| {
        let hour_beginning = h.start();
        Row {
            hour_beginning: hour_beginning.clone(),
            b5x16: Bucket::B5x16.contains(&hour_beginning),
            b2x16h: Bucket::B2x16H.contains(&hour_beginning),
            b7x8: Bucket::B7x8.contains(&hour_beginning),
            offpeak: Bucket::Offpeak.contains(&hour_beginning),
        }
    });

    let file = std::fs::File::create(file_path).expect("Unable to create file");
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(["hour_beginning", "b5x16", "b2x16h", "b7x8", "offpeak"])
        .unwrap();
    for row in rows {
        wtr.write_record(&[
            row.hour_beginning.strftime("%Y-%m-%d %H:00:00.000%:z").to_string(),
            row.b5x16.to_string().to_uppercase(),
            row.b2x16h.to_string().to_uppercase(),
            row.b7x8.to_string().to_uppercase(),
            row.offpeak.to_string().to_uppercase(),
        ])
        .unwrap();
    }
    wtr.flush().unwrap();
    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{db::calendar::hours::generate_csv, interval::term::Term};
    use std::error::Error;

    #[ignore]
    #[test]
    fn make_file() -> Result<(), Box<dyn Error>> {
        let term = "Cal25-Cal26".parse::<Term>().unwrap();
        generate_csv(term, "/home/adrian/Downloads/Archive/Calendars/buckets.csv")?;
        Ok(())
    }
}
