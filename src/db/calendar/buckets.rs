use std::{path::Path, process::Command};

use jiff::Zoned;

use crate::{
    bucket::{Bucket, BucketLike}, db::prod_db::ProdDb, elec::iso::ISONE, interval::{interval_base::IntervalTzLike, term::Term}
};

#[derive(Clone)]
pub struct BucketsArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[allow(dead_code)]
struct Row {
    hour_beginning: Zoned,
    b5x16: bool,
    b2x16h: bool,
    b7x8: bool,
    offpeak: bool,
    atc: bool,
}

/// Generate a CSV file with the bucket mask for each hour of the given term.
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
            atc: true,
        }
    });

    let file = std::fs::File::create(file_path).expect("Unable to create file");
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(["hour_beginning", "5x16", "2x16H", "7x8", "offpeak", "atc"])
        .unwrap();
    for row in rows {
        wtr.write_record(&[
            row.hour_beginning
                .strftime("%Y-%m-%d %H:00:00.000%:z")
                .to_string(),
            row.b5x16.to_string().to_uppercase(),
            row.b2x16h.to_string().to_uppercase(),
            row.b7x8.to_string().to_uppercase(),
            row.offpeak.to_string().to_uppercase(),
            row.atc.to_string().to_uppercase(),
        ])
        .unwrap();
    }
    wtr.flush().unwrap();

    // gzip the file
    let dir = Path::new(file_path).parent().unwrap();
    Command::new("gzip")
        .args(["-f", file_path])
        .current_dir(dir)
        .spawn()
        .unwrap()
        .wait()
        .expect("gzip failed");

    Ok(())
}

/// Make a file with the count of hours by month
pub fn count_hour_by_month(term: Term, file_path: &str) -> Result<(), Box<dyn std::error::Error>> {
    let term_tz = term.with_tz(&ISONE.tz);
    let mut counts: Vec<(String, Bucket, i32)> = Vec::new();
    for bucket in [
        Bucket::Atc,
        Bucket::B5x16,
        Bucket::B2x16H,
        Bucket::B7x8,
        Bucket::Offpeak,
    ] {
        for month in term_tz.months() {
            let hours = bucket.count_hours(&month);
            counts.push((month.start().strftime("%Y-%m").to_string(), bucket, hours));
        }
    }

    // Write to CSV
    let file = std::fs::File::create(file_path).expect("Unable to create file");
    let mut wtr = csv::Writer::from_writer(file);
    wtr.write_record(["month", "bucket", "hour_count"]).unwrap();
    for e in &counts {
        wtr.write_record(&[e.0.clone(), e.1.name(), e.2.to_string()])
            .unwrap();
    }
    wtr.flush().unwrap();

    Command::new("gzip")
        .args(["-f", file_path])
        .current_dir(ProdDb::buckets().base_dir.as_str())
        .spawn()
        .unwrap()
        .wait()
        .expect("gzip failed");

    Ok(())
}

#[cfg(test)]
mod tests {
    use crate::{
        db::{calendar::buckets::*, prod_db::ProdDb},
        interval::term::Term,
    };
    use std::error::Error;

    #[ignore]
    #[test]
    fn make_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::buckets();
        let term = "Cal10-Cal34".parse::<Term>().unwrap();
        generate_csv(term, format!("{}/buckets.csv", archive.base_dir).as_str())?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn make_hour_count_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::buckets();
        let term = "Cal10-Cal34".parse::<Term>().unwrap();
        count_hour_by_month(
            term,
            format!("{}/hour_count.csv", archive.base_dir).as_str(),
        )?;
        Ok(())
    }
}
