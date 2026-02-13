use std::{error::Error, path::Path};

use bust::{db::prod_db::ProdDb, interval::month::Month};
use jiff::{ToSpan, Zoned};

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(".env/test.env")).unwrap();

    let archive = ProdDb::hq_fuel_mix();
    let start = Zoned::now().date().saturating_sub(5.days());
    let days = start.series(1.day()).take(5).collect::<Vec<_>>();
    for day in days {
        log::info!("Downloading HQ fuel mix for {}", day);
        archive.download_file(day)?;
    }

    if Zoned::now().date().day() < 4 {
        log::info!("Updating previous month in DuckDB");
        let prev_month = Month::containing(Zoned::now().datetime()).previous();
        archive.update_duckdb(prev_month)?;
    }
    let month = Month::containing(Zoned::now().datetime());
    archive.update_duckdb(month)?;

    Ok(())
}
