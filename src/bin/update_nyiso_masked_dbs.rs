use std::{error::Error, path::Path};

use bust::{db::prod_db::ProdDb, interval::month::month};
use clap::Parser;
use jiff::Zoned;
use log::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about = "Download NYISO masked bid/offer data.  See https://mis.nyiso.com/public/P-27list.htm")]
struct Args {}

fn main() -> Result<(), Box<dyn Error>> {
    let _ = Args::parse();
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(".env/test.env")).unwrap();

    let today = Zoned::now().date();
    // they don't publish the data on Sat/Sun or (likely) holidays.
    if today.weekday().to_monday_one_offset() > 5 {
        info!("Exiting program because today is a weekend.");
        return Ok(());
    }

    let month = month(today.year(), today.month()).add(-4)?;
    info!("Processing month {}", month);

    let archive = ProdDb::nyiso_capacity_offers();
    archive.download_file(&month)?;
    archive.update_duckdb(&month)?;

    Ok(())
}
