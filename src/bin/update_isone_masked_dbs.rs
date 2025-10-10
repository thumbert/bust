use std::{error::Error, path::Path, vec};

use bust::{db::prod_db::ProdDb, interval::month::month};
use clap::Parser;
use jiff::{civil::date, Zoned};
use log::{error, info};

#[derive(Parser, Debug)]
#[command(
    version,
    about,
    long_about = "Utility to download DASI reserve data and upload it to Shooju."
)]
struct Args {}

/// Run this job every day after the DAM is published
fn main() -> Result<(), Box<dyn Error>> {
    let _ = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(".env/test.env")).unwrap();

    let today = Zoned::now().date();
    let month = month(today.year(), today.month()).add(-4)?;
    info!("Processing month {}", month);

    let days = month.days();
    // let days = vec![date(2025, 6, 29), date(2025, 6, 30)];
    let archive = ProdDb::isone_masked_da_energy_offers();
    let archive = ProdDb::isone_masked_daas_offers();
    // for day in &days {
    //     println!("Processing {}", day);
    //     archive.download_file(day)?;
    // }
    archive.update_duckdb(&month)?;


    // archive.download_missing_days(month)?;
    // match archive.update_duckdb(month) {
    //     Ok(_) => info!("Updated month {} successfully", month),
    //     Err(e) => error!("{:?}", e),
    // }

    Ok(())
}
