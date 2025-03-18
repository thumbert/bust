use std::{error::Error, path::Path};

use bust::{db::prod_db::ProdDb, interval::month::month};
use clap::Parser;
use jiff::Zoned;
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
    let archive = ProdDb::isone_daas_reserve_data();

    let today = Zoned::now().date();
    if today.day() < 5 {
        let focus = month(today.year(), today.month()).previous();
        if focus >= month(2025, 3) {
            archive.download_missing_days(focus)?;
            match archive.update_duckdb(focus) {
                Ok(_) => info!("Updated month {} successfully", focus),
                Err(e) => error!("{:?}", e),
            }
        }
    }
    let month = month(today.year(), today.month());
    archive.download_missing_days(month)?;
    match archive.update_duckdb(month) {
        Ok(_) => info!("Updated month {} successfully", month),
        Err(e) => error!("{:?}", e),
    }

    Ok(())
}
