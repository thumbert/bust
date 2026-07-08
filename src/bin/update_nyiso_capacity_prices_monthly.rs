use std::{error::Error, path::Path};

use bust::{db::prod_db::ProdDb, interval::month::month};
use clap::Parser;
use jiff::Zoned;

#[derive(Parser, Debug)]
#[command(version, about, long_about = "Update NYISO capacity prices monthly data (Rust).")]
struct Args {}

/// Run every month on the 15th of the month
fn main() -> Result<(), Box<dyn Error>> {
    let _ = Args::parse();
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(".env/test.env")).unwrap();

    let today = Zoned::now().date();
    let month = month(today.year(), today.month()).next();

    let archive = ProdDb::nyiso_capacity_prices_monthly();
    archive.update_duckdb(&month)?;

    Ok(())
}
