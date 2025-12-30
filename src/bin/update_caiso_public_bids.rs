use std::{error::Error, path::Path};

use bust::{
    db::prod_db::ProdDb,
    interval::month::month,
};
use clap::Parser;
use jiff::Zoned;
use log::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

/// Run this job every day at 18:15[America/New_York]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let archive = ProdDb::caiso_public_bids();

    let today = Zoned::now().date();
    let current_month = month(today.year(), today.month());
    let month = current_month.add(-3).unwrap();
    info!("Working on month {}", month);
    archive.download_missing_days(month).await?;
    archive.update_duckdb(&month)?;

    Ok(())
}
