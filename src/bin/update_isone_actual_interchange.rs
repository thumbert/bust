use std::{error::Error, path::Path};

use bust::{
    db::prod_db::ProdDb, interval::month::month
};
use clap::Parser;
use jiff::{ToSpan, Zoned};
use log::{error, info};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

/// Run this job every day at 6:00AM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let today = Zoned::now().date();
    let archive = ProdDb::isone_actual_interchange();
    for i in 1..5 {
        let date = today - i.days();
        let file = archive.filename(&date) + ".gz";
        if !Path::new(&file).exists() {
            match archive.download_file(date) {
                Ok(_) => info!(
                    "Downloaded ISONE actual interchange file for {} successfully",
                    date
                ),
                Err(e) => error!("{:?}", e),
            }
        }
    }
    let current_month = month(today.year(), today.month());
    if today.day() < 5 {
        let prev_month = current_month.previous();
        archive.download_missing_days(prev_month)?;
        archive.update_duckdb(&prev_month)?;
    }
    archive.update_duckdb(&current_month)?;

    Ok(())
}
