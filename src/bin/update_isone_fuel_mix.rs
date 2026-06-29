use std::{error::Error, path::Path};

use bust::db::prod_db::ProdDb;
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

/// Run this job every day at 7:00AM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let today = Zoned::now().date();
    let archive = ProdDb::isone_fuel_mix();
    for i in 1..7 {
        let date = today - i.days();
        let file = archive.filename(&date) + ".gz";
        if !Path::new(&file).exists() {
            match archive.download_file(date) {
                Ok(_) => info!(
                    "Downloaded ISONE fuel mix file for {} successfully",
                    date
                ),
                Err(e) => error!("{:?}", e),
            }
        }
        let _ = archive.update_duckdb(&date);
    }

    Ok(())
}
