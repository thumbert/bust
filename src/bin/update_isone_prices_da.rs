use std::{error::Error, path::Path};

use bust::{
    db::{isone::lib_dam::is_dalmp_published, prod_db::ProdDb},
    interval::month::month,
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

/// Run this job every day at 12:30PM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let tomorrow = Zoned::now().date().tomorrow().unwrap();
    let archive = ProdDb::isone_dalmp();
    for i in 0..5 {
        if i == 0 && !is_dalmp_published(tomorrow).unwrap() {
            continue;
        }
        let date = tomorrow - i.days();
        let file = archive.filename(&date) + ".gz";
        if !Path::new(&file).exists() {
            match archive.download_file(date) {
                Ok(_) => info!(
                    "Downloaded ISONE DA LMP hourly prices file for {} successfully",
                    date
                ),
                Err(e) => error!("{:?}", e),
            }
        }
    }
    let current_month = month(tomorrow.year(), tomorrow.month());
    if tomorrow.day() < 5 {
        let prev_month = current_month.previous();
        archive.download_missing_days(prev_month)?;
        archive.update_duckdb(&prev_month)?;
    }
    archive.update_duckdb(&current_month)?;

    Ok(())
}
