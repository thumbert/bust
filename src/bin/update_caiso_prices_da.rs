use std::{error::Error, path::Path};

use bust::{
    db::{nyiso::dalmp::LmpComponent, prod_db::ProdDb},
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

/// Run this job every day at 18:00[America/New_York]
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let tomorrow = Zoned::now().date().tomorrow().unwrap();
    let archive = ProdDb::caiso_dalmp();
    for i in 0..6 {
        if i == 0 && Zoned::now().hour() < 18 {
            continue;
        }
        let date = tomorrow - i.days();
        let file = archive.filename(&date, LmpComponent::Lmp) + ".gz";
        if !Path::new(&file).exists() {
            match archive.download_file(date).await {
                Ok(_) => info!(
                    "Downloaded CAISO DA LMP hourly prices file for {} successfully",
                    date
                ),
                Err(e) => error!("{:?}", e),
            }
        }
    }
    let current_month = month(tomorrow.year(), tomorrow.month());
    if tomorrow.day() < 5 {
        let prev_month = current_month.previous();
        archive.download_missing_days(prev_month).await?;
        archive.update_duckdb(&prev_month)?;
    }
    archive.update_duckdb(&current_month)?;

    Ok(())
}
