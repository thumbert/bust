use std::{error::Error, path::Path};

use bust::{
    db::{isone::lib_dam::is_dalmp_published, prod_db::ProdDb},
    interval::month::month,
};
use clap::Parser;
use jiff::Zoned;
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
    let archive = ProdDb::isone_dalmp();

    let tomorrow = Zoned::now().date().tomorrow().unwrap();
    while !is_dalmp_published(tomorrow).unwrap() && Zoned::now().hour() < 22 {
        info!(
            "DA LMP for {} is not published yet. Sleeping for 5 minutes...",
            tomorrow
        );
        std::thread::sleep(std::time::Duration::from_secs(300));
    }
    match archive.download_file(tomorrow) {
        Ok(_) => info!(
            "Downloaded ISONE DA LMP hourly prices file for {} successfully",
            tomorrow
        ),
        Err(e) => error!("{:?}", e),
    }
    let current_month = month(tomorrow.year(), tomorrow.month());
    archive.update_duckdb(&current_month)?;

    // repair the previous month's missing files if tomorrow is the first of the month
    if tomorrow.day() == 1 {
        let prev_month = current_month.previous();
        archive.download_missing_days(prev_month)?;
        archive.update_duckdb(&prev_month)?;
    }

    Ok(())
}
