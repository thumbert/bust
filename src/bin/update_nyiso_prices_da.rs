use std::{error::Error, path::Path};

use bust::{
    db::{nyiso::dalmp::*, prod_db::ProdDb},
    interval::month::{month, Month},
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

/// Run this job every day at 11:00AM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let mut asof = Zoned::now().date();
    // let mut asof = date(2025, 6, 5);
    if Zoned::now().hour() >= 10 {
        asof = asof.tomorrow().unwrap();
    }
    info!("Updating NYISO DALMP for asof date: {}", asof);

    let current_month = month(asof.year(), asof.month());
    let mut months: Vec<Month> = Vec::new();
    if asof.day() < 4 {
        months.push(current_month.previous());
    }
    months.push(current_month);
    info!("Updating NYISO DALMP for months: {:?}", months);

    let archive = ProdDb::nyiso_dalmp();
    for month in months {
        archive.download_file(month, NodeType::Gen)?;
        archive.download_file(month, NodeType::Zone)?;
        archive.update_duckdb(month)?;
    }

    Ok(())
}
