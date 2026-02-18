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

/// Run this job on the 15th of the month at 12:05PM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let today = Zoned::now().date();
    let current_month = month(today.year(), today.month());
    let previous_month = current_month.previous();
    info!("Updating NYISO zonal uplift for {}", previous_month);

    let archive = ProdDb::nyiso_zonal_uplift();
    archive.download_file(&previous_month)?;
    archive.update_duckdb(&previous_month)?;

    Ok(())
}
