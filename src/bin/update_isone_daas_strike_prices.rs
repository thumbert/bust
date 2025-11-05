use std::{error::Error, path::Path};

use bust::{
    db::prod_db::ProdDb,
    interval::month::month,
};
use clap::Parser;
use jiff::Zoned;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

/// Run this job on the first of the month at 10:33PM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let archive = ProdDb::isone_daas_strike_prices();
    let today = Zoned::now().date();
    let current_month = month(today.year(), today.month());
    let m = current_month.previous();
    archive.download_missing_days(m)?;
    archive.update_duckdb(m)?;

    Ok(())
}
