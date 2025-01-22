use std::error::Error;

use bust::{
    db::{isone::mis::lib_mis::MisArchiveDuckDB, prod_db::ProdDb},
    interval::month::Month,
};
use duckdb::{params, Connection};
use jiff::{
    civil::{Date, DateTime},
    ToSpan, Zoned,
};
use log::{error, info};

/// Run this job every day at 10AM
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    info!("Starting ...");

    let archives: Vec<Box<dyn MisArchiveDuckDB>> =
        vec![Box::new(ProdDb::sd_daasdt()), Box::new(ProdDb::sd_rtload())];

    for archive in archives {
        let months = archive.get_months();
        println!("{:?}", months);
    }

    info!("Done");

    Ok(())
}
