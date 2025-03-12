use std::{error::Error, path::Path};

use bust::{
    db::prod_db::ProdDb,
    interval::month::month,
};
use jiff::Zoned;
use log::{error, info};

/// Run this job every day after the DAM is published
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(".env/test.env")).unwrap();
    let archive = ProdDb::isone_daas_reserve_data();

    let today = Zoned::now().date();
    if today.day() < 5 {
        let month = month(today.year(), today.month()).previous();
        archive.download_missing_days(month)?;
        match archive.update_duckdb(month) {
            Ok(_) => info!("Updated month {} successfully", month),
            Err(e) => error!("{:?}", e),
        }
    }
    let month = month(today.year(), today.month()).previous();
    archive.download_missing_days(month)?;
    match archive.update_duckdb(month) {
        Ok(_) => info!("Updated month {} successfully", month),
        Err(e) => error!("{:?}", e),
    }

    Ok(())
}
