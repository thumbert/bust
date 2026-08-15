use std::{error::Error, path::Path};

use bust::{
    db::{isone::ttc_archive::*, prod_db::ProdDb},
    interval::{interval_base::DateExt, month::month},
};
use log::info;

/// Run this job every day at 10:35PM
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(".env/prod.env")).unwrap();
    let archive = ProdDb::isone_ttc();

    // look at the current reports published
    let info = get_ttc_reports_info()?;
    let start_date = info
        .iter()
        .map(|(report_date, _)| report_date)
        .min()
        .unwrap();
    let end_date = info
        .iter()
        .map(|(report_date, _)| report_date)
        .max()
        .unwrap();
    info!(
        "Found reports for dates from {} to {}",
        start_date, end_date
    );
    archive.download_days(start_date.up_to(*end_date))?;
    info!("Downloaded the reports");

    // update the DuckDB database with the new data
    let month_start = month(start_date.year(), start_date.month());
    info!("Updating DuckDB for month {}", month_start);
    archive.update_duckdb(&month_start)?;
    let month_end = month(end_date.year(), end_date.month());
    if month_end != month_start {
        info!("Updating DuckDB for month {}", month_end);
        archive.update_duckdb(&month_end)?;
    }

    info!("Done.  Exiting.");
    Ok(())
}
