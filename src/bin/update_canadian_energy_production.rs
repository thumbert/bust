use std::error::Error;

use bust::db::prod_db::ProdDb;

/// Run this job at the beginning of every month, say on the 3rd day 
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let archive = ProdDb::statistics_canada_generation();
    archive.download_file()?;
    archive.update_duckdb()?;

    Ok(())
}
