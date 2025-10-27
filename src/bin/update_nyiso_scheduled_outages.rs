use std::error::Error;

use jiff::Zoned;
use bust::db::prod_db::ProdDb;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let archive = ProdDb::nyiso_scheduled_outages();
    archive.download_file()?;

    let today = Zoned::now().date();
    let res = archive.update_duckdb(today);
    match res {
        Ok(_) => log::info!(
            "Uploaded NYISO scheduled outages to DuckDB for day {}",
            today
        ),
        Err(e) => log::error!("Failed to upload NYISO scheduled outages to DuckDB: {}", e),
    }
    Ok(())
}

