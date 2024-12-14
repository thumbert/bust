use std::{error::Error, fs};

use bust::db::prod_db::ProdDb;
use jiff::Zoned;

/// Run this job every day at 8AM
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let archive = ProdDb::hq_hydro_data();
    archive.download_file()?;

    let day = Zoned::now().date();
    archive.update_duckdb(vec![day])?;

    // Remove all files in tmp/ folder if a new month
    if day.day() == 1 {
        let path = archive.base_dir + "/tmp";
        fs::remove_dir_all(&path).unwrap();
        fs::create_dir_all(&path).unwrap();
    }

    Ok(())
}
