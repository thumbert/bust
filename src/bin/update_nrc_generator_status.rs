use std::error::Error;

use bust::db::prod_db::ProdDb;
use jiff::Zoned;
use log::{error, info};

/// Run this job every day at 7AM
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let archive = ProdDb::nrc_generator_status();
    let yesterday = Zoned::now().date().yesterday().unwrap();
    let year = yesterday.year();
    match archive.download_years(vec![year.into()]) {
        Ok(_) => info!("Downloaded file successfully"),
        Err(e) => error!("{:?}", e),
    }

    match archive.update_duckdb(year.into())  {
        Ok(n) => info!("{} rows were updated", n),
        Err(e) => error!("{}", e),
    }

    Ok(())
}
