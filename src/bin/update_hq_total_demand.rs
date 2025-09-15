use std::error::Error;

use bust::{db::prod_db::ProdDb, interval::month::Month};
use jiff::Zoned;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let archive = ProdDb::hq_total_demand();
    archive.download_file()?;

    let month = Month::containing(Zoned::now().datetime());
    archive.update_duckdb(month)?;

    Ok(())
}
