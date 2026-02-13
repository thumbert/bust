use std::error::Error;

use bust::{db::prod_db::ProdDb, interval::month::Month};
use jiff::Zoned;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let archive = ProdDb::hq_total_demand();

    if Zoned::now().datetime().day() < 4 {
        let prev_month = Month::containing(Zoned::now().datetime()).previous();
        archive.download_file(&prev_month)?;
        archive.update_duckdb(&prev_month)?;
    }

    let month = Month::containing(Zoned::now().datetime());
    archive.download_file(&month)?;
    archive.update_duckdb(&month)?;

    Ok(())
}
