use std::error::Error;

use bust::interval::month::Month;
use jiff::Zoned;
use bust::db::prod_db::ProdDb;

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let archive = ProdDb::nyiso_transmission_outages_da();

    let today = Zoned::now().date();
    let mut months = vec![Month::containing(Zoned::now().datetime())];
    if today.day() <= 5 {
        months.push(months[0].previous());
    }

    for month in months {
        archive.download_file(&month)?;

        let res = archive.update_duckdb(month);
        match res {
            Ok(_) => log::info!(
                "Uploaded NYISO transmission outages to DuckDB for month {}",
                month
            ),
            Err(e) => log::error!(
                "Failed to upload NYISO transmission outages to DuckDB: {}",
                e
            ),
        }
    }
    Ok(())
}

