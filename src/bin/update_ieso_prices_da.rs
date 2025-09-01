use std::{error::Error, path::Path};

use bust::{db::prod_db::ProdDb, interval::month::Month};
use clap::Parser;
use jiff::{ToSpan, Zoned};
use log::{error, info};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

/// Run this job every day at 11:45AM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let mut tomorrow = Zoned::now().date().tomorrow().unwrap();
    if Zoned::now().hour() < 13 {
        tomorrow = tomorrow.checked_sub(1.day())?;
    }

    // area price
    let archive = ProdDb::ieso_dalmp_area();
    for i in 0..5 {
        let date = tomorrow - i.days();
        let file = archive.filename(&date) + ".gz";
        if !Path::new(&file).exists() {
            match archive.download_file(&date) {
                Ok(_) => info!("Downloaded area price file for {} successfully", date),
                Err(e) => error!("{:?}", e),
            }
        }
    }
    let mut months = vec![Month::containing(Zoned::now().datetime())];
    if tomorrow.day() < 5 {
        months.push(months.first().unwrap().previous());
    }
    for month in months {
        archive.make_gzfile_for_month(&month)?;
        archive.update_duckdb(&month)?;
    }

    // zonal prices
    let archive = ProdDb::ieso_dalmp_zonal();
    for i in 0..5 {
        let date = tomorrow - i.days();
        let file = archive.filename(&date) + ".gz";
        if !Path::new(&file).exists() {
            match archive.download_file(&date) {
                Ok(_) => info!("Downloaded zonal prices file for {} successfully", date),
                Err(e) => error!("{:?}", e),
            }
        }
    }
    let mut months = vec![Month::containing(Zoned::now().datetime())];
    if tomorrow.day() < 5 {
        months.push(months.first().unwrap().previous());
    }
    for month in months {
        archive.make_gzfile_for_month(&month)?;
        archive.update_duckdb(&month)?;
    }

    // nodal prices
    let archive = ProdDb::ieso_dalmp_nodes();
    for i in 0..5 {
        let date = tomorrow - i.days();
        let file = archive.filename(&date) + ".gz";
        if !Path::new(&file).exists() {
            match archive.download_file(&date) {
                Ok(_) => info!("Downloaded nodal prices file for {} successfully", date),
                Err(e) => error!("{:?}", e),
            }
        }
        archive.update_duckdb(&date)?;
    }


    Ok(())
}
