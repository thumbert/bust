use std::{error::Error, path::Path};

use bust::db::prod_db::ProdDb;
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

/// Run this job every day at 12:45AM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let tomorrow = Zoned::now().date().tomorrow().unwrap();

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


    Ok(())
}
