use std::error::Error;

use bust::{
    db::prod_db::ProdDb,
};
use clap::Parser;
use jiff::Zoned;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "prod")]
    env: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let asof = Zoned::now().date();
    let archive = ProdDb::nyiso_ptid_table();
    let _ = archive.download_file();
    // No checks are made to see if there are nodes that are no longer active. 
    let _ = archive.update_duckdb(asof);

    Ok(())
}
