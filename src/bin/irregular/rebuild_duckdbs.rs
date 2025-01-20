use log::{error, info};
use std::{
    error::Error,
    fs::{self},
};

use bust::db::prod_db::ProdDb;
use duckdb::{params, Connection};
use regex::Regex;

fn rebuild_isone_sevenday_solar_forecast() -> Result<(), Box<dyn Error>> {
    info!("rebuilding isone_seven_day_forecast archive ...");
    let archive = ProdDb::isone_sevenday_solar_forecast();
    fs::remove_file(&archive.duckdb_path)?;
    let conn = Connection::open(archive.duckdb_path)?;
    conn.execute_batch(
        r"
CREATE TABLE IF NOT EXISTS forecast (
    report_date DATE,
    forecast_hour_beginning TIMESTAMPTZ,
    forecast_generation USMALLINT,
);",
    )?;

    // list all the monthly files and add them to the db, in order
    let mut paths: Vec<_> = fs::read_dir(archive.base_dir.clone() + "/month")
        .unwrap()
        .map(|e| e.unwrap())
        .collect();
    paths.sort_by_key(|e| e.path());

    let re = Regex::new(r"[0-9]{4}-[0-9]{2}").unwrap();
    for path in paths {
        let filename = path.file_name();
        let month = re.find(filename.to_str().unwrap()).unwrap().as_str();
        info!("month {month} ...");
        let sql = format!(
            r"
INSERT INTO forecast
FROM read_csv(
    '{}/month/solar_forecast_{}.csv.gz', 
    header = true, 
    timestampformat = '%Y-%m-%dT%H:%M:%S.000%z');",
            archive.base_dir, month
        );
        match conn.execute(&sql, params![]) {
            Ok(n) => info!("  inserted {} rows", n),
            Err(e) => error!("{:?}", e),
        }
    }
    info!("done\n");
    Ok(())
}

fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    rebuild_isone_sevenday_solar_forecast()?;
    Ok(())
}
