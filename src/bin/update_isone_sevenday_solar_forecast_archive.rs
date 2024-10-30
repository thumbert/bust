use std::{error::Error, path::Path};

use bust::{db::prod_db::ProdDb, interval::month::Month};
use duckdb::{params, Connection};
use jiff::{civil::Date, ToSpan, Zoned};
use log::info;



/// Insert today's report into the DB
fn add_day(date: Date) -> Result<(), Box<dyn Error>> {
    let archive = ProdDb::isone_sevenday_solar_forecast();
    let conn = Connection::open(&archive.duckdb_path)?;

    // check if the data is already there not add it again
    let n: usize = conn
        .query_row(
            &format!(
                "SELECT COUNT(*) FROM forecast WHERE report_date = '{}'",
                date.strftime("%Y-%m-%d")
            ),
            [],
            |row| row.get(0),
        )
        .unwrap();
    if n == 0 {
        let data = archive.read_file(archive.filename(date))?;
        for one in data {
            conn.execute(r"INSERT INTO forecast (report_date, forecast_hour_beginning, forecast_generation) VALUES (?, ?, ?)", 
                params![one.report_date.strftime("%Y-%m-%d").to_string(), one.forecast_hour_beginning.strftime("%Y-%m-%dT%H:%M:%S.000%z").to_string(), one.forecast_generation])?;
        }
        info!("Inserting data for {} in the DB", date);
    } else {
        info!("Data already in the DB for {}, not inserting", date);
    } 

    Ok(())
}


/// Run this job every day at 10AM
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let archive = ProdDb::isone_sevenday_solar_forecast();

    // the report for next day gets published at 9:45AM every day
    let now = Zoned::now();
    if i32::from(now.hour()) * 60 + i32::from(now.minute()) > 585 {
        let today = now.date();
        archive.download_days(vec![today])?;
        add_day(today)?;

        if today == today.first_of_month() {
            // make the gz file for the previous month
            let month = today
                .saturating_sub(1.day())
                .strftime("%Y-%m")
                .to_string()
                .parse::<Month>()?;

            // check for missing downloaded days
            archive.download_missing_days(&month)?;

            // make the gzfile for month (need all days!)
            archive.make_gzfile_for_month(&month)?;

            let conn = Connection::open(archive.duckdb_path)?;
            // remove what you have in the DB
            let stmt = format!(
                r"
        DELETE FROM forecast
        WHERE forecast_hour_beginning >= {}
        AND forecast_hour_beginning < {}
        ",
                today.saturating_sub(1.month()).strftime("%Y-%m-%d"),
                today.strftime("%Y-%m-%d"),
            );
            println!("{}", stmt);
            conn.execute_batch(&stmt)?;

            // upload this month's data to the DB
            let stmt = format!(
                r"
        INSERT INTO forecast
        FROM read_csv(
            '{}/month/rt_reserve_price_{}.csv.gz', 
            header = true, 
            timestampformat = '%Y-%m-%dT%H:%M:%S.000%z');
                ",
                archive.base_dir,
                today.strftime("%Y-%m")
            );
            conn.execute_batch(&stmt)?;
        }
    }

    Ok(())
}
