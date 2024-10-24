use std::error::Error;

use bust::db::prod_db::ProdDb;
use duckdb::Connection;



fn main() -> Result<(),Box<dyn Error>> {
    let archive = ProdDb::isone_sevenday_solar_forecast();
    let conn = Connection::open(archive.duckdb_path)?;



    conn.execute_batch(
        r"
CREATE TABLE IF NOT EXISTS forecast (
    report_date DATE,
    forecast_hour_beginning TIMESTAMPTZ,
    forecast_generation USMALLINT,
);
INSERT INTO forecast
FROM read_csv(
    '/month/rt_reserve_price_2021-01.csv.gz', 
    header = true, 
    timestampformat = '%Y-%m-%dT%H:%M:%S.000%z');
        ")?;

    Ok(())
}