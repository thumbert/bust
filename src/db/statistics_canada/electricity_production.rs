use duckdb::Connection;
use jiff::Timestamp;
use reqwest::blocking::get;
use std::error::Error;
use std::fs::File;
use std::io::copy;
use std::process::Command;


#[derive(Clone)]
pub struct StatisticsCanadaGenerationArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl StatisticsCanadaGenerationArchive {
    /// Return the CSV filename with all historical data from 2008 to present.
    pub fn filename(&self) -> String {
        self.base_dir.to_owned() + "/Raw/25100015-eng.zip"
    }

    pub fn update_duckdb(&self) -> Result<(), Box<dyn Error>> {
        let config = duckdb::Config::default().access_mode(duckdb::AccessMode::ReadWrite)?;
        let conn = Connection::open_with_flags(&self.duckdb_path, config)?;

        conn.execute("DROP TABLE IF EXISTS electricity_production;", [])?;
        let query = format!(
            r#"
        CREATE TABLE electricity_production AS (
            SELECT *
            FROM read_csv('{}/Raw/25100015.csv')
        );
        "#,
            self.base_dir,
        );
        conn.execute(&query, [])?;

        Ok(())
    }

    /// Data is published at the beginning of every month.
    pub fn download_file(&self) -> Result<(), Box<dyn Error>> {
        println!("{}", self.filename());
        let url = "https://www150.statcan.gc.ca/n1/tbl/csv/25100015-eng.zip";
        let mut resp = get(url)?;
        let mut out = File::create(self.filename())?;
        copy(&mut resp, &mut out)?;

        Command::new("unzip")
            .args([
                "-o",
                &self.filename(),
                "-d",
                &(self.base_dir.clone() + "/Raw/"),
            ])
            .spawn()?
            .wait()?;
        Ok(())
    }

    /// Get monthly data as a timeseries
    pub fn get_data(
        &self,
        conn: &Connection,
        type_of_electricity_generation: &str,
        class_of_electricity_producer: &str,
        zone: &str,
    ) -> Result<Vec<(Timestamp, f64)>, Box<dyn Error>> {
        let query = format!(
            r#"
SELECT 
    REF_DATE as month,
    VALUE as MWh,
FROM electricity_production
WHERE "Type of electricity generation" = '{}'
AND "Class of electricity producer" = '{}'
AND "GEO" = '{}'
ORDER BY REF_DATE; 
    "#,
            type_of_electricity_generation, class_of_electricity_producer, zone
        );
        // println!("{}", query);
        let mut stmt = conn.prepare(&query).unwrap();
        let prices_iter = stmt.query_map([], |row| {
            let ts: Timestamp = format!("{}-01T00:00:00Z", row.get::<usize, String>(0)?)
                .parse()
                .unwrap();
            let mw = row.get::<usize, i64>(1).unwrap() as f64;
            Ok((ts, mw))
        })?;
        let prices: Vec<(Timestamp, f64)> = prices_iter.map(|e| e.unwrap()).collect();

        Ok(prices)
    }
}

#[cfg(test)]
mod tests {
    use duckdb::{AccessMode, Config};

    use crate::db::prod_db::ProdDb;
    use std::error::Error;

    use super::*;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::statistics_canada_generation();
        archive.download_file()?;
        archive.update_duckdb()?;
        Ok(())
    }

    #[test]
    fn get_data_test() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::statistics_canada_generation();
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(&archive.duckdb_path, config).unwrap();

        let rows = archive.get_data(
            &conn,
            "Hydraulic turbine",
            "Total all classes of electricity producer",
            "Quebec",
        )?;
        assert_eq!(
            rows[0],
            ("2008-01-01T00:00:00Z".parse().unwrap(), 18523856.0)
        );
        Ok(())
    }
}
