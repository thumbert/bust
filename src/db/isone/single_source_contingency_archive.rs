use duckdb::Connection;
use jiff::civil::*;
use log::info;
use std::error::Error;
use std::path::Path;

#[derive(Clone)]
pub struct SingleSourceContingencyArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SingleSourceContingencyArchive {
    /// Return the json filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/ssc_"
            + &date.to_string()
            + ".json"
    }

    /// Data is updated every 5 min or so
    pub fn download_file(&self, date: Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        super::lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/singlesrccontingencylimits/day/{}",
                yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(&date)),
            true,
        )
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.
    pub fn update_duckdb(&self, days: Vec<Date>) -> Result<(), Box<dyn Error>> {
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
                CREATE TABLE IF NOT EXISTS WaterLevel (
                    station_id VARCHAR NOT NULL,
                    hour_beginning TIMESTAMP NOT NULL,
                    value DOUBLE NOT NULL,
                );",
        )?;

        // for day in days {
        //     // extract the water level data
        //     let path = self.filename(&day) + ".gz";
        //     if !Path::new(&path).exists() {
        //         info!("No file for {}.  Skipping", day);
        //         continue;
        //     }
        //     let xs = self.process_file(&path, Variable::WaterLevel)?;
        //     let path = self.base_dir.clone() + &format!("/tmp/water_level_data_{}.csv", day);
        //     let mut wtr = csv::Writer::from_path(path)?;
        //     for x in xs {
        //         wtr.serialize(x)?;
        //     }
        //     wtr.flush()?;

        //     // insert into duckdb
        //     let query = format!(
        //         r"
        //         INSERT INTO WaterLevel
        //             SELECT station_id, hour_beginning, value
        //             FROM read_csv('{}/tmp/water_level_data_{}.csv',
        //                 header = true)
        //             EXCEPT SELECT * FROM WaterLevel;
        //         ",
        //         self.base_dir, day
        //     );
        //     match conn.execute(&query, []) {
        //         Ok(updated) => info!("{} rows were updated", updated),
        //         Err(e) => error!("{}", e),
        //     }
        // }

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use jiff::{civil::date, ToSpan};
    use std::{error::Error, path::Path};

    use crate::db::prod_db::ProdDb;

    use super::*;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        let archive = ProdDb::isone_single_source_contingency();
        // let days = vec![date(2024, 12, 4), date(2024, 12, 5), date(2024, 12, 6)];
        let days = date(2024, 12, 8).series(1.day()).take(5).collect();

        archive.update_duckdb(days)
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_single_source_contingency();
        archive.download_file(date(2025, 1, 9))?;
        Ok(())
    }
}
