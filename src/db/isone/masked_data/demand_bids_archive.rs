use jiff::civil::*;
use std::error::Error;
use std::path::Path;

use crate::db::isone::lib_isoexpress;

#[derive(Clone)]
pub struct DemandBidsArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl DemandBidsArchive {
    /// Return the json filename for the day.  Does not check if the file exists.
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/hbdayaheaddemandbid_"
            + &date.strftime("%Y%m%d").to_string()
            + ".json"
    }

    /// https://webservices.iso-ne.com/api/v1.1/hbdayaheaddemandbid/day/20250301
    pub fn download_file(&self, date: &Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = date.strftime("%Y%m%d");
        lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/hbdayaheaddemandbid/day/{}",
                yyyymmdd
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(date)),
            true,
        )
    }

}

#[cfg(test)]
mod tests {

    use jiff::civil::date;
    use std::{error::Error, path::Path};

    use crate::{
        db::prod_db::ProdDb,
        interval::{interval::DateExt, term::Term},
    };

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::isone_masked_demand_bids();
        let term = "Apr25-May25".parse::<Term>()?;
        for day in &term.days() {
            println!("Processing {}", day);
            archive.download_file(day)?;
        }
        let months = term.months();
        for month in &months {
            println!("Updating DuckDB for month {}", month);
            // archive.update_duckdb(month)?;
        }

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_masked_demand_bids();
        let days = date(2021, 4, 3).up_to(date(2021, 4, 9));
        for day in days {
            archive.download_file(&day)?;
        }   
        Ok(())
    }
}
