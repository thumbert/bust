use csv::StringRecord;
use itertools::Itertools;
use jiff::civil::*;
use jiff::Zoned;
use regex::Regex;
use std::error::Error;
use std::fs::File;
use std::io;
use std::io::Read;
use std::path::Path;
use std::process::Command;
use std::time::Duration;

use crate::interval::month::Month;

#[derive(Debug)]
pub struct Row {
    pub report_date: Date,
    pub unit_name: String,
    pub percent_online: u8,
}

pub struct GeneratorStatusArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl GeneratorStatusArchive {
    /// Path to the CSV file with the ISO report for a given day
    pub fn filename(&self, year: u32) -> String {
        self.base_dir.to_owned() + "/Raw" + "/" + &year.to_string() + "powerstatus.txt"
    }


    /// Read a raw CSV file as provided by the ISONE
    pub fn read_file(&self, path: String) -> Result<Vec<Row>, Box<dyn Error>> {
        let mut file = File::open(path).unwrap();
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b'|')
            .flexible(true)
            .has_headers(true)
            .from_reader(buffer.as_bytes());
        let rows: Vec<Row> = rdr
            .records()
            .filter(|x| x.as_ref().unwrap().len() == 3)
            .map(|x| {
                let record = x.unwrap();
                Row {
                    report_date: date(2023, 1, 1),
                    unit_name: record.get(1).unwrap().to_string(),
                    percent_online: record.get(2).unwrap().parse::<u8>().unwrap(),
                }
            })
            .collect();
        Ok(rows)
    }

    // Not working! 
    // looks like there is a cookie being set on the server side    
    pub fn download_year(&self, year: i32) -> Result<(), Box<dyn Error>> {
        let url = format!("https://www.nrc.gov/reading-rm/doc-collections/event-status/reactor-status/{}/{}powerstatus.txt", year, year);
        println!("{}", url);
        let client = reqwest::blocking::Client::builder()
            .timeout(Duration::from_secs(60))
            .build()?;
        // let resp = reqwest::blocking::get(url).expect("request failed");
        let resp = client.get(url).send().expect("request failed");
        let body = resp.text().expect("body invalid");
        let path = self.base_dir.clone() + "/Raw/" + &year.to_string() + "PowerStatus.txt";
        let mut out = File::create(path).expect("failed to create file");
        io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use itertools::Itertools;
    use jiff::civil::date;
    use std::error::Error;

    use crate::db::prod_db::ProdDb;

    use super::*;

    #[ignore]
    #[test]
    fn download_year() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nrc_generator_status();
        archive.download_year(2022)?;
        Ok(())
    }

    #[test]
    fn read_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nrc_generator_status();
        let path = archive.filename(2023);
        let res = archive.read_file(path)?;
        assert_eq!(res.len(), 34078);
        // let x = res
        //     .iter()
        //     .find_or_first(|&x| {
        //         x.forecast_hour_beginning
        //             == "2024-08-01 11:00[America/New_York]"
        //                 .parse::<Zoned>()
        //                 .unwrap()
        //     })
        //     .unwrap();
        // assert_eq!(x.forecast_generation, 789);
        Ok(())
    }
}
