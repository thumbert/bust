use csv::StringRecord;
use itertools::Itertools;
use jiff::civil::*;
use jiff::Zoned;
use regex::Regex;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::process::Command;

use crate::interval::month::Month;

use super::mis::lib_mis::MisReport;

#[derive(Debug)]
pub struct Row {
    pub report_date: Date,
    pub forecast_hour_beginning: Zoned,
    pub forecast_generation: usize,
}

pub struct SevendaySolarForecastArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SevendaySolarForecastArchive {
    /// Path to the CSV file with the ISO report for a given day
    pub fn filename(&self, date: Date) -> String {
        self.base_dir.to_owned()
            + "/Raw"
            + "/seven_day_solar_power_forecast_"
            + &date.strftime("%Y%m%d").to_string()
            + ".csv"
    }

    /// Read a raw CSV file as provided by the ISONE
    pub fn read_file(&self, path: String) -> Result<Vec<Row>, Box<dyn Error>> {
        let filename = Path::new(&path).file_name().unwrap().to_str().unwrap();
        let re = Regex::new(r"[0-9]{8}").unwrap();
        let report_date = Date::strptime("%Y%m%d", re.find(filename).unwrap().as_str()).unwrap();

        let mut file = File::open(path).unwrap();
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(buffer.as_bytes());
        // get only the rows with data
        let rows: Vec<StringRecord> = rdr
            .records()
            .filter(|x| x.as_ref().unwrap().get(0) == Some("D"))
            .map(|x| x.unwrap())
            .collect();
        let future_dates: Vec<Date> = rows[1]
            .iter()
            .skip(3)
            .map(|e| Date::strptime("%m/%d/%Y", e).unwrap())
            .collect();

        let mut out: Vec<Row> = Vec::new();
        for row in rows.iter().skip(2) {
            let n = row.len();
            let hour = row.get(2).unwrap();  // "01", "02", "02X", "03", .. "24"
            for j in 3..n {
                if row.get(j) == Some("") {
                    continue;
                }
                let forecast_hour_beginning = MisReport::parse_hour_ending(future_dates[j-3], hour);                
                let forecast_generation = row
                    .get(j)
                    .unwrap()
                    .parse::<usize>()
                    .expect("generation value");
                let one = Row {
                    report_date,
                    forecast_hour_beginning,
                    forecast_generation,
                };
                out.push(one);
                // println!("{:?}", one);
            }
        }
        out.sort_by_key(|e| e.forecast_hour_beginning.clone());

        Ok(out)
    }

    /// Aggregate all the daily files into one monthly file for convenience.
    /// Be strict about missing days and error out.
    ///
    /// File is ready to be uploaded into database.
    ///
    pub fn make_gzfile_for_month(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let file_out = format!(
            "{}/month/solar_forecast_{}.csv",
            self.base_dir.to_owned(),
            month
        );
        let mut wtr = csv::Writer::from_path(&file_out)?;

        for date in month.days() {
            let path = self.filename(date);
            let rows = self.read_file(path)?;
            for row in rows {
                let _ = wtr.write_record(&[
                    row.report_date.to_string(),
                    row.forecast_hour_beginning
                        .strftime("%Y-%m-%dT%H:%M:%S.000%:z")
                        .to_string(),
                    row.forecast_generation.to_string(),
                ]);
            }
        }
        wtr.flush()?;

        // gzip it
        Command::new("gzip")
            .args(["-f", &file_out])
            .current_dir(format!("{}/month", self.base_dir))
            .spawn()
            .unwrap()
            .wait()
            .expect("gzip failed");
        Ok(())
    }

    pub fn download_days(&self, days: Vec<Date>) -> Result<(), Box<dyn Error>> {
        let mut out = Command::new("python")
            .args(["/home/adrian/Documents/repos/git/thumbert/elec-server/bin/python/isone_sevenday_solar_forecast_download.py", 
             &format!("--days={}", days.iter().map(|e| e.strftime("%Y%m%d")).join(","))])
            .current_dir(format!("{}/Raw", self.base_dir))
            .spawn()
            .expect("downloads failed");
        let _ = out.wait();
        Ok(())
    }

    /// Check if the files for some days are missing, and download them.
    pub fn download_missing_days(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let days = month.days();
        let mut missing_days: Vec<Date> = Vec::new();
        for day in days {
            let file = self.filename(day);
            if !Path::new(&file).exists() {
                missing_days.push(day);
            }
        }
        if !missing_days.is_empty() {
            self.download_days(missing_days)?;
        }
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

    #[test]
    fn download_days() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::isone_sevenday_solar_forecast();
        archive.download_days(vec![date(2024, 10, 24)])?;
        Ok(())
    }

    #[test]
    fn read_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::isone_sevenday_solar_forecast();
        let path = archive.filename(date(2024, 8, 1));
        let res = archive.read_file(path)?;
        assert_eq!(res.len(), 7 * 24);
        let x = res
            .iter()
            .find_or_first(|&x| {
                x.forecast_hour_beginning
                    == "2024-08-01 11:00[America/New_York]"
                        .parse::<Zoned>()
                        .unwrap()
            })
            .unwrap();
        assert_eq!(x.forecast_generation, 789);
        Ok(())
    }

    #[test]
    fn read_file_dst() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::isone_sevenday_solar_forecast();
        let path = archive.filename(date(2024, 10, 29));
        let res = archive.read_file(path)?;
        assert_eq!(res.len(), 7 * 24 + 1);
        // println!("{:?}", res);
        let x = res
            .iter()
            .find_or_first(|&x| {
                x.forecast_hour_beginning
                    == "2024-11-03 11:00[America/New_York]"
                        .parse::<Zoned>()
                        .unwrap()
            })
            .unwrap();
        assert_eq!(x.forecast_generation, 696);
        Ok(())
    }


    #[test]
    fn make_gzfile() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::isone_sevenday_solar_forecast();
        let month = "2024-09".parse::<Month>()?;
        let _ = archive.make_gzfile_for_month(&month);
        Ok(())
    }
}
