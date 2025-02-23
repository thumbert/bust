use duckdb::{params, Connection};
use flate2::read::GzDecoder;
use itertools::Itertools;
use jiff::{civil::*, ToSpan};
use log::{error, info};
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::process::Command;

#[derive(Debug, PartialEq)]
pub struct Row {
    pub report_date: Date,
    pub unit_name: String,
    pub percent_online: u8,
}

pub struct GeneratorStatusArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Debug, PartialEq)]
pub struct DailyChangeResult {
    report_date: Date,
    unit_name: String,
    rating: u8,
    previous_rating: u8,
    change: i8,
}

impl GeneratorStatusArchive {
    pub fn get_dod_changes(
        conn: &Connection,
        asof_date: Date,
    ) -> Result<Vec<DailyChangeResult>, Box<dyn Error>> {
        conn.execute(
            "SET VARIABLE asof_date = DATE ?;",
            params![asof_date.to_string()],
        )?;
        let query = r#"
SELECT ReportDt, Unit, Power, Prev_Power, Change
FROM( 
    SELECT ReportDt, 
      Unit, 
      Power,
      LAG(Power) OVER (PARTITION BY Unit ORDER BY ReportDt) as Prev_Power,
      Power - Prev_Power as Change, 
    FROM Status 
    WHERE ReportDt > getvariable('asof_date') - 10
    ) AS a
WHERE Change != 0
AND ReportDt = getvariable('asof_date')
ORDER BY Unit;
        "#;
        // println!("{}", query);
        let mut stmt = conn.prepare(query).unwrap();
        let res_iter = stmt.query_map([], |row| {
            let n = 719528 + row.get::<usize, i32>(0).unwrap();
            Ok(DailyChangeResult {
                report_date: Date::ZERO.checked_add(n.days()).unwrap(),
                unit_name: row.get(1).unwrap(),
                rating: row.get::<usize, u8>(2).unwrap(),
                previous_rating: row.get::<usize, u8>(3).unwrap(),
                change: row.get::<usize, i8>(3).unwrap(),
            })
        })?;
        let res: Vec<DailyChangeResult> = res_iter.map(|e| e.unwrap()).collect();

        Ok(res)
    }

    /// Path to the txt.gz file for a given year.  
    pub fn filename(&self, year: u32) -> String {
        self.base_dir.to_owned() + "/Raw" + "/" + &year.to_string() + "powerstatus.txt.gz"
    }

    /// Read the txt.gz file.  Entries are separated by '|'.
    /// Sort the entries decreasingly by date and unit.  In the original file the last date is at the top.
    ///
    pub fn read_file(&self, path: String) -> Result<Vec<Row>, Box<dyn Error>> {
        // let mut file = File::open(path).unwrap();
        let mut file = GzDecoder::new(File::open(path).unwrap());
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let mut rdr = csv::ReaderBuilder::new()
            .delimiter(b'|')
            .flexible(true)
            .has_headers(true)
            .from_reader(buffer.as_bytes());
        let mut rows: Vec<Row> = rdr
            .records()
            .filter(|x| x.as_ref().unwrap().len() == 3)
            .map(|x| {
                let record = x.unwrap();
                let mmddyyyy = record
                    .get(0)
                    .unwrap()
                    .split_ascii_whitespace()
                    .next()
                    .unwrap();
                Row {
                    report_date: Date::strptime("%m/%d/%Y", mmddyyyy).unwrap(),
                    unit_name: record.get(1).unwrap().to_string(),
                    percent_online: record.get(2).unwrap().parse::<u8>().unwrap(),
                }
            })
            .collect();

        rows.sort_unstable_by_key(|e| (e.report_date, e.unit_name.clone()));

        Ok(rows)
    }

    /// There is a cookie being set on the server side, that's why I'm using
    /// python.
    pub fn download_years(&self, years: Vec<i32>) -> Result<(), Box<dyn Error>> {
        let dir = format!("{}/Raw", self.base_dir);

        let mut out = Command::new("python")
        .args(["/home/adrian/Documents/repos/git/thumbert/elec-server/bin/python/nrc_reactor_status_download.py", 
         &format!("--years={}", years.iter().join(","))])
        .current_dir(&dir)
        .spawn()
        .expect("downloads failed");
        let _ = out.wait();

        // gzip the file for storage
        for year in years {
            let path = format!("{}powerstatus.txt", year);
            Command::new("gzip")
                .args(["-f", &path])
                .current_dir(&dir)
                .spawn()
                .unwrap()
                .wait()
                .unwrap_or_else(|_| panic!("gzip year {} failed", year));
        }

        Ok(())
    }

    /// Update the DB with data from one year.
    /// Return the number of rows inserted for the year or an error.
    ///
    pub fn update_duckdb(&self, year: i32) -> Result<usize, duckdb::Error> {
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute(&format!(
            "
        CREATE TEMPORARY TABLE tmp 
        AS 
            SELECT * 
            FROM read_csv('/home/adrian/Downloads/Archive/NRC/ReactorStatus/Raw/{}powerstatus.txt.gz', 
                delim = '|', 
                header = true, 
                ignore_errors = true,
                columns = {{
                    'ReportDt': 'VARCHAR',
                    'Unit': 'VARCHAR',
                    'Power': 'INT',
            }}
        );", year), [])?;

        conn.execute(
            &format!(
                "
        DELETE FROM Status
        WHERE ReportDt >= '{}-01-01'
        AND ReportDt <= '{}-12-31';",
                year, year
            ),
            [],
        )?;

        conn.execute("
            INSERT INTO Status
            SELECT strptime(split_part(ReportDt, ' ', 1), '%m/%d/%Y')::DATE AS ReportDt, Unit, Power 
            FROM tmp
            ORDER BY ReportDt, Unit;
        ", [])
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
    fn update() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nrc_generator_status();
        archive.update_duckdb(2024)?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_years() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nrc_generator_status();
        archive.download_years((2024..2025).collect_vec())?;
        Ok(())
    }

    #[test]
    fn read_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nrc_generator_status();
        let path = archive.filename(2023);
        let res = archive.read_file(path)?;
        assert_eq!(res.len(), 34078);
        assert_eq!(
            *res.first().unwrap(),
            Row {
                report_date: date(2023, 1, 1),
                unit_name: "Arkansas Nuclear 1".to_string(),
                percent_online: 52,
            }
        );
        assert_eq!(
            *res.last().unwrap(),
            Row {
                report_date: date(2023, 12, 31),
                unit_name: "Wolf Creek 1".to_string(),
                percent_online: 100,
            }
        );
        Ok(())
    }
}
