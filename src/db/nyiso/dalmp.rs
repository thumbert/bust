use duckdb::Connection;
use itertools::Itertools;
use jiff::{civil::*, tz, Timestamp, ToSpan, Zoned};
use log::{error, info};
use reqwest::blocking::get;
use rust_decimal::Decimal;
use serde::{Deserialize, Serialize};
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::path::Path;
use std::process::Command;
use std::str::FromStr;

use crate::interval::month::Month;

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq, Copy)]
pub enum LmpComponent {
    // locational marginal price
    Lmp,
    // marginal cost losses
    Mcl,
    // marginal cost congestion
    Mcc,
}

impl fmt::Display for LmpComponent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LmpComponent::Lmp => write!(f, "lmp"),
            LmpComponent::Mcl => write!(f, "mcl"),
            LmpComponent::Mcc => write!(f, "mcc"),
        }
    }
}

fn parse_component(s: &str) -> Result<LmpComponent, String> {
    match s.to_lowercase().as_str() {
        "lmp" => Ok(LmpComponent::Lmp),
        "mcl" => Ok(LmpComponent::Mcl),
        "mcc" => Ok(LmpComponent::Mcc),
        _ => Err(format!("Unknown LMP component: {}", s)),
    }
}

impl FromStr for LmpComponent {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parse_component(s) {
            Ok(month) => Ok(month),
            Err(_) => Err(format!("Failed parsing {} as an Lmp component", s)),
        }
    }
}

pub enum NodeType {
    Gen,
    Zone,
}

#[derive(Debug, PartialEq)]
pub struct Row {
    pub hour_beginning: Zoned,
    pub ptid: u32,
    pub component: LmpComponent,
    pub value: Decimal,
}

#[derive(Clone)]
pub struct NyisoDalmpArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl NyisoDalmpArchive {
    /// Get data from DuckDB.  
    /// If `ptids` is `None`, return all of them.
    /// If `components` is `None`, return all three LMP components (lmp, mcc, mlc).
    ///
    pub fn get_data(
        &self,
        conn: &Connection,
        start_date: Date,
        end_date: Date,
        component: LmpComponent,
        ptids: Option<Vec<i32>>,
    ) -> Result<Vec<Row>, Box<dyn Error>> {
        let mut query = "SELECT ptid, hour_beginning, ".to_string();
        query.push_str(&format!("{} FROM dalmp ", component));
        query = format!(
            r#"
            {}
            WHERE hour_beginning >= '{}'
            AND hour_beginning < '{}'
            {}
            ORDER BY ptid, hour_beginning;
        "#,
            query,
            start_date
                .to_zoned(tz::TimeZone::get("America/New_York").unwrap())
                .unwrap()
                .strftime("%Y-%m-%d %H:%M:%S%:z"),
            end_date
                .tomorrow()
                .unwrap()
                .to_zoned(tz::TimeZone::get("America/New_York").unwrap())
                .unwrap()
                .strftime("%Y-%m-%d %H:%M:%S%:z"),
            match ptids {
                Some(ids) => format!("AND ptid in ({})", ids.iter().join("','")),
                None => "".to_string(),
            }
        );
        println!("{}", query);
        let mut stmt = conn.prepare(&query).unwrap();
        let res_iter = stmt.query_map([], |row| {
            let value = match row.get_ref_unwrap(2) {
                duckdb::types::ValueRef::Decimal(v) => v,
                _ => Decimal::MIN,
            };
            let micro: i64 = row.get(1).unwrap();
            let ts = Timestamp::from_second(micro / 1_000_000).unwrap();
            Ok(Row {
                hour_beginning: Zoned::new(
                    ts,
                    jiff::tz::TimeZone::get("America/New_York").unwrap(),
                ),
                ptid: row.get(0).unwrap(),
                component,
                value,
            })
        })?;
        let res: Vec<Row> = res_iter.map(|e| e.unwrap()).collect();
        Ok(res)
    }

    /// Return the full file path of the zip file with data for the entire month  
    pub fn filename(&self, month: &Month, node_type: NodeType) -> String {
        match node_type {
            NodeType::Gen => {
                self.base_dir.to_owned()
                    + "/Raw/"
                    + &month.start_date().strftime("%Y%m%d").to_string()
                    + "damlbmp_gen_csv.zip"
            }
            NodeType::Zone => {
                self.base_dir.to_owned()
                    + "/Raw/"
                    + &month.start_date().strftime("%Y%m%d").to_string()
                    + "damlbmp_zone_csv.zip"
            }
        }
    }

    /// Data is published around 10:30 every day
    /// https://mis.nyiso.com/public/csv/damlbmp/20250501damlbmp_gen_csv.zip
    pub fn download_file(&self, month: Month, node_type: NodeType) -> Result<(), Box<dyn Error>> {
        let binding = self.filename(&month, node_type);
        let path = Path::new(&binding);

        let url = format!(
            "https://mis.nyiso.com/public/csv/damlbmp/{}",
            path.file_name().unwrap().to_str().unwrap()
        );
        let mut resp = get(url)?;
        let mut out = File::create(&binding)?;
        std::io::copy(&mut resp, &mut out)?;
        info!("downloaded file: {}", binding);
        Ok(())
    }

    pub fn setup(&self) -> Result<(), Box<dyn Error>> {
        info!("initializing NYISO DALMP archive ...");
        let dir = Path::new(&self.duckdb_path).parent().unwrap();
        fs::create_dir_all(dir)
            .unwrap_or_else(|_| panic!("Failed to create directory: {}", dir.display()));

        if fs::exists(&self.duckdb_path)? {
            fs::remove_file(&self.duckdb_path)?;
        }
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
    BEGIN;
    CREATE TABLE IF NOT EXISTS dalmp (
        hour_beginning TIMESTAMPTZ NOT NULL,
        ptid INTEGER NOT NULL,
        lmp DECIMAL(9,2) NOT NULL,
        mlc DECIMAL(9,2) NOT NULL,
        mcc DECIMAL(9,2) NOT NULL,
    );
    CREATE INDEX idx ON dalmp (ptid);    
    COMMENT ON TABLE dalmp IS 'Hourly DAM prices for all NYISO zones + generators';
    COMMIT;
        ",
        )?;
        Ok(())
    }

    /// Update duckdb with published data for the month.  No checks are made to see
    /// if there are missing files.  Does not delete any existing data.  So if data
    /// is wrong for some reason, it needs to be manually deleted first!
    ///  
    pub fn update_duckdb(&self, month: Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting zone + gen files from the monthly zip for {} ...",
            month
        );
        let sql = format!(
            r#"
        LOAD zipfs;    
        CREATE TEMPORARY TABLE tmp1 AS SELECT * FROM 'zip://{}/*.csv';
        CREATE TEMPORARY TABLE tmp2 AS SELECT * FROM 'zip://{}/*.csv';

        CREATE TEMPORARY TABLE tmp AS
        (SELECT day + INTERVAL (idx) HOUR AS hour_beginning, ptid, lmp, mlc, mcc
        FROM (
            SELECT 
            strptime("Time Stamp"[0:10], '%m/%d/%Y')::TIMESTAMPTZ AS "day",
            ptid::INTEGER AS ptid,
            row_number() OVER (PARTITION BY ptid, strptime("Time Stamp"[0:10], '%m/%d/%Y')) - 1 AS idx, -- 0 to 23 for each day
            "LBMP ($/MWHr)"::DECIMAL(9,2) AS "lmp",
            "Marginal Cost Losses ($/MWHr)"::DECIMAL(9,2) AS "mlc",
            "Marginal Cost Congestion ($/MWHr)"::DECIMAL(9,2) AS "mcc"
        FROM tmp1))
        UNION
        (SELECT day + INTERVAL (idx) HOUR AS hour_beginning, ptid, lmp, mlc, mcc
        FROM (SELECT 
            strptime("Time Stamp"[0:10], '%m/%d/%Y')::TIMESTAMPTZ AS "day",
            ptid::INTEGER AS ptid,
            row_number() OVER (PARTITION BY ptid, strptime("Time Stamp"[0:10], '%m/%d/%Y')) - 1 AS idx, -- 0 to 23 for each day
            "LBMP ($/MWHr)"::DECIMAL(9,2) AS "lmp",
            "Marginal Cost Losses ($/MWHr)"::DECIMAL(9,2) AS "mlc",
            "Marginal Cost Congestion ($/MWHr)"::DECIMAL(9,2) AS "mcc"
        FROM tmp2));


        INSERT INTO dalmp
        SELECT hour_beginning, ptid, lmp, mlc, mcc FROM tmp
        EXCEPT 
            SELECT * FROM dalmp            
        ORDER BY hour_beginning, ptid;
        "#,
            self.filename(&month, NodeType::Zone),
            self.filename(&month, NodeType::Gen),
        );
        let output = Command::new("duckdb")
            .arg("-c")
            .arg(&sql)
            .arg(&self.duckdb_path)
            .output()
            .expect("Failed to invoke duckdb command");

        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() {
            info!("done");
        } else {
            error!("Failed to update duckdb for month {}: {}", month, stderr);
        }
        // let stdout = String::from_utf8_lossy(&output.stdout);
        // println!("Stdout: {}", stdout);
        // println!("Stderr: {}", stderr);

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::{error::Error, path::Path, vec};

    use rust_decimal_macros::dec;

    use crate::{
        db::{nyiso::dalmp::*, prod_db::ProdDb},
        interval::month::month,
    };

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::nyiso_dalmp();
        // archive.setup()?;

        let months = month(2020, 2).up_to(month(2025, 5))?;
        for month in months {
            archive.update_duckdb(month)?;
        }
        Ok(())
    }

    #[test]
    fn get_data_test() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::nyiso_dalmp();
        let conn = duckdb::Connection::open(archive.duckdb_path.clone())?;
        let rows = archive.get_data(
            &conn,
            date(2024, 11, 3),
            date(2024, 11, 3),
            LmpComponent::Lmp,
            Some(vec![61752]),
        )?;
        assert_eq!(rows.len(), 25);
        assert_eq!(
            rows[2].hour_beginning,
            "2024-11-03T01:00:00-05:00[America/New_York]".parse()?
        );
        assert_eq!(rows[2].value, dec!(27.14));
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::nyiso_dalmp();
        let months = month(2020, 1).up_to(month(2021, 12))?;
        for month in months {
            archive.download_file(month, NodeType::Gen)?;
            archive.download_file(month, NodeType::Zone)?;
        }
        Ok(())
    }
}

// /// Read one csv.zip file corresponding to one month.
// pub fn read_file(&self, path_zip: String) -> Result<Vec<Row>, Box<dyn Error>> {
//     // Open the zip file
//     let file = File::open(path_zip)?;
//     let mut archive = ZipArchive::new(BufReader::new(file))?;

//     let mut all_records: Vec<Row> = Vec::new();

//     // Iterate through each file in the archive
//     for i in 0..archive.len() {
//         let mut file = archive.by_index(i)?;
//         let name = file.name().to_owned();
//         if name.ends_with(".csv") {
//             // Read file to a string (alternatively, use file directly as a reader)
//             let mut contents = String::new();
//             file.read_to_string(&mut contents)?;

//             let date: Date = name[0..8]
//                 .parse()
//                 .map_err(|_| format!("Invalid date in filename: {}", name))?;
//             // need to check DST!
//             // let is_dst = false;

//             // Set up CSV reader
//             let mut rdr = csv::Reader::from_reader(contents.as_bytes());

//             // Parse each record and collect
//             for result in rdr.records() {
//                 let record = result?;
//                 let hour: i8 = record[0][11..13].parse()?;
//                 let hour_beginning = date.at(hour, 0, 0, 0).in_tz("America/New_York")?;
//                 let row = Row {
//                     hour_beginning,
//                     ptid: record[2].parse()?,
//                     lmp: Decimal::from_str(&record[3])?,
//                     mcl: Decimal::from_str(&record[4])?,
//                     mcc: Decimal::from_str(&record[5])?,
//                 };
//                 all_records.push(row);
//             }
//         }
//     }

//     Ok(all_records)
// }
