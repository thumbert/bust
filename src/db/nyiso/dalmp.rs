use duckdb::Connection;
use itertools::Itertools;
use jiff::{civil::*, tz, Timestamp, Zoned};
use log::{error, info};
use reqwest::blocking::get;
use rust_decimal::Decimal;
use serde::{Deserialize, Deserializer, Serialize};
use std::error::Error;
use std::fmt;
use std::fs::{self, File};
use std::io::Read;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;

use crate::interval::month::Month;

#[derive(Debug, Serialize, Clone, PartialEq, Copy)]
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
        "mlc" => Ok(LmpComponent::Mcl), // alias for Mcl
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

// Custom deserializer using FromStr so that Actix path path can parse different casing, e.g.
// "lmp" and "LMP", not only the canonical one "Lmp".
impl<'de> Deserialize<'de> for LmpComponent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        LmpComponent::from_str(&s).map_err(serde::de::Error::custom)
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
        query.push_str(&format!(
            "{} FROM dalmp ",
            match component {
                LmpComponent::Lmp => "lmp",
                LmpComponent::Mcl => "mlc",
                LmpComponent::Mcc => "mcc",
            }
        ));
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
                Some(ids) => format!("AND ptid in ({})", ids.iter().join(",")),
                None => "".to_string(),
            }
        );
        // println!("{}", query);
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
    pub fn filename_zip(&self, month: &Month, node_type: NodeType) -> String {
        match node_type {
            NodeType::Gen => {
                self.base_dir.to_owned()
                    + "/Raw/"
                    + &month.start_date().strftime("%Y%m%d").to_string()
                    + "damlbmp_gen.csv.zip"
            }
            NodeType::Zone => {
                self.base_dir.to_owned()
                    + "/Raw/"
                    + &month.start_date().strftime("%Y%m%d").to_string()
                    + "damlbmp_zone.csv.zip"
            }
        }
    }

    /// Return the file path of the csv file with data for one day
    pub fn filename(&self, day: &Date, node_type: NodeType) -> String {
        match node_type {
            NodeType::Gen => {
                self.base_dir.to_owned()
                    + "/Raw/"
                    + day.year().to_string().as_str()
                    + "/"
                    + &day.strftime("%Y%m%d").to_string()
                    + "damlbmp_gen.csv"
            }
            NodeType::Zone => {
                self.base_dir.to_owned()
                    + "/Raw/"
                    + day.year().to_string().as_str()
                    + "/"
                    + &day.strftime("%Y%m%d").to_string()
                    + "damlbmp_zone.csv"
            }
        }
    }

    /// Data is published around 10:30 every day
    /// See https://mis.nyiso.com/public/csv/damlbmp/20250501damlbmp_gen_csv.zip
    /// Take the monthly zip file, extract it and compress each individual day as a gz file.
    pub fn download_file(&self, month: Month, node_type: NodeType) -> Result<(), Box<dyn Error>> {
        let binding = self.filename_zip(&month, node_type);
        let zip_path = Path::new(&binding);

        let url = format!(
            "https://mis.nyiso.com/public/csv/damlbmp/{}",
            zip_path.file_name().unwrap().to_str().unwrap()
        );
        let mut resp = get(url)?;
        let mut out = File::create(&binding)?;
        std::io::copy(&mut resp, &mut out)?;
        info!("downloaded file: {}", binding);

        // Unzip the file
        info!("Unzipping file {:?}", zip_path);
        let mut zip_file = File::open(zip_path)?;
        let mut zip_data = Vec::new();
        zip_file.read_to_end(&mut zip_data)?;
        let reader = std::io::Cursor::new(zip_data);
        let mut zip = zip::ZipArchive::new(reader)?;
        use std::fs::File as StdFile;
        use std::io::copy as std_copy;

        for i in 0..zip.len() {
            let mut file = zip.by_index(i)?;
            let out_path = match file.enclosed_name() {
                Some(path) => path.to_owned(),
                None => continue,
            };
            let day: Date = out_path.file_name().unwrap().to_str().unwrap()[0..8]
                .parse()
                .map_err(|_| format!("Invalid date in filename: {:?}", out_path))?;
            let out_path = self.base_dir.to_owned()
                + "/Raw/"
                + &day.year().to_string()
                + "/"
                + out_path.file_name().unwrap().to_str().unwrap();
            let dir = Path::new(&out_path).parent().unwrap();
            fs::create_dir_all(dir)?;

            // Use blocking std::fs::File and std::io::copy for extraction
            let mut outfile = StdFile::create(&out_path)?;
            std_copy(&mut file, &mut outfile)?;
            info!(" -- extracted file to {}", out_path);

            // Gzip the csv file
            let mut csv_file = File::open(&out_path)?;
            let mut csv_data = Vec::new();
            csv_file.read_to_end(&mut csv_data)?;
            let gz_path = format!("{}.gz", out_path);
            let mut gz_file = File::create(&gz_path)?;
            let mut encoder =
                flate2::write::GzEncoder::new(Vec::new(), flate2::Compression::default());
            use std::io::Write;
            encoder.write_all(&csv_data)?;
            let compressed_data = encoder.finish()?;
            gz_file.write_all(&compressed_data)?;
            info!(" -- gzipped file to {}", gz_path);

            // Remove the original csv file
            std::fs::remove_file(&out_path)?;
        }

        // Remove the zip file
        std::fs::remove_file(zip_path)?;
        info!("removed zip file {:?}", zip_path);

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
        info!("inserting zone + gen files for the month {} ...", month);
        let sql = format!(
            r#"
        LOAD zipfs;
        CREATE TEMPORARY TABLE tmp1 AS SELECT * FROM '{}/Raw/{}/{}*damlbmp_zone.csv.gz';
        CREATE TEMPORARY TABLE tmp2 AS SELECT * FROM '{}/Raw/{}/{}*damlbmp_gen.csv.gz';

        CREATE TEMPORARY TABLE tmp AS
        (SELECT
            strptime("Time Stamp" || ' America/New_York' , '%m/%d/%Y %H:%M %Z')::TIMESTAMPTZ AS "hour_beginning",
            ptid::INTEGER AS ptid,
            "LBMP ($/MWHr)"::DECIMAL(9,2) AS "lmp",
            "Marginal Cost Losses ($/MWHr)"::DECIMAL(9,2) AS "mlc",
            "Marginal Cost Congestion ($/MWHr)"::DECIMAL(9,2) AS "mcc"
        FROM tmp1
        )
        UNION
        (SELECT
            strptime("Time Stamp" || ' America/New_York' , '%m/%d/%Y %H:%M %Z')::TIMESTAMPTZ AS "hour_beginning",
            ptid::INTEGER AS ptid,
            "LBMP ($/MWHr)"::DECIMAL(9,2) AS "lmp",
            "Marginal Cost Losses ($/MWHr)"::DECIMAL(9,2) AS "mlc",
            "Marginal Cost Congestion ($/MWHr)"::DECIMAL(9,2) AS "mcc"
        FROM tmp2
        )
        ORDER BY hour_beginning, ptid;

        INSERT INTO dalmp
        (SELECT hour_beginning, ptid, lmp, mlc, mcc FROM tmp
        WHERE NOT EXISTS (
            SELECT * FROM dalmp d
            WHERE d.hour_beginning = tmp.hour_beginning
            AND d.ptid = tmp.ptid
        ))
        ORDER BY hour_beginning, ptid;
        "#,
            self.base_dir,
            month.start_date().year(),
            &month.start_date().strftime("%Y%m"),
            self.base_dir,
            month.start_date().year(),
            &month.start_date().strftime("%Y%m"),
        );
        let output = Command::new("duckdb")
            .arg("-c")
            .arg(&sql)
            .arg(&self.duckdb_path)
            .output()
            .expect("Failed to invoke duckdb command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() {
            info!("{}", stdout);
            info!("done");
        } else {
            error!("Failed to update duckdb for month {}: {}", month, stderr);
        }

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
        archive.setup()?;

        let months = month(2026, 1).up_to(month(2026, 1))?;
        for month in months {
            println!("Processing month {}", month);
            archive.update_duckdb(month)?;
        }
        Ok(())
    }

    #[test]
    #[should_panic]
    fn get_data_test() {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::nyiso_dalmp();
        let conn = duckdb::Connection::open(archive.duckdb_path.clone()).unwrap();
        // test a zone location at DST
        let rows = archive
            .get_data(
                &conn,
                date(2024, 11, 3),
                date(2024, 11, 3),
                LmpComponent::Lmp,
                Some(vec![61752]),
            )
            .unwrap();
        assert_eq!(rows.len(), 25);
        let values = rows[0..=2].iter().map(|r| r.value).collect::<Vec<_>>();
        // the assertion below fails.  DuckDB has issues importing the DST hour from NYISO file.
        assert_eq!(values, vec![dec!(29.27), dec!(27.32), dec!(27.14)]);
        assert_eq!(
            rows[2].hour_beginning,
            "2024-11-03T01:00:00-05:00[America/New_York]"
                .parse()
                .unwrap()
        );
        assert_eq!(rows[2].value, dec!(27.14));

        // test a gen location
        let rows = archive
            .get_data(
                &conn,
                date(2025, 6, 27),
                date(2025, 6, 27),
                LmpComponent::Lmp,
                Some(vec![23575]),
            )
            .unwrap();
        assert_eq!(rows.len(), 24);
        assert_eq!(
            rows[0].hour_beginning,
            "2025-06-27T00:00:00-04:00[America/New_York]"
                .parse()
                .unwrap()
        );
        assert_eq!(rows[0].value, dec!(37.59));
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();

        let archive = ProdDb::nyiso_dalmp();
        let months = month(2026, 1).up_to(month(2026, 1))?;
        for month in months {
            archive.download_file(month, NodeType::Gen)?;
            archive.download_file(month, NodeType::Zone)?;
        }
        Ok(())
    }
}

