use duckdb::Connection;
use glob::glob;
use jiff::civil::*;
use jiff::Timestamp;
use jiff::ToSpan;
use jiff::Zoned;
use log::error;
use log::info;
use serde::Serialize;
use serde::Serializer;
use std::error::Error;
use std::fs;
use std::fs::File;
use std::io;
use std::path::Path;
use std::process::Command;

use crate::elec::iso::ISONE;
use crate::interval::month::Month;

pub struct NyisoTransmissionOutagesDaArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Debug, Serialize)]
pub struct Row {
    pub as_of_date: Date,
    pub ptid: i32,
    pub equipment_name: String,
    #[serde(
        serialize_with = "serialize_zoned_as_offset", 
    )]
    pub outage_start: Zoned,
    #[serde(
        serialize_with = "serialize_zoned_as_offset",
    )]
    pub outage_end: Zoned,
}

fn serialize_zoned_as_offset<S>(z: &Zoned, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&z.strftime("%Y-%m-%d %H:%M:%S%:z").to_string())
}


// Custom deserialization function for the Zoned field
// fn deserialize_zoned_assume_ny<'de, D>(deserializer: D) -> Result<Zoned, D::Error>
// where
//     D: Deserializer<'de>,
// {
//     struct ZonedVisitor;

//     impl Visitor<'_> for ZonedVisitor {
//         type Value = Zoned;

//         fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
//             formatter.write_str("a timestamp string with or without a zone name")
//         }

//         fn visit_str<E>(self, v: &str) -> Result<Zoned, E>
//         where
//             E: de::Error,
//         {
//             // Otherwise, append the assumed zone
//             let s = format!("{v}[America/New_York]");
//             Zoned::strptime("%F %T%:z[%Q]", &s).map_err(E::custom)
//         }
//     }

//     deserializer.deserialize_str(ZonedVisitor)
// }


impl NyisoTransmissionOutagesDaArchive {
    /// Return the csv filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/"
            + &date.strftime("%Y%m%d").to_string()
            + "outSched.csv"
    }

    pub fn get_data(
        &self,
        conn: &Connection,
        start_date: &Date,
        end_date: &Date,
        equipment_name: Option<String>,
    ) -> Result<Vec<Row>, Box<dyn Error>> {
        let query = format!(
            r#"
SELECT *
FROM nyiso_da_outages
WHERE day >= '{}'
AND day <= '{}'{}
ORDER BY day, ptid; 
    "#,
            start_date,
            end_date,
            match equipment_name {
                Some(name) => format!(" AND equipment_name = '{}'", name),
                None => "".to_string(),
            }
        );
        // println!("{}", query);
        let mut stmt = conn.prepare(&query).unwrap();
        let prices_iter = stmt.query_map([], |row| {
            let n = 719528 + row.get::<usize, i32>(0).unwrap();
            let start: Timestamp =
                Timestamp::from_second(row.get::<usize, i64>(3)? / 1_000_000).unwrap();
            let end: Timestamp =
                Timestamp::from_second(row.get::<usize, i64>(4)? / 1_000_000).unwrap();
            Ok(Row {
                as_of_date: Date::ZERO.checked_add(n.days()).unwrap(),
                ptid: row.get::<usize, i32>(1)?,
                equipment_name: row.get::<usize, String>(2)?,
                outage_start: Zoned::new(start, ISONE.tz.clone()),
                outage_end: Zoned::new(end, ISONE.tz.clone()),
            })
        })?;
        let rows: Vec<Row> = prices_iter.map(|e| e.unwrap()).collect();

        Ok(rows)
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.   
    pub fn update_duckdb(&self, month: Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting NYISO transmission outage files for month {} ...",
            month
        );

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS nyiso_da_outages (
    day DATE,
    ptid INT,
    equipment_name VARCHAR,
    outage_start TIMESTAMPTZ,
    outage_end TIMESTAMPTZ
);


CREATE TEMPORARY TABLE tmp AS
    SELECT 
        "Timestamp"::DATE as day,
        "PTID" as ptid,
        "Equipment Name" as equipment_name,
        "Scheduled Out Date/Time":: TIMESTAMPTZ as outage_start,
        "Scheduled In Date/Time":: TIMESTAMPTZ as outage_end
    FROM read_csv(
        '{}/Raw/{}/{}*outSched.csv.gz', 
        header = true, 
        timestampformat = '%m/%d/%Y %H:%M:%S', 
        columns = {{
            'Timestamp': 'TIMESTAMP',
            'PTID': 'INT32',
            'Equipment Name': 'VARCHAR',
            'Scheduled Out Date/Time': 'TIMESTAMP',
            'Scheduled In Date/Time': 'TIMESTAMP',
        }});

INSERT INTO nyiso_da_outages
(
    SELECT * FROM tmp
    WHERE NOT EXISTS (
        SELECT 1 FROM nyiso_da_outages AS existing
        WHERE existing.day = tmp.day
          AND existing.ptid = tmp.ptid
          AND existing.equipment_name = tmp.equipment_name
          AND existing.outage_start = tmp.outage_start
          AND existing.outage_end = tmp.outage_end
    )
);

            "#,
            self.base_dir,
            month.start_date().year(),
            month.strftime("%Y%m")
        );
        // println!("{}", sql);

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

    pub fn download_file(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let url = format!(
            "http://mis.nyiso.com/public/csv/outSched/{}01outSched_csv.zip",
            month.strftime("%Y%m")
        );
        let resp = reqwest::blocking::get(url).expect("request failed");
        let zip_path = self.filename(&month.start_date()) + ".zip";
        let dir = Path::new(&zip_path).parent().unwrap();
        let _ = fs::create_dir_all(dir);
        let mut out = File::create(&zip_path).expect("failed to create file");
        io::copy(&mut resp.bytes()?.as_ref(), &mut out).expect("failed to copy content");

        Command::new("unzip")
            .args(["-o", &zip_path])
            .current_dir(dir)
            .spawn()
            .unwrap()
            .wait()
            .expect("unzip failed");

        // gzip all csv files for the month.  Need to expand the glob pattern in Rust.
        let pattern = dir.join(format!("{}*outSched.csv", month.strftime("%Y%m")));
        for entry in glob(pattern.to_str().unwrap()).unwrap() {
            match entry {
                Ok(path) => {
                    Command::new("gzip")
                        .arg(path.file_name().unwrap()) // just the filename, since current_dir is set
                        .current_dir(dir)
                        .spawn()
                        .unwrap()
                        .wait()
                        .expect("gzip failed");
                }
                Err(e) => println!("{:?}", e),
            }
        }

        Command::new("rm")
            .args(["-f", &zip_path])
            .current_dir(dir)
            .spawn()
            .unwrap()
            .wait()
            .expect("removed zip file");

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::error::Error;

    use duckdb::Connection;
    use jiff::civil::date;

    use crate::{db::prod_db::ProdDb, interval::term::Term};

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        let archive = ProdDb::nyiso_transmission_outages_da();
        let term = "Jan21-Aug25".parse::<Term>().unwrap();
        for month in term.months() {
            archive.update_duckdb(month)?;
        }

        Ok(())
    }

    #[ignore]
    #[test]
    fn get_data_test() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nyiso_transmission_outages_da();
        let conn = Connection::open(&archive.duckdb_path).unwrap();
        let start_date = date(2006, 1, 1);
        let end_date = date(2006, 1, 3);
        let data = archive.get_data(&conn, &start_date, &end_date, None)?;
        println!("{:?}", data);
        assert_eq!(data.len(), 97);
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nyiso_transmission_outages_da();
        let term = "Jan21-Dec23".parse::<Term>().unwrap();
        for month in term.months() {
            archive.download_file(&month)?;
        }
        Ok(())
    }
}
