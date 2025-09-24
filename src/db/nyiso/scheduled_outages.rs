use duckdb::Connection;
use jiff::civil::*;
use jiff::Timestamp;
use jiff::ToSpan;
use jiff::Zoned;
use log::error;
use log::info;
use serde::Deserialize;
use serde::Serialize;
use std::error::Error;
use std::path::Path;
use std::process::Command;

use crate::db::isone::lib_isoexpress::download_file;
use crate::interval::month::Month;

pub struct NyisoScheduledOutagesArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Debug, Serialize, Deserialize)]
pub struct Row {
    pub as_of: Date,
    pub ptid: i64,
    pub outage_id: String,
    pub equipment_name: String,
    pub equipment_type: String,
    pub outage_start_date: Date,
    pub outage_time_out: Time,
    pub outage_end_date: Date,
    pub outage_time_in: Time,
    pub called_in_by: String,
    pub status: Option<String>,
    pub last_update: Option<Timestamp>,
    pub message: Option<String>,
    pub arr: Option<i64>,
}

#[derive(Debug, Deserialize)]
pub struct QueryOutages {
    ptid: Option<i64>,
    outage_id: Option<String>,
    as_of_gte: Option<Date>,
    as_of_lte: Option<Date>,
    outage_start_date_gte: Option<Date>,
    outage_start_date_lte: Option<Date>,
    outage_end_date_gte: Option<Date>,
    outage_end_date_lte: Option<Date>,
    equipment_name: Option<String>,
    equipment_type: Option<String>,
}


impl NyisoScheduledOutagesArchive {
    /// Return the csv filename for the day.  Does not check if the file exists.  
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/outage_schedule_"
            + &date.to_string()
            + ".csv"
    }

    pub fn get_data(
        &self,
        conn: &Connection,
        query_outages: QueryOutages
    ) -> Result<Vec<Row>, Box<dyn Error>> {
        let mut query = String::from("SELECT * FROM scheduled_outages WHERE 1=1");
        if let Some(ptid) = query_outages.ptid {
            query.push_str(&format!(" AND ptid = {}", ptid));
        }
        if let Some(outage_id) = query_outages.outage_id {
            query.push_str(&format!(" AND outage_id = '{}'", outage_id));
        }
        if let Some(as_of_gte) = query_outages.as_of_gte {
            query.push_str(&format!(" AND as_of >= '{}'", as_of_gte));
        }
        if let Some(as_of_lte) = query_outages.as_of_lte {
            query.push_str(&format!(" AND as_of <= '{}'", as_of_lte));
        }
        if let Some(outage_start_date_gte) = query_outages.outage_start_date_gte {
            query.push_str(&format!(
                " AND outage_start_date >= '{}'",
                outage_start_date_gte
            ));
        }
        if let Some(outage_start_date_lte) = query_outages.outage_start_date_lte {
            query.push_str(&format!(
                " AND outage_start_date <= '{}'",
                outage_start_date_lte
            ));
        }
        if let Some(outage_end_date_gte) = query_outages.outage_end_date_gte {
            query.push_str(&format!(
                " AND outage_end_date >= '{}'",
                outage_end_date_gte
            ));
        }
        if let Some(outage_end_date_lte) = query_outages.outage_end_date_lte {
            query.push_str(&format!(
                " AND outage_end_date <= '{}'",
                outage_end_date_lte
            ));
        }
        if let Some(equipment_name) = query_outages.equipment_name {
            query.push_str(&format!(" AND equipment_name = '{}'", equipment_name));
        }
        if let Some(equipment_type) = query_outages.equipment_type {
            query.push_str(&format!(" AND equipment_type = '{}'", equipment_type));
        }
        query.push(';');
        // println!("{}", query);
        let mut stmt = conn.prepare(&query).unwrap();
        let prices_iter = stmt.query_map([], |row| {
            let n = 719528 + row.get::<usize, i32>(0).unwrap();
            Ok(Row {
                as_of: Date::ZERO.checked_add(n.days()).unwrap(),
                ptid: row.get::<usize, i64>(1)?,
                outage_id: row.get::<usize, String>(2)?,
                equipment_name: row.get::<usize, String>(3)?,
                equipment_type: row.get::<usize, String>(4)?,
                outage_start_date: Date::ZERO
                    .checked_add((719528 + row.get::<usize, i32>(5)?).days())
                    .unwrap(),
                outage_time_out: Time::midnight()
                    .saturating_add((row.get::<usize, i64>(6)? / 1_000_000).seconds()),
                outage_end_date: Date::ZERO
                    .checked_add((719528 + row.get::<usize, i32>(7)?).days())
                    .unwrap(),
                outage_time_in: Time::midnight()
                    .saturating_add((row.get::<usize, i64>(8)? / 1_000_000).seconds()),
                called_in_by: row.get::<usize, String>(9)?,
                status: row.get::<usize, Option<String>>(10)?,
                last_update: match row.get_ref_unwrap(11) {
                    duckdb::types::ValueRef::Timestamp(_, value) => {
                        Some(Timestamp::from_second(value / 1_000_000).unwrap())
                    }
                    _ => None,
                },
                message: row.get::<usize, Option<String>>(12)?,
                arr: row.get::<usize, Option<i64>>(13)?,
            })
        })?;
        let rows: Vec<Row> = prices_iter.map(|e| e.unwrap()).collect();

        Ok(rows)
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.   
    pub fn update_duckdb(&self, month: Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting NYISO outage schedule files for month {} ...",
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

    pub fn download_file(&self) -> Result<(), Box<dyn Error>> {
        download_file(
            "http://mis.nyiso.com/public/csv/os/outage-schedule.csv".to_string(),
            false,
            Some("application/json".to_string()),
            Path::new(&self.filename(&Zoned::now().date())),
            true,
        )
        .expect("download failed");

        Ok(())
    }
}

#[cfg(test)]
mod tests {

    use std::error::Error;

    use duckdb::Connection;

    use crate::{db::{nyiso::scheduled_outages::QueryOutages, prod_db::ProdDb}, interval::term::Term};

    // #[ignore]
    // #[test]
    // fn update_db() -> Result<(), Box<dyn Error>> {
    //     let _ = env_logger::builder()
    //         .filter_level(log::LevelFilter::Info)
    //         .is_test(true)
    //         .try_init();
    //     let archive = ProdDb::nyiso_scheduled_outages();
    //     let term = "24Sep25-26Sep25".parse::<Term>().unwrap();
    //     for day in term.days() {
    //         archive.update_duckdb(day)?;
    //     }

    //     Ok(())
    // }

    #[ignore]
    #[test]
    fn get_data_test() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nyiso_scheduled_outages();
        let conn = Connection::open(&archive.duckdb_path).unwrap();
        let query_outages = QueryOutages {
            ptid: Some(25858),
            outage_id: None,
            as_of_gte: None,
            as_of_lte: None,
            outage_start_date_gte: None,
            outage_start_date_lte: None,
            outage_end_date_gte: None,
            outage_end_date_lte: None,
            equipment_name: None,
            equipment_type: None,
        };
        let data = archive.get_data(&conn, query_outages).unwrap();      
        // println!("{:?}", data);
        assert_eq!(data.len(), 1);
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nyiso_scheduled_outages();
        archive.download_file()
    }
}
