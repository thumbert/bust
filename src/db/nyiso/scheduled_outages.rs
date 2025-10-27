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

#[derive(Clone)]
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

#[derive(Debug, Deserialize, Default)]
pub struct QueryOutages {
    pub ptid: Option<i64>,
    pub outage_id: Option<String>,
    pub as_of: Option<Date>,
    pub as_of_gte: Option<Date>,
    pub as_of_lte: Option<Date>,
    pub outage_start_date_gte: Option<Date>,
    pub outage_start_date_lte: Option<Date>,
    pub outage_end_date_gte: Option<Date>,
    pub outage_end_date_lte: Option<Date>,
    pub equipment_name: Option<String>,
    pub equipment_type: Option<String>,
    // incomplete name
    pub equipment_name_like: Option<String>,
}


#[derive(Default)]
pub struct QueryOutagesBuilder {
    inner: QueryOutages,
}

impl QueryOutagesBuilder {
    pub fn new() -> Self {
        Self {
            inner: QueryOutages::default(),
        }
    }

    pub fn ptid(mut self, ptid: i64) -> Self {
        self.inner.ptid = Some(ptid);
        self
    }

    pub fn outage_id<S: Into<String>>(mut self, outage_id: S) -> Self {
        self.inner.outage_id = Some(outage_id.into());
        self
    }

    pub fn as_of(mut self, date: Date) -> Self {
        self.inner.as_of = Some(date);
        self
    }

    pub fn as_of_gte(mut self, date: Date) -> Self {
        self.inner.as_of_gte = Some(date);
        self
    }

    pub fn as_of_lte(mut self, date: Date) -> Self {
        self.inner.as_of_lte = Some(date);
        self
    }

    pub fn outage_start_date_gte(mut self, date: Date) -> Self {
        self.inner.outage_start_date_gte = Some(date);
        self
    }

    pub fn outage_start_date_lte(mut self, date: Date) -> Self {
        self.inner.outage_start_date_lte = Some(date);
        self
    }

    pub fn outage_end_date_gte(mut self, date: Date) -> Self {
        self.inner.outage_end_date_gte = Some(date);
        self
    }

    pub fn outage_end_date_lte(mut self, date: Date) -> Self {
        self.inner.outage_end_date_lte = Some(date);
        self
    }

    pub fn equipment_name<S: Into<String>>(mut self, name: S) -> Self {
        self.inner.equipment_name = Some(name.into());
        self
    }

    pub fn equipment_type<S: Into<String>>(mut self, typ: S) -> Self {
        self.inner.equipment_type = Some(typ.into());
        self
    }

    pub fn equipment_name_like<S: Into<String>>(mut self, name: S) -> Self {
        self.inner.equipment_name_like = Some(name.into());
        self
    }

    pub fn build(self) -> QueryOutages {
        self.inner
    }
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
        query_outages: QueryOutages,
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
    ///
    pub fn update_duckdb(&self, day: Date) -> Result<(), Box<dyn Error>> {
        info!("inserting NYISO outage schedule files for day {} ...", day);

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS scheduled_outages (
    as_of DATE NOT NULL,
    ptid INT64 NOT NULL,
    outage_id VARCHAR NOT NULL,
    equipment_name VARCHAR NOT NULL,
    equipment_type VARCHAR NOT NULL,
    outage_start_date DATE NOT NULL,
    outage_time_out TIME NOT NULL,
    outage_end_date DATE NOT NULL,
    outage_time_in TIME NOT NULL,
    called_in_by VARCHAR NOT NULL,
    status VARCHAR,
    last_update TIMESTAMP,
    message VARCHAR,
    arr INT64
);

CREATE TEMPORARY TABLE tmp
AS (
    SELECT 
        CURRENT_DATE AS as_of,
        "PTID"::int64 AS ptid,
        "Outage ID"::VARCHAR AS outage_id,
        "Equipment Name"::VARCHAR AS equipment_name,
        "Equipment Type"::VARCHAR AS equipment_type,
        "Date Out"::DATE AS outage_start_date,
        "Time Out"::TIME AS outage_time_out,
        "Date In"::DATE AS outage_end_date,
        "Time In"::TIME AS outage_time_in,
        "Called In"::VARCHAR AS called_in_by,
        "Status"::VARCHAR AS status,
        strptime("Status Date", '%m-%d-%Y %H:%M') AS last_update,
        "Message"::VARCHAR AS message,
        "ARR"::int64 AS arr,
    FROM read_csv('{}/Raw/{}/outage_schedule_{}.csv.gz')
);

INSERT INTO scheduled_outages
(
    SELECT * FROM tmp t
    WHERE NOT EXISTS (
        SELECT * FROM scheduled_outages d
        WHERE
            d.as_of = t.as_of AND
            d.ptid = t.ptid AND
            d.outage_id = t.outage_id AND
            d.equipment_name = t.equipment_name AND
            d.equipment_type = t.equipment_type AND
            d.outage_start_date = t.outage_start_date AND
            d.outage_time_out = t.outage_time_out AND
            d.outage_end_date = t.outage_end_date AND
            d.outage_time_in = t.outage_time_in AND
            d.called_in_by = t.called_in_by AND
            d.status = t.status AND
            d.last_update = t.last_update
    )
);
            "#,
            self.base_dir,
            day.year(),
            day
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
            error!("Failed to update duckdb for day {}: {}", day, stderr);
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

    use crate::db::{
        nyiso::scheduled_outages::QueryOutagesBuilder,
        prod_db::ProdDb,
    };

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
        let query_outages = QueryOutagesBuilder::new().ptid(25858).build();
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
