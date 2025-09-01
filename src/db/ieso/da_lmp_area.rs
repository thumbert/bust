use flate2::read::GzDecoder;
use jiff::tz::{self, TimeZone};
use jiff::{civil::*, Zoned};
use log::{error, info};
use quick_xml::de::from_str;
use rust_decimal::Decimal;
use std::error::Error;
use std::fs::File;
use std::io::Read;
use std::path::Path;
use std::process::Command;
use std::str::FromStr;

use crate::db::isone::lib_isoexpress::download_file;
use crate::interval::month::Month;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct IesoDaLmpAreaArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IesoDaLmpAreaArchive {
    pub fn get_missing_days() -> Vec<Date> {
        vec![date(2025, 6, 1), date(2025, 6, 2)]
    }

    /// Return the xml filename for the day
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/PUB_DAHourlyOntarioZonalPrice_"
            + &date.strftime("%Y%m%d").to_string()
            + ".xml"
    }

    /// Data is published every day after 12PM
    pub fn download_file(&self, date: &Date) -> Result<(), Box<dyn Error>> {
        download_file(
            format!(
                "https://reports-public.ieso.ca/public/DAHourlyOntarioZonalPrice/{}",
                Path::new(&self.filename(date))
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("")
            ),
            false,
            None,
            Path::new(&self.filename(date)),
            true,
        )
    }

    /// Aggregate all the daily files into one monthly file for convenience.
    /// Be strict about missing days and error out.
    ///
    /// File is ready to be uploaded into database.
    ///
    pub fn make_gzfile_for_month(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let file_out = format!(
            "{}/month/area_da_prices_{}.csv",
            self.base_dir.to_owned(),
            month
        );
        let mut wtr = csv::Writer::from_path(&file_out)?;
        wtr.write_record([
            "hour_beginning",
            "lmp",
            "mcc",
            "mcl",
        ])?;

        let mut last = Zoned::now().date();
        if Zoned::now().hour() > 13 {
            last = last.tomorrow()?;
        }
        for day in month.days() {
            if day > last {
                continue;
            }
            if IesoDaLmpAreaArchive::get_missing_days().contains(&day) {
                continue;
            }
            let rows = self.read_file(&day)?;
            for row in rows {
                let _ = wtr.write_record(&[
                    row.begin_hour
                        .strftime("%Y-%m-%dT%H:%M:%S.000%:z")
                        .to_string(),
                    row.lmp.to_string(),
                    row.mcc.to_string(),
                    row.mcl.to_string(),
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

    pub fn read_file(&self, date: &Date) -> Result<Vec<Row>, Box<dyn Error>> {
        let path_gz = self.filename(date) + ".gz";
        let mut file = GzDecoder::new(File::open(path_gz).unwrap());
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let doc: Document = from_str(&buffer)?;
        let mut rows: Vec<Row> = Vec::new();
        let delivery_date: Date = doc.doc_body.delivery_date.parse()?;
        for hpc in doc.doc_body.hourly_price_components {
            let begin_hour = delivery_date
                .at(hpc.hour - 1, 0, 0, 0)
                .to_zoned(TimeZone::fixed(tz::offset(-5)))?;
            rows.push(Row {
                begin_hour,
                lmp: Decimal::from_str(&hpc.lmp)?,
                mcc: Decimal::from_str(&hpc.mcc)?,
                mcl: Decimal::from_str(&hpc.mcl)?,
            });
        }

        Ok(rows)
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.
    /// This method only works well for a few day.  For a lot of days, don't loop over days.
    /// Consider using DuckDB directly by globbing the file names.
    ///  
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting DALMP hourly prices for IESO's area for month {} ...",
            month
        );
        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS da_lmp (
    location_type ENUM('AREA', 'HUB', 'NODE') NOT NULL,
    location_name VARCHAR NOT NULL,
    hour_beginning TIMESTAMPTZ NOT NULL,
    lmp DECIMAL(9,4) NOT NULL,
    mcc DECIMAL(9,4) NOT NULL,
    mcl DECIMAL(9,4) NOT NULL,
);

CREATE TEMPORARY TABLE tmp_z
AS
    SELECT 
        'AREA' AS location_type,
        'ONTARIO' AS location_name,
        hour_beginning, lmp, mcc, mcl
    FROM read_csv('{}/month/area_da_prices_{}.csv.gz', 
    columns = {{
        'hour_beginning': "TIMESTAMPTZ NOT NULL",
        'lmp': "DECIMAL(9,4) NOT NULL",
        'mcc': "DECIMAL(9,4) NOT NULL",
        'mcl': "DECIMAL(9,4) NOT NULL"
        }}
    )
;

INSERT INTO da_lmp BY NAME
(SELECT * FROM tmp_z 
WHERE NOT EXISTS (
    SELECT * FROM da_lmp d
    WHERE d.hour_beginning = tmp_z.hour_beginning
    AND d.location_name = tmp_z.location_name
    )
)
ORDER BY hour_beginning, location_name;
"#,
            self.base_dir,
            month.start_date().strftime("%Y-%m"),
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
}

#[derive(Debug, Deserialize, Serialize)]
struct Document {
    #[serde(rename = "DocHeader")]
    doc_header: DocHeader,
    #[serde(rename = "DocBody")]
    doc_body: DocBody,
}

#[derive(Debug, Deserialize, Serialize)]
struct DocHeader {
    #[serde(rename = "DocTitle")]
    doc_title: String,
    #[serde(rename = "DocRevision")]
    doc_revision: String,
    #[serde(rename = "DocConfidentiality")]
    doc_confidentiality: DocConfidentiality,
    #[serde(rename = "CreatedAt")]
    created_at: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct DocConfidentiality {
    #[serde(rename = "DocConfClass")]
    doc_conf_class: String,
}

#[derive(Debug, Deserialize, Serialize)]
struct DocBody {
    #[serde(rename = "DeliveryDate")]
    delivery_date: String,
    #[serde(rename = "HourlyPriceComponents")]
    hourly_price_components: Vec<HourlyPriceComponents>,
}

#[derive(Debug, Deserialize, Serialize)]
struct HourlyPriceComponents {
    #[serde(rename = "PricingHour")]
    hour: i8,
    #[serde(rename = "ZonalPrice")]
    lmp: String,
    #[serde(rename = "LossPriceCapped")]
    mcl: String,
    #[serde(rename = "CongestionPriceCapped")]
    mcc: String,
    #[serde(rename = "Flag")]
    flag: String,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub begin_hour: Zoned,
    pub lmp: Decimal,
    pub mcc: Decimal,
    pub mcl: Decimal,
}



#[cfg(test)]
mod tests {

    use jiff::civil::date;
    use rust_decimal_macros::dec;
    use std::{error::Error, path::Path};

    use crate::{
        db::prod_db::ProdDb,
        interval::{month::month, term::Term},
    };

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::ieso_dalmp_area();
        let term = "Jun25-Aug25".parse::<Term>()?;
        let months = term.months();
        for month in months {
            // archive.make_gzfile_for_month(&month)?;
            archive.update_duckdb(&month)?;
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn read_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::ieso_dalmp_area();
        let rows = archive.read_file(&date(2025, 6, 3))?;
        println!("Read file, had {} rows", rows.len());
        assert_eq!(rows.len(), 24);
        assert_eq!(rows[8].lmp, dec!(21.21));
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::ieso_dalmp_area();
        let term = "Jul25-Aug25".parse::<Term>().unwrap();
        for day in term.days() {
            archive.download_file(&day)?;
        }
        Ok(())
    }

    #[ignore]
    #[test]
    fn make_monthly_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::ieso_dalmp_area();
        archive.make_gzfile_for_month(&month(2025, 8))?;
        Ok(())
    }
}
