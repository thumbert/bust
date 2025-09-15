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
pub struct IesoVGForecastSummaryArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IesoVGForecastSummaryArchive {


    /// Return the xml filename for the day
    pub fn filename(&self, day: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &day.year().to_string()
            + "/PUB_VGForecastSummary_"
            + &day.strftime("%Y%m%d").to_string()
            + ".xml"
    }

    /// Data is published every day at 06:51, don't know timezone.
    pub fn download_file(&self, day: &Date) -> Result<(), Box<dyn Error>> {
        download_file(
            format!(
                "https://reports-public.ieso.ca/public/VGForecastSummary/{}",
                Path::new(&self.filename(day))
                    .file_name()
                    .and_then(|name| name.to_str())
                    .unwrap_or("")
            ),
            false,
            None,
            Path::new(&self.filename(day)),
            true,
        )
    }

    pub fn read_file(&self, day: &Date) -> Result<Vec<Row>, Box<dyn Error>> {
        let path_gz = self.filename(day) + ".gz";
        let mut file = GzDecoder::new(File::open(path_gz).unwrap());
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let doc: Document = from_str(&buffer)?;
        let mut rows: Vec<Row> = Vec::new();
        let forecast_timestamp: Zoned = format!("{}-05:00[-05:00]", doc.doc_body.forecast_timestamp)
            .parse::<Zoned>()
            .unwrap();
        for od in doc.doc_body.organization_data {
            let organization = od.organization_type;
            for fd in od.fuel_data {
                let fuel = fd.fuel_type;
                for rd in fd.resource_data {
                    let zone = rd.zone_name;
                    for ef in rd.energy_forecast {
                        let forecast_date: Date = ef.forecast_date.parse()?;
                        for hd in ef.forecast_interval {
                            let delivery_date: Date = forecast_date;
                            rows.push(Row {
                                forecast_timestamp: forecast_timestamp.clone(),
                                organization: organization.clone(),
                                fuel_type: fuel.clone(),
                                zone: zone.clone(),
                                hour_beginning: delivery_date
                                    .at(hd.forecast_hour - 1, 0, 0, 0)
                                    .to_zoned(TimeZone::fixed(tz::offset(-5)))?,
                                mw: Decimal::from_str(&hd.mw_output)?,
                            });
                        }
                    }


                }
            }
        }

        Ok(rows)
    }

    /// Aggregate all the daily files into one monthly file for convenience.
    /// Be strict about missing days and error out.
    /// File is ready to be uploaded into database.
    ///
    pub fn make_gzfile_for_month(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        let file_out = format!(
            "{}/month/PUB_VGForecastSummary_{}.csv",
            self.base_dir.to_owned(),
            month
        );
        let mut wtr = csv::Writer::from_path(&file_out)?;
        wtr.write_record([
            "forecast_timestamp",
            "organization",
            "fuel_type",
            "zone",
            "hour_beginning",
            "mw",
        ])?;

        let last = Zoned::now().date().yesterday()?;
        for day in month.days() {
            if day > last {
                continue;
            }
            if day < date(2025, 8, 8) {
                continue;
            }
            let rows = self.read_file(&day)?;
            for row in rows {
                let _ = wtr.write_record(&[
                    row.forecast_timestamp
                        .strftime("%Y-%m-%dT%H:%M:%S.000%:z")
                        .to_string(),
                    row.organization,
                    row.fuel_type,
                    row.zone,
                    row.hour_beginning
                        .strftime("%Y-%m-%dT%H:%M:%S.000%:z")
                        .to_string(),
                    row.mw.to_string(),
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

    /// Upload each one year to DuckDB.
    /// Assumes the corresponding json.gz file exists.  
    ///
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting hourly forecasts for variable generation {} ...",
            month
        );
        let sql = format!(
            r#"
SET TimeZone = 'America/Cancun';
CREATE TABLE IF NOT EXISTS forecast_summary (
    forecast_timestamp TIMESTAMPTZ NOT NULL,
    organization ENUM('MARKET PARTICIPANT', 'EMBEDDED') NOT NULL,
    fuel_type ENUM('Wind', 'Solar') NOT NULL,
    zone VARCHAR NOT NULL,
    hour_beginning TIMESTAMPTZ NOT NULL,    
    mw DECIMAL(9,4) NOT NULL,
);

CREATE TEMPORARY TABLE tmp
AS
    SELECT 
        forecast_timestamp, organization, fuel_type, zone, hour_beginning, mw
    FROM read_csv('{}/month/PUB_VGForecastSummary_{}.csv.gz', 
    columns = {{
        'forecast_timestamp': "TIMESTAMPTZ NOT NULL",
        'organization': "ENUM('MARKET PARTICIPANT', 'EMBEDDED') NOT NULL",
        'fuel_type': "ENUM('Wind', 'Solar') NOT NULL",
        'zone': "VARCHAR NOT NULL",
        'hour_beginning': "TIMESTAMPTZ NOT NULL",
        'mw': "DECIMAL(9,4) NOT NULL"
        }}
    )
;

INSERT INTO forecast_summary BY NAME
(SELECT * FROM tmp 
WHERE NOT EXISTS (
    SELECT * FROM forecast_summary f
    WHERE f.forecast_timestamp = tmp.forecast_timestamp
    AND f.organization = tmp.organization
    AND f.zone = tmp.zone
    AND f.fuel_type = tmp.fuel_type
    AND f.hour_beginning = tmp.hour_beginning
    )
)
ORDER BY forecast_timestamp, organization, fuel_type, zone, hour_beginning;
"#,
            self.base_dir,
            month.strftime("%Y-%m"),
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
    #[serde(rename = "ForecastTimeStamp")]
    forecast_timestamp: String,
    #[serde(rename = "OrganizationData")]
    organization_data: Vec<OrganizationData>,
}

#[derive(Debug, Deserialize, Serialize)]
struct OrganizationData {
    #[serde(rename = "OrganizationType")]
    organization_type: String,
    #[serde(rename = "FuelData")]
    fuel_data: Vec<FuelData>,
}

#[derive(Debug, Deserialize, Serialize)]
struct FuelData {
    #[serde(rename = "FuelType")]
    fuel_type: String,
    #[serde(rename = "ResourceData")]
    resource_data: Vec<ResourceData>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ResourceData {
    #[serde(rename = "ZoneName")]
    zone_name: String,
    #[serde(rename = "EnergyForecast")]
    energy_forecast: Vec<EnergyForecast>,
}

#[derive(Debug, Deserialize, Serialize)]
struct EnergyForecast {
    #[serde(rename = "ForecastDate")]
    forecast_date: String,
    #[serde(rename = "ForecastInterval")]
    forecast_interval: Vec<ForecastInterval>,
}

#[derive(Debug, Deserialize, Serialize)]
struct ForecastInterval {
    #[serde(rename = "ForecastHour")]
    forecast_hour: i8,
    #[serde(rename = "MWOutput")]
    mw_output: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
pub struct Row {
    pub forecast_timestamp: Zoned,
    pub organization: String,
    pub fuel_type: String,
    pub zone: String,
    pub hour_beginning: Zoned,
    pub mw: Decimal,
}

#[cfg(test)]
mod tests {
    use std::{error::Error, path::Path};

    use rust_decimal_macros::dec;

    use crate::{db::prod_db::ProdDb, interval::term::Term};

    use super::*;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::ieso_vgforecast_summary();
        let term = "8Aug25-8Sep25".parse::<Term>().unwrap();
        // for day in term.days() {
        //     archive.download_file(&day)?;
        // }
        for month in term.months() {
            archive.make_gzfile_for_month(&month)?;
            archive.update_duckdb(&month)?;
        }

        Ok(())
    }

    #[ignore]
    #[test]
    fn read_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::ieso_vgforecast_summary();
        let rows = archive.read_file(&date(2025, 9, 8))?;
        assert_eq!(
            rows[0],
            Row {
                forecast_timestamp: "2025-09-08T05:33:09-05:00[-05:00]".parse()?,
                organization: "MARKET PARTICIPANT".into(),
                fuel_type: "Solar".into(),
                zone: "NORTHEAST".into(),
                hour_beginning: "2025-09-08T00:00:00-05:00[-05:00]".parse()?,
                mw: dec!(29.9),
            }
        );
        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::ieso_vgforecast_summary();
        let term = "8Sep25".parse::<Term>().unwrap();
        for day in term.days() {
            archive.download_file(&day)?;
        }
        Ok(())
    }
}
