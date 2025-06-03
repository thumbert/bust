use duckdb::Connection;
use flate2::read::GzDecoder;
use jiff::tz::{self, TimeZone};
use jiff::{civil::*, Zoned};
use log::{error, info};
use quick_xml::de::from_str;
use std::collections::HashMap;
use std::error::Error;
use std::io::Read;
use std::path::Path;
use std::{collections::HashSet, fs::File};

use crate::db::isone::lib_isoexpress::download_file;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct IesoDaLmpZonalArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl IesoDaLmpZonalArchive {
    /// Return the xml filename for the day
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.to_owned()
            + "/Raw/"
            + &date.year().to_string()
            + "/PUB_DAHourlyZonal_"
            + &date.strftime("%Y%m%d").to_string()
            + ".xml"
    }

    /// Data is published every day after 12PM
    pub fn download_file(&self, date: &Date) -> Result<(), Box<dyn Error>> {
        download_file(
            format!(
                "https://reports-public.ieso.ca/public/DAHourlyZonal/{}",
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

    pub fn read_file(&self, date: &Date) -> Result<Vec<Row>, Box<dyn Error>> {
        let path_gz = self.filename(date) + ".gz";
        let mut file = GzDecoder::new(File::open(path_gz).unwrap());
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let doc: Document = from_str(&buffer)?;
        let mut rows = Vec::new();
        let delivery_date: Date = doc.doc_body.delivery_date.parse()?;
        for transaction_zone in doc.doc_body.hourly_prices.transaction_zones {
            for component in transaction_zone.components {
                let lmp_component = match component.price_component.as_str() {
                    "Zonal Price" => LmpComponent::Lmp,
                    "Energy Loss Price" => LmpComponent::Mcl,
                    "Energy Congestion Price" => LmpComponent::Mcc,
                    _ => panic!("Unknown component: {}", component.price_component),
                };
                for delivery_hour in component.delivery_hours {
                    let begin_hour = delivery_date
                        .at(delivery_hour.hour - 1, 0, 0, 0)
                        .to_zoned(TimeZone::fixed(tz::offset(-5)))?;
                    rows.push(Row2 {
                        location_name: transaction_zone.zone_name.clone(),
                        component: lmp_component.clone(),
                        begin_hour,
                        price: delivery_hour.lmp,
                    });
                }
            }
        }

        Ok(transpose(rows))
    }

    /// Upload each individual day to DuckDB.
    /// Assumes a json.gz file exists.  Skips the day if it doesn't exist.
    /// This method only works well for a few day.  For a lot of days, don't loop over days.
    /// Consider using DuckDB directly by globbing the file names.
    ///  
    pub fn update_duckdb(&self, days: &HashSet<Date>) -> Result<(), Box<dyn Error>> {
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
CREATE TABLE IF NOT EXISTS ssc (
        BeginDate TIMESTAMPTZ NOT NULL,
        RtFlowMw DOUBLE NOT NULL,
        LowestLimitMw DOUBLE NOT NULL,
        DistributionFactor DOUBLE NOT NULL,
        InterfaceName VARCHAR NOT NULL,
        ActualMarginMw DOUBLE NOT NULL,
        AuthorizedMarginMw DOUBLE NOT NULL,
        BaseLimitMw DOUBLE NOT NULL,
        SingleSourceContingencyLimitMw DOUBLE NOT NULL,
);",
        )?;
        conn.execute_batch(
            r"
CREATE TEMPORARY TABLE tmp (
        BeginDate TIMESTAMPTZ NOT NULL,
        RtFlowMw DOUBLE NOT NULL,
        LowestLimitMw DOUBLE NOT NULL,
        DistributionFactor DOUBLE NOT NULL,
        InterfaceName VARCHAR NOT NULL,
        ActMarginMw DOUBLE NOT NULL,
        AuthorizedMarginMw DOUBLE NOT NULL,
        BaseLimitMw DOUBLE NOT NULL,
        SingleSrcContingencyMw DOUBLE NOT NULL,
);",
        )?;

        for day in days {
            let path = self.filename(day) + ".gz";
            if !Path::new(&path).exists() {
                info!("No file for {}.  Skipping", day);
                continue;
            }

            // insert into duckdb
            conn.execute_batch(&format!(
                "
INSERT INTO tmp
    SELECT unnest(SingleSrcContingencyLimits.SingleSrcContingencyLimit, recursive := true)
    FROM read_json('~/Downloads/Archive/IsoExpress/SingleSourceContingency/Raw/{}/ssc_{}.json.gz')
;",
                day.year(),
                day
            ))?;

            let query = r"
INSERT INTO ssc
    SELECT 
        BeginDate::TIMESTAMPTZ,
        RtFlowMw::DOUBLE,
        LowestLimitMw::DOUBLE,
        DistributionFactor::DOUBLE,
        InterfaceName::VARCHAR,
        ActMarginMw::DOUBLE as ActualMarginMw,
        AuthorizedMarginMw::DOUBLE,
        BaseLimitMw::DOUBLE,
        SingleSrcContingencyMw::DOUBLE as SingleSourceContingencyLimitMw,
    FROM tmp
EXCEPT 
    SELECT * FROM ssc
;";
            match conn.execute(query, []) {
                Ok(updated) => info!("{} rows were updated for day {}", updated, day),
                Err(e) => error!("{}", e),
            }
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
    #[serde(rename = "HourlyPrices")]
    hourly_prices: HourlyPrices,
}

#[derive(Debug, Deserialize, Serialize)]
struct HourlyPrices {
    #[serde(rename = "TransactionZone")]
    transaction_zones: Vec<TransactionZone>,
}

#[derive(Debug, Deserialize, Serialize)]
struct TransactionZone {
    #[serde(rename = "ZoneName")]
    zone_name: String,
    #[serde(rename = "Components")]
    components: Vec<Components>,
}

#[derive(Debug, Deserialize, Serialize)]
struct Components {
    #[serde(rename = "PriceComponent")]
    price_component: String,
    #[serde(rename = "DeliveryHour")]
    delivery_hours: Vec<DeliveryHour>,
}

#[derive(Debug, Deserialize, Serialize)]
struct DeliveryHour {
    #[serde(rename = "Hour")]
    hour: i8,
    #[serde(rename = "LMP")]
    lmp: f64,
    #[serde(rename = "FLAG")]
    flag: u8,
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    pub location_name: String,
    pub begin_hour: Zoned,
    pub lmp: f64,
    pub mcc: f64,
    pub mcl: f64,
}

#[derive(Debug, Deserialize, Serialize, Clone, PartialEq)]
pub enum LmpComponent {
    Lmp,
    Mcl,
    Mcc,
}

#[derive(Debug, Deserialize, Serialize)]
struct Row2 {
    location_name: String,
    component: LmpComponent,
    begin_hour: Zoned,
    price: f64,
}

impl std::fmt::Display for Row2 {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "Location: {}, Component: {:?}, Begin Hour: {}, Price: {}",
            self.location_name, self.component, self.begin_hour, self.price
        )
    }
}

/// Transpose the data to have the tree prices in one row.
fn transpose(data: Vec<Row2>) -> Vec<Row> {
    let mut groups: HashMap<(String, Zoned), Vec<Row2>> = HashMap::new();
    for e in data {
        let key = (e.location_name.clone(), e.begin_hour.clone());
        groups.entry(key).or_default().push(e);
    }

    let mut rows: Vec<Row> = Vec::new();
    for (k, v) in &groups {
        let location_name = k.0.clone();
        let begin_hour = k.1.clone();
        let mut lmp = 0.0;
        let mut mcc = 0.0;
        let mut mcl = 0.0;

        for row in v {
            match row.component {
                LmpComponent::Lmp => lmp = row.price,
                LmpComponent::Mcc => mcc = row.price,
                LmpComponent::Mcl => mcl = row.price,
            }
        }
        rows.push(Row {
            location_name,
            begin_hour,
            lmp,
            mcc,
            mcl,
        });
    }
    rows.sort_unstable_by_key(|e| (e.location_name.clone(), e.begin_hour.clone()));

    rows
}

#[cfg(test)]
mod tests {

    use jiff::{civil::date, ToSpan, Zoned};
    use std::{error::Error, path::Path};

    use crate::db::prod_db::ProdDb;

    use super::*;

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        let archive = ProdDb::isone_single_source_contingency();
        // let days = vec![date(2024, 12, 4), date(2024, 12, 5), date(2024, 12, 6)];
        // let days: Vec<Date> = date(2024, 1, 1).series(1.day()).take(366).collect();
        // let days: HashSet<Date> = date(2024, 4, 1)
        //     .series(1.day())
        //     .take_while(|e| e <= &date(2024, 12, 31))
        //     .collect();
        let today = Zoned::now().date();
        let days: HashSet<Date> = date(2025, 4, 29)
            .series(1.day())
            .take_while(|e| e <= &today)
            .collect();
        for day in &days {
            println!("Processing {}", day);
            archive.download_file(day)?;
        }
        archive.update_duckdb(&days)?;
        Ok(())
    }

    #[ignore]
    #[test]
    fn read_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::ieso_dalmp_zonal();
        let rows = archive.read_file(&date(2025, 5, 5))?;

        let toronto: Vec<&Row> = rows
            .iter()
            .filter(|row| row.location_name == "TORONTO")
            .collect();
        assert_eq!(toronto.len(), 24);
        assert_eq!(toronto[8].lmp, 26.11);

        Ok(())
    }

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::ieso_dalmp_zonal();
        archive.download_file(&date(2025, 5, 5))?;
        Ok(())
    }
}
