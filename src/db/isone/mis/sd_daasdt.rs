use std::{error::Error, fs::OpenOptions, str::FromStr};

use jiff::{civil::Date, Timestamp, Zoned};
use serde::{Deserialize, Serialize};

use super::lib_mis::*;

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
enum AssetType {
    generator,
    asset_related_demand,
    demand_reponse_resource,
}

impl FromStr for AssetType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "GENERATOR" => Ok(AssetType::generator),
            "ASSET RELATED DEMAND" => Ok(AssetType::asset_related_demand),
            "DEMAND RESPONSE_RESOURCE" => Ok(AssetType::demand_reponse_resource),
            _ => Err(format!("Failed to parse {s} as AssetType")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Clone, Copy)]
enum ProductType {
    da_tmsr,
    da_tmnsr,
    da_tmor,
    da_eir,
}

impl FromStr for ProductType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "DA_TMSR" => Ok(ProductType::da_tmsr),
            "DA_TMNSR" => Ok(ProductType::da_tmnsr),
            "DA_TMOR" => Ok(ProductType::da_tmor),
            "DA_EIR" => Ok(ProductType::da_eir),
            _ => Err(format!("Failed to parse {s} as ProductType")),
        }
    }
}

#[derive(Debug, Serialize, Deserialize)]
struct RowTab0 {
    account_id: usize,
    report_date: Date,
    version: Timestamp,
    hour_beginning: Zoned,
    asset_id: u32,
    asset_name: String,
    subaccount_id: u32,
    subaccount_name: String,
    asset_type: AssetType,
    ownership_share: f32,
    product_type: Option<ProductType>,
    product_obligation: Option<f64>,
    product_clearing_price: Option<f64>,
    product_credit: Option<f64>,
    customer_share_of_product_credit: Option<f64>,
    strike_price: Option<f64>,
    hub_rt_lmp: Option<f64>,
    product_closeout_charge: Option<f64>,
    customer_share_of_product_closeout_charge: Option<f64>,
}

pub struct SdDaasdtReport {
    info: MisReportInfo,
    lines: Vec<String>,
}

impl MisReport for SdDaasdtReport {}

impl SdDaasdtReport {
    fn process_tab0(&self) -> Result<Vec<RowTab0>, Box<dyn Error>> {
        let mut out: Vec<RowTab0> = Vec::new();
        let tab0 = extract_tab(0, &self.lines).unwrap();
        let data = tab0.lines.join("\n");
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .from_reader(data.as_bytes());
        for result in rdr.records() {
            let record = result?;

            let hour_beginning = parse_hour_ending(&self.info.report_date, &record[1]);
            let asset_id: u32 = record[2].parse()?;
            let asset_name: String = record[3].to_owned();
            let subaccount_id: u32 = record[4].parse()?;
            let subaccount_name: String = record[5].to_owned();
            let asset_type: AssetType = record[6].parse()?;
            let ownership_share: f32 = record[7].parse()?;
            let product_type: Option<ProductType> = record[8].parse().ok();
            let product_obligation: Option<f64> = record[9].parse().ok();
            let product_clearing_price: Option<f64> = record[10].parse().ok();
            let product_credit: Option<f64> = record[11].parse().ok();
            let customer_share_of_product_credit: Option<f64> = record[12].parse().ok();
            let strike_price: Option<f64> = record[13].parse().ok();
            let hub_rt_lmp: Option<f64> = record[14].parse().ok();
            let product_closeout_charge: Option<f64> = record[15].parse().ok();
            let customer_share_of_product_closeout_charge: Option<f64> = record[16].parse().ok();

            out.push(RowTab0 {
                account_id: self.info.account_id,
                report_date: self.info.report_date,
                version: self.info.version,
                hour_beginning,
                asset_id,
                asset_name,
                subaccount_id,
                subaccount_name,
                asset_type,
                ownership_share,
                product_type,
                product_obligation,
                product_clearing_price,
                product_credit,
                customer_share_of_product_credit,
                strike_price,
                hub_rt_lmp,
                product_closeout_charge,
                customer_share_of_product_closeout_charge,
            });
        }

        Ok(out)
    }

    fn export_csv<K>(&self, archive: SdDaasdtArchive) -> Result<(), Box<dyn Error>> {
        // tab 0
        let file = OpenOptions::new()
            .create(true)
            .append(true)
            .open(archive.filename(self.info.report_date, 0))
            .unwrap();
        let mut wtr = csv::Writer::from_writer(file);
        let records = self.process_tab0().unwrap();
        for record in records {
            wtr.serialize(record)?;
        }
        wtr.flush()?;

        Ok(())
    }
}

pub struct SdDaasdtArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SdDaasdtArchive {
    /// Path to the monthly CSV file with the ISO report for a given tab
    pub fn filename(&self, date: Date, tab: u8) -> String {
        self.base_dir.to_owned()
            + "/Raw"
            + "/sd_daasdt_"
            + &format!("tab{}_", tab)
            + &date.strftime("%Y-%m").to_string()
            + ".csv"
    }
}

#[cfg(test)]
mod tests {
    use std::{error::Error, str::FromStr};

    use jiff::{civil::Date, Timestamp, Zoned};

    use crate::db::isone::mis::{
        lib_mis::*,
        sd_daasdt::{AssetType, SdDaasdtReport},
    };

    #[test]
    fn read_tab0_test() -> Result<(), Box<dyn Error>> {
        let path = "../elec-server/test/_assets/sd_daasdt_000000002_2024111500_20241203135151.csv";
        let info = MisReportInfo::from(path.to_string());
        let lines = read_report(path).unwrap();
        assert_eq!(lines.len(), 198);

        let report = SdDaasdtReport { info, lines };
        let rows = report.process_tab0()?;
        println!("{:?}", rows);

        Ok(())
    }

    #[test]
    fn parse_enums_test() -> Result<(), Box<dyn Error>> {
        assert_eq!(
            AssetType::from_str("GENERATOR").unwrap(),
            AssetType::generator
        );
        Ok(())
    }
}
