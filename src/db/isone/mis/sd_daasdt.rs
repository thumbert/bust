use std::{collections::HashSet, error::Error, fs::{self, OpenOptions}, str::FromStr};

use duckdb::{params, Connection};
use jiff::{civil::Date, Timestamp, ToSpan, Zoned};
use log::{error, info};
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

    // fn export_csv<K>(&self, archive: SdDaasdtArchive) -> Result<(), Box<dyn Error>> {
    //     // tab 0
    //     let file = OpenOptions::new()
    //         .create(true)
    //         .append(true)
    //         .open(archive.filename(self.info.report_date, 0))
    //         .unwrap();
    //     let mut wtr = csv::Writer::from_writer(file);
    //     let records = self.process_tab0().unwrap();
    //     for record in records {
    //         wtr.serialize(record)?;
    //     }
    //     wtr.flush()?;

    //     Ok(())
    // }
}

pub struct SdDaasdtArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl MisArchiveDuckDB for SdDaasdtArchive {
    /// Path to the monthly CSV file with the ISO report for a given tab
    fn filename(&self, tab: u8, info: &MisReportInfo) -> String {
        self.base_dir.to_owned()
            + "/Raw"
            + "/sd_daasdt_"
            + &format!("tab{}_", tab)
            // + &date.strftime("%Y-%m").to_string()
            + ".csv"
    }

    fn get_reports_duckdb(&self) -> Result<HashSet<MisReportInfo>, Box<dyn Error>> {
        let conn = Connection::open(&self.duckdb_path)?;
        let query = r#"
        SELECT DISTINCT account_id, report_date, version
        FROM tab0;
        "#;
        let mut stmt = conn.prepare(query).unwrap();
        let res_iter = stmt.query_map([], |row| {
            let n = 719528 + row.get::<usize, i32>(1).unwrap();
            let microseconds: i64 = row.get(2).unwrap();
            Ok(MisReportInfo {
                report_name: "SD_RTLOAD".to_string(),
                account_id: row.get::<usize, usize>(0).unwrap(),
                report_date: Date::ZERO.checked_add(n.days()).unwrap(),
                version: Timestamp::from_microsecond(microseconds).unwrap(),
            })
        })?;
        let res: HashSet<MisReportInfo> = res_iter.map(|e| e.unwrap()).collect();

        Ok(res)
    }

    fn setup_duckdb(&self) -> Result<(), Box<dyn Error>> {
        info!("initializing SD_RTLOAD archive ...");
        if fs::exists(&self.duckdb_path)? {
            fs::remove_file(&self.duckdb_path)?;
        }
        let conn = Connection::open(self.duckdb_path.clone())?;
        conn.execute_batch(
            r"
    BEGIN;
    CREATE TABLE IF NOT EXISTS tab0 (
        account_id UINTEGER NOT NULL,
        report_date DATE NOT NULL,
        version TIMESTAMP NOT NULL,
        hour_beginning TIMESTAMPTZ NOT NULL,
        asset_name VARCHAR NOT NULL,
        asset_id UINTEGER NOT NULL,
        asset_subtype ENUM ('LOSSES', 'NORMAL', 'STATION SERVICE', 'ENERGY STORAGE', 'PUMP STORAGE'),
        location_id UINTEGER NOT NULL,
        location_name VARCHAR NOT NULL,
        location_type ENUM ('METERING DOMAIN', 'NETWORK NODE'),
        load_reading DOUBLE NOT NULL,
        ownership_share FLOAT NOT NULL,
        share_of_load_reading DOUBLE NOT NULL,
        subaccount_id UINTEGER,
        subaccount_name VARCHAR,
    );
    CREATE INDEX idx ON tab0 (report_date);
    COMMIT;
    ",
        )?;

        conn.close().unwrap();
        Ok(())
    }

    fn update_duckdb(&self, files: Vec<String>) -> Result<(), Box<dyn Error>> {
        // get all reports in the db first
        let existing = self.get_reports_duckdb().unwrap();
        fs::remove_dir_all(format!("{}/tmp", &self.base_dir))?;
        fs::create_dir_all(format!("{}/tmp", &self.base_dir))?;

        for filename in files.iter() {
            let info = &MisReportInfo::from(filename.clone());
            if existing.contains(info) {
                continue;
            }
            let lines = read_report(filename.as_str()).unwrap();
            let report = SdDaasdtReport {
                info: info.clone(),
                lines,
            };
            // report.export_csv(self)?;
            info!("Wrote file {}", self.filename(0, info));
        }

        // list all the files and add them to the db, in order
        let mut paths: Vec<_> = fs::read_dir(self.base_dir.clone() + "/tmp")
            .unwrap()
            .map(|e| e.unwrap())
            .collect();
        paths.sort_by_key(|e| e.path());

        if paths.is_empty() {
            info!("No files to upload to DuckDB.  Exiting...");
            return Ok(());
        } else {
            info!("Inserting {} files into DuckDB.", paths.len());
        }

        let conn = Connection::open(&self.duckdb_path)?;
        let sql = format!(
            r"
            INSERT INTO tab0 
            SELECT account_id, report_date, version, 
                strptime(left(hour_beginning, 25), '%Y-%m-%dT%H:%M:%S%z') AS hour_beginning,
                asset_name,
                asset_id,
                asset_subtype,
                location_id,
                location_name,
                location_type,
                load_reading,
                ownership_share,
                share_of_load_reading,
                subaccount_id,
                subaccount_name
            FROM read_csv(
                '{}/tmp/tab0_*.CSV', 
                header = true, 
                timestampformat = '%Y-%m-%dT%H:%M:%SZ'
            );
            ",
            self.base_dir,
        );
        match conn.execute(&sql, params![]) {
            Ok(n) => info!("  inserted {} rows in SD_RTLOAD tab0 table", n),
            Err(e) => error!("{:?}", e),
        }

        info!("done\n");

        Ok(())
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
