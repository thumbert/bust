use std::{collections::{HashMap, HashSet}, env, error::Error, fs, path::Path};

use build_html::Html;
use bust::{
    db::{nrc::generator_status_archive::DailyChangeResult, prod_db::ProdDb},
    utils::send_email::send_email,
};
use clap::Parser;
use duckdb::Connection;
use jiff::{civil::Date, Zoned};
use log::{error, info};
use serde::{Deserialize, Serialize};
use tabled::{builder::Builder, settings::Style};

#[derive(Serialize, Deserialize, Debug)]
struct EmailGroup {
    #[serde(rename = "group")]
    group_name: String,
    emails: Vec<String>,
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

/// Make an ASCII table from the data
fn ascii_table(data: Vec<DailyChangeResult>) -> tabled::Table {
    let mut builder = Builder::new();
    builder.push_record(vec![
        "Report Date",
        "Unit",
        "Current Rating",
        "Previous Rating",
        "Change",
    ]);
    for change in data {
        builder.push_record(vec![
            change.report_date.to_string(),
            change.unit_name.clone(),
            change.rating.to_string(),
            change.previous_rating.to_string(),
            change.change.to_string(),
        ]);
    }
    let mut table = builder.build();
    table.with(Style::empty());
    // table.with(Style::sharp());
    table
}

/// Make an HTML table from the data
fn html_table(data: Vec<DailyChangeResult>) -> build_html::Table {
    let mut table = build_html::Table::new();
    table.add_header_row(vec![
        "Report Date",
        "Unit",
        "Previous Rating",
        "Current Rating",
        "Change",
    ]);
    for change in data {
        table.add_body_row(vec![
            change.report_date.to_string(),
            change.unit_name.clone(),
            change.previous_rating.to_string(),
            change.rating.to_string(),
            change.change.to_string(),
        ]);
    }
    table
}

/// Run this job every day at 7AM
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let archive = ProdDb::isone_single_source_contingency();
    let today = Zoned::now().date();
    match archive.download_file(&today) {
        Ok(_) => info!("Downloaded file successfully"),
        Err(e) => error!("{:?}", e),
    }

    let mut days: HashSet<Date> = HashSet::new();
    days.insert(today);
    archive.update_duckdb(&days)?;

    Ok(())
}
