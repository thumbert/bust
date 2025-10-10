use std::{collections::HashMap, env, error::Error, fs, path::Path};

use build_html::Html;
use bust::{
    db::{nrc::generator_status_archive::DailyChangeResult, prod_db::ProdDb},
    utils::send_email::send_email,
};
use clap::Parser;
use duckdb::Connection;
use jiff::Zoned;
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
    let notification_threshold_mw = 5;

    let archive = ProdDb::nrc_generator_status();
    let today = Zoned::now().date();
    let yesterday = today.yesterday().unwrap();
    let year = yesterday.year();
    match archive.download_years(vec![year.into()]) {
        Ok(_) => info!("Downloaded file successfully"),
        Err(e) => error!("{:?}", e),
    }

    match archive.update_duckdb(year.into()) {
        Ok(n) => info!("{} rows were updated", n),
        Err(e) => error!("{}", e),
    }

    // Get the last changes partitioned in groups
    let conn = Connection::open(archive.duckdb_path.clone())?;
    let changes = archive.get_dod_changes(&conn, yesterday)?;
    let mut change_groups: HashMap<String, Vec<DailyChangeResult>> = HashMap::new();
    for change in changes {
        let key = change.0;
        if let Some(v) = change_groups.get_mut(&key) {
            v.push(change.1);
        } else {
            change_groups.insert(key, vec![change.1]);
        }
    }
    // println!("Groups: {:?}", change_groups);
    if change_groups.is_empty() {
        info!("No changes found for the groups of interest!");
        return Ok(());
    }

    // Get email groups
    let email_groups: Vec<EmailGroup> = serde_json::from_reader(fs::File::open(format!(
        "{}/update_nrc_generator_status/emails.json",
        env::var("CONFIG_DIR").unwrap()
    ))?)?;

    // Notify the groups
    for email in email_groups {
        if change_groups.contains_key(email.group_name.as_str()) {
            let data = change_groups.get(&email.group_name).unwrap();
            let filtered_data: Vec<_> = data
                .iter()
                .filter(|e| e.change.abs() >= notification_threshold_mw)
                .cloned()
                .collect();
            if filtered_data.is_empty() {
                info!(
                    "No significant changes found for group: {}",
                    email.group_name
                );
                continue;
            }
            let table = html_table(filtered_data.clone()).to_html_string();
            let html = format!(
                r#"<html>
                <head>
                    <style>
                        table {{
                            border-collapse: collapse;
                        }}
                        th, td {{
                            border: 1px solid black;
                            padding: 8px;
                            text-align: left;
                        }}
                    </style>
                </head>
                <body>
                    <h3>NRC generator status change for group: {}</h3>
                    {}
                </body>
                </html>"#,
                email.group_name, table
            );

            let response = send_email(
                env::var("EMAIL_FROM").unwrap(),
                email.emails,
                format!(
                    "NRC generator status change for group: {}",
                    email.group_name
                ),
                format!(
                    "Changes to generator status:\n{}",
                    ascii_table(filtered_data.clone())
                ),
                Some(html),
            )
            .await?;

            if response.status().is_success() {
                println!("Email sent successfully!");
            } else {
                println!("Failed to send email. Status: {:?}", response.status());

                // Print the response body for additional information
                let body = response.text().await?;
                println!("Response body: {}", body);
            }
        }
    }

    Ok(())
}
