use std::{env, error::Error, path::Path};

use bust::{
    api::isone::{
        _api_isone_core::Market,
        lmp::{get_hourly_prices, Row},
    },
    db::{nyiso::dalmp::LmpComponent, prod_db::ProdDb},
    utils::{lib_duckdb::open_with_retry, send_email::*},
};
use clap::Parser;
use duckdb::AccessMode;
use jiff::{civil::Date, Zoned};
use log::info;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal_macros::dec;

fn make_content(changes: Vec<Row>) -> Result<String, Box<dyn Error>> {
    let tbl = html_table(changes);

    let html = format!(
        r#"
    <html>
        <head>
            <style>
                .col-border {{
                    border-right: 1px solid #d8dee9;
                }}
                table {{
                    border-collapse: collapse;
                }}
                thead tr {{
                    background: #eceff4;       
                }}
                th, td {{
                    padding: 4px;
                    text-align: right;
                }}
            </style>
        </head>
        <body>
            <h3>Negative DA LMP prices:</h3>
            {}
        </body>
    </html>"#,
        tbl,
    );

    Ok(html)
}

fn html_table(data: Vec<Row>) -> String {
    let mut rows_html = String::new();
    let mut last_ptid: Option<u32> = None;
    let mut color_index: u8 = 0;
    let colors = ["#f0f0f0", "#ffffff"];

    for row in &data {
        if last_ptid != Some(row.ptid) {
            last_ptid = Some(row.ptid);
            color_index = 1 - color_index;
        }
        let bg = colors[color_index as usize];
        rows_html.push_str(&format!(
            r#"<tr style="background-color: {}"><td>{}</td><td>{}</td><td>{:.2}</td></tr>"#,
            bg,
            row.ptid,
            row.hour_beginning.strftime("%Y-%m-%d %H:%M:%S %Z"),
            row.price.to_f64().unwrap(),
        ));
    }

    format!(
        "<table><thead><tr><th>Ptid</th><th>Hour Beginning</th><th>Price, $/MWh</th></tr></thead><tbody>{}</tbody></table>",
        rows_html
    )
}

async fn send_email_alert(changes: Vec<Row>) -> Result<(), Box<dyn Error>> {
    let html = make_content(changes)?;
    info!("Generated email content: {:?}", html);

    let for_date = Zoned::now().date().tomorrow()?;
    let response = send_email(
        env::var("EMAIL_FROM").unwrap(),
        vec![env::var("EMAIL_WORK").unwrap()],
        format!("ISONE negative DA LMP prices for {}", for_date),
        "".to_string(),
        Some(html),
    )
    .await?;

    if response.status().is_success() {
        info!("Email sent successfully!");
    } else {
        info!("Failed to send email. Status: {:?}", response.status());
        let body = response.text().await?;
        info!("Response body: {}", body);
    }
    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

fn get_data(as_of: Date) -> Result<Vec<Row>, Box<dyn Error>> {
    let conn = open_with_retry(
        &ProdDb::isone_dalmp().duckdb_path,
        8,
        std::time::Duration::from_millis(25),
        AccessMode::ReadOnly,
    )
    .unwrap();
    let data = get_hourly_prices(
        &conn,
        as_of,
        as_of,
        Market::DA,
        Some(vec![4000, 4001, 4002, 4004, 4005, 4006, 4007, 4008]),
        Some(vec![LmpComponent::Lmp]),
    )
    .unwrap();
    Ok(data)
}

/// Run this job at 2pm, after the DAM is published
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    info!("Checking for negative DA LMP prices at the ISONE load zones for tomorrow...");
    let data = get_data(Zoned::now().date().tomorrow()?)?;
    if data.is_empty() {
        info!("No data found in the DB for tomorrow, exiting...");
        return Ok(());
    }

    // filter negative prices
    let data: Vec<Row> = data
        .into_iter()
        .filter(|row| row.price <= dec!(0.0))
        .collect();
    if !data.is_empty() {
        info!("Found negative prices, sending alert email...");
        send_email_alert(data).await?;
    } else {
        info!("No negative prices found, exiting...");
    }

    Ok(())
}

