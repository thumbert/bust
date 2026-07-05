use std::{env, error::Error};

use build_html::Html;
use bust::{
    db::{nyiso::ptid_table::*, prod_db::ProdDb},
    utils::send_email::send_email,
};
use clap::Parser;
use duckdb::{AccessMode, Config, Connection};
use jiff::{ToSpan, Zoned};
use log::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    #[arg(short, long, default_value = "prod")]
    env: String,
}

fn make_content(rows: Vec<Record>) -> Result<String, Box<dyn Error>> {
    let mut table = build_html::Table::new();
    table.add_header_row(vec!["Ptid", "Name", "Zone"]);

    for record in rows {
        table.add_body_row(vec![
            record.ptid.to_string(),
            record.name.clone(),
            record.zone.clone(),
        ]);
    }

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
            <h3>Peak load forecast changes:</h3>
            {}
        </body>
    </html>"#,
        table.to_html_string(),
    );

    Ok(html)
}

async fn send_email_alert(changes: Vec<Record>) -> Result<(), Box<dyn Error>> {
    let html = make_content(changes)?;
    let response = send_email(
        env::var("EMAIL_FROM").unwrap(),
        vec![env::var("EMAIL_WORK").unwrap()],
        "Found new NYISO nodes!".to_string(),
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

// Run every month on the 1st of the month
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    let asof = Zoned::now().date();
    let archive = ProdDb::nyiso_ptid_table();
    tokio::task::block_in_place(|| -> Result<(), Box<dyn Error>> {
        archive.download_file()?;
        archive.update_duckdb(asof)?;
        Ok(())
    })?;

    // check if there are new nodes
    let day1 = asof.first_of_month().checked_sub(1.month())?;
    info!("Checking for new nodes between {} and {}", day1, asof);
    let config = Config::default().access_mode(AccessMode::ReadOnly)?;
    let conn = Connection::open_with_flags(ProdDb::nyiso_ptid_table().duckdb_path, config).unwrap();
    let rows = get_new_nodes(&conn, day1, asof)?;

    // email new nodes
    match rows.as_slice() {
        [] => {
            info!("No new nodes found between {} and {}", day1, asof);
        }
        _ => {
            send_email_alert(rows).await?;
        }
    }

    Ok(())
}
