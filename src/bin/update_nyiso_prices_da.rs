use std::{env, error::Error, path::Path, time::Duration};

use build_html::{Html, HtmlContainer, HtmlPage};
use bust::{
    db::{nyiso::dalmp::NodeType, prod_db::ProdDb},
    interval::month::{Month, month},
    utils::{lib_duckdb::open_with_retry, send_email::send_email},
};
use clap::Parser;
use jiff::Zoned;
use log::info;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

async fn send_email_alert(ptids: Vec<i32>, asof: jiff::civil::Date) -> Result<(), Box<dyn Error>> {
    let page = HtmlPage::new()
        .with_paragraph(format!("The following new nodes were found in the NYISO DA LMP file for {}:", asof))
        .with_paragraph(format!("{:?}", ptids));
    let html = page.to_html_string();

    let response = send_email(
        env::var("EMAIL_FROM").unwrap(),
        vec![env::var("EMAIL_WORK").unwrap()],
        "Found new NYISO nodes in the file with DA LMPs!".to_string(),
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

/// Run this job every day at 10:30AM
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let mut asof = Zoned::now().date();
    if Zoned::now().hour() >= 10 {
        asof = asof.tomorrow().unwrap();
    }
    info!("Updating NYISO DALMP for asof date: {}", asof);

    let current_month = month(asof.year(), asof.month());
    let mut months: Vec<Month> = Vec::new();
    if asof.day() < 4 {
        months.push(current_month.previous());
    }
    months.push(current_month);
    info!("Updating NYISO DALMP for months: {:?}", months);

    let archive = ProdDb::nyiso_dalmp();

    tokio::task::block_in_place(|| -> Result<(), Box<dyn Error>> {
        for month in months {
            archive.download_file(month, NodeType::Gen)?;
            archive.download_file(month, NodeType::Zone)?;
            archive.update_duckdb(month)?;
        }
        Ok(())
    })?;

    // if there are new nodes in the file, send an email
    let conn = open_with_retry(
        &archive.duckdb_path,
        8,
        Duration::from_millis(25),
        duckdb::AccessMode::ReadOnly,
    )?;
    let new_ptids = tokio::task::block_in_place(|| archive.get_nodes_starting(&conn, asof))?;

    if new_ptids.is_empty() {
        info!("No new nodes found.");
    } else {
        info!("New nodes found: {:?}", new_ptids);
        send_email_alert(new_ptids, asof).await?;
    }

    Ok(())
}
