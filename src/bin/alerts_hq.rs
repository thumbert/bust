use std::{env, error::Error, path::Path};

use bust::{
    db::prod_db::ProdDb,
    utils::send_email::*,
};
use clap::Parser;
use duckdb::{types::ValueRef, Connection};
use log::info;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;

fn make_content(threshold: Decimal) -> Result<Option<String>, Box<dyn Error>> {
    let archive = ProdDb::hq_total_demand_prelim();
    let conn = Connection::open(&archive.duckdb_path).unwrap();
    conn.execute("LOAD ICU;", [])?;    
    let query = r#"
SELECT MAX(value) as max_demand
FROM total_demand_prelim
WHERE zoned >= CURRENT_TIMESTAMP::TIMESTAMPTZ - INTERVAL '1 days';
    "#;
    let mut stmt = conn.prepare(query).unwrap();
    let mw_iter = stmt.query_map([], |row| {
        let mw = match row.get_ref_unwrap(0) {
            ValueRef::Decimal(v) => v,
            _ => Decimal::MIN,
        };
        Ok(mw)
    })?;
    let binding = mw_iter
        .map(|e| e.unwrap())
        .collect::<Vec<Decimal>>();
    let mw = binding
        .first()
        .unwrap();
    if mw < &threshold {
        info!(
            "Max demand in last 24 hours is {}, less than {} MW, no alert needed",
            mw.to_string(),
            threshold.to_string()
        );
        return Ok(None);
    }

    let html = format!(
        r#"
    <html>
        <body>
            <h3>HQ total demand has reached {} MW in the past 24 hours.</h3>
        </body>
    </html>"#,
        mw,
    );

    Ok(Some(html))
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

/// Run this job every day at 17:01
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    info!("Running HQ total demand alert check...");
    let html = make_content(dec!(35000))?;
    if html.is_none() {
        return Ok(());
    }

    let response = send_email(
        env::var("EMAIL_FROM").unwrap(),
        vec![env::var("EMAIL_MAIN").unwrap()],
        "HQ high electricity demand alert!".to_string(),
        "".to_string(),
        Some(html.unwrap()),
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

