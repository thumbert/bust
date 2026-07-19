use std::{env, error::Error, path::Path, time::Duration};

use build_html::{Html, HtmlContainer, HtmlPage};
use bust::{
    db::{
        isone::{binding_constraints_da::get_new_constraints, lib_dam::is_dalmp_published},
        prod_db::ProdDb,
    },
    interval::month::month,
    utils::{lib_duckdb::open_with_retry, send_email::send_email_blocking},
};
use clap::Parser;
use duckdb::AccessMode;
use jiff::Zoned;
use log::{error, info};

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();
    let archive = ProdDb::isone_da_binding_constraints();

    let tomorrow = Zoned::now().date().tomorrow().unwrap();
    while !is_dalmp_published(tomorrow).unwrap() && Zoned::now().hour() < 22 {
        info!(
            "DA LMP for {} is not published yet. Sleeping for 5 minutes...",
            tomorrow
        );
        std::thread::sleep(std::time::Duration::from_secs(300));
    }
    match archive.download_file(tomorrow) {
        Ok(_) => info!(
            "Downloaded ISONE DA binding constraints file for {} successfully",
            tomorrow
        ),
        Err(e) => error!("{:?}", e),
    }

    let current_month = month(tomorrow.year(), tomorrow.month());
    archive.update_duckdb(&current_month)?;

    // repair the previous month's missing files if tomorrow is the first of the month
    if tomorrow.day() == 1 {
        let prev_month = current_month.previous();
        archive.download_missing_days(prev_month)?;
        archive.update_duckdb(&prev_month)?;
    }

    // Check if there are new constraints and email them
    let conn = open_with_retry(
        &archive.duckdb_path,
        8,
        Duration::from_millis(25),
        AccessMode::ReadOnly,
    )?;
    let new_constraints = get_new_constraints(&conn, &tomorrow)?;
    if new_constraints.is_empty() {
        info!("No new ISONE DA binding constraints for {}", tomorrow);
    } else {
        info!(
            "Found new ISONE DA binding constraints for {}: {:?}",
            tomorrow, new_constraints
        );
        let page = HtmlPage::new()
            .with_paragraph(format!(
                "The following new constraints appeared in the ISONE DAM for {}:",
                tomorrow
            ))
            .with_paragraph(format!("{:?}", new_constraints));
        let html = page.to_html_string();

        let response = send_email_blocking(
            env::var("EMAIL_FROM").unwrap(),
            vec![env::var("EMAIL_WORK").unwrap()],
            "Found new ISONE DA binding constraints!".to_string(),
            "".to_string(),
            Some(html),
        )?;

        if response.status().is_success() {
            info!("Email sent successfully!");
        } else {
            info!("Failed to send email. Status: {:?}", response.status());
            let body = response.text()?;
            info!("Response body: {}", body);
        }
    }

    Ok(())
}
