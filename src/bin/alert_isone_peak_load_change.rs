use std::{env, error::Error, fs::remove_file, path::Path, time::SystemTime};

use build_html::Html;
use bust::utils::send_email::*;
use clap::Parser;
use jiff::{Timestamp, Zoned};
use log::info;
use num_format::{Locale, ToFormattedString};
use regex::Regex;
use rust_decimal::prelude::ToPrimitive;
use rust_decimal::Decimal;
use rust_decimal_macros::dec;
use serde::{Deserialize, Serialize};
use tokio::fs::metadata;

#[derive(Debug, Serialize, Deserialize)]
struct PeakLoad {
    report_timestamp: Zoned,
    snapshot_as_of: Zoned,
    mw: Decimal,
}

#[derive(Debug, Serialize, Deserialize)]
struct PeakLoadWithChange {
    report_timestamp: Zoned,
    snapshot_as_of: Zoned,
    mw: Decimal,
    change: Option<Decimal>,
}

fn make_content(changes: Vec<PeakLoadWithChange>) -> Result<String, Box<dyn Error>> {
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
            <h3>Peak load forecast changes:</h3>
            {}
        </body>
    </html>"#,
        tbl.to_html_string(),
    );

    Ok(html)
}

fn check_change() -> Result<Vec<PeakLoadWithChange>, Box<dyn Error>> {
    // read file for the day
    let file_path = get_file_path();
    if !file_path.exists() {
        return Ok(Vec::new());
    }
    let mut rdr = csv::Reader::from_reader(std::fs::File::open(file_path)?);
    let mut values = Vec::new();
    for result in rdr.deserialize() {
        let record: PeakLoad = result?;
        values.push(record);
    }

    // calculate changes, if any
    let mut changes: Vec<PeakLoadWithChange> = Vec::new();
    for (i, value) in values.iter().enumerate() {
        let change = if i == 0 {
            None
        } else {
            Some(value.mw - values[i - 1].mw)
        };
        if change == Some(dec!(0)) {
            continue;
        }
        changes.push(PeakLoadWithChange {
            report_timestamp: value.report_timestamp.clone(),
            snapshot_as_of: value.snapshot_as_of.clone(),
            mw: value.mw,
            change,
        });
    }

    Ok(changes)
}

fn get_file_path() -> std::path::PathBuf {
    let str = format!(
        "{}{}",
        env::var("CONFIG_DIR").unwrap(),
        "/alert_isone_peak_load_change/values.csv"
    );
    std::path::PathBuf::from(str)
}

/// Look if the ISO has updated the report online today.  
fn has_published_report_today(html: &str) -> bool {
    let line = html
        .lines()
        .find(|line| line.contains("Report Generated"))
        .ok_or("Report timestamp info not found")
        .unwrap();
    let report_timestamp = extract_report_timestamp(line)
        .ok_or("Failed to extract report timestamp")
        .unwrap();
    report_timestamp.date() == Zoned::now().date()
}

// Archive the current peak load value to the CSV file
async fn archive_current_peak_load() -> Result<(), Box<dyn Error>> {
    let (as_of, value) = extract_current_peak_load().await?;
    let record = PeakLoad {
        report_timestamp: as_of,
        snapshot_as_of: Zoned::now(),
        mw: value,
    };
    info!("Archiving current peak load: {:?}", record);

    let file_path_str = format!(
        "{}{}",
        env::var("CONFIG_DIR")?,
        "/alert_isone_peak_load_change/values.csv"
    );
    let file_path = Path::new(&file_path_str);
    let dir = file_path.parent().unwrap();
    match std::fs::create_dir_all(dir) {
        Ok(_) => (),
        Err(e) => return Err(Box::from(format!("Failed to create dir {:?}: {}", dir, e))),
    }

    let mut wtr = if file_path.exists() {
        csv::WriterBuilder::new()
            .has_headers(false)
            .from_writer(std::fs::OpenOptions::new().append(true).open(file_path)?)
    } else {
        csv::Writer::from_writer(std::fs::File::create(file_path)?)
    };
    wtr.serialize(record)?;
    wtr.flush()?;
    info!("Archived current peak load to {:?}", file_path);

    Ok(())
}

async fn get_html_page() -> Result<String, Box<dyn Error>> {
    use reqwest::get;

    let url = "https://www.iso-ne.com/markets-operations/system-forecast-status/seven-day-capacity-forecast";
    let html = get(url).await?.text().await?;
    Ok(html)
}

async fn extract_current_peak_load() -> Result<(Zoned, Decimal), Box<dyn Error>> {
    use scraper::{Html, Selector};
    let html = get_html_page().await?;

    let mut day = 2;
    if !has_published_report_today(&html) {
        day = 3;
    }

    let line = html
        .lines()
        .find(|line| line.contains("Report Generated"))
        .ok_or("Report timestamp info not found")?;
    let report_timestamp =
        extract_report_timestamp(line).ok_or("Failed to extract report timestamp")?;

    let document = Html::parse_document(&html);
    // Select the row with id "row-PeakLoadMw"
    let row_selector = Selector::parse("tr.row-PeakLoadMw").unwrap();
    // Select the cell with class "seven-day-data-column day2"
    let cell_selector = Selector::parse(&format!("td.seven-day-data-column.day{}", day)).unwrap();

    if let Some(row) = document.select(&row_selector).next() {
        if let Some(cell) = row.select(&cell_selector).next() {
            let text = cell
                .text()
                .collect::<Vec<_>>()
                .join("")
                .replace(",", "")
                .trim()
                .to_string();
            let value = Decimal::from_str_exact(&text)?;
            return Ok((report_timestamp, value));
        }
    }
    Err(format!("Peak load day{} cell not found", day).into())
}

/// Pass in the line containing the report timestamp, e.g.
/// `<p>Report Generated 12/07/2025 09:07 EST</p>`
fn extract_report_timestamp(s: &str) -> Option<Zoned> {
    // Regex to capture MM/DD/YYYY HH:MM TZ
    let re =
        Regex::new(r"Report Generated (\d{2})/(\d{2})/(\d{4}) (\d{2}):(\d{2}) (\w{3})").unwrap();
    if let Some(caps) = re.captures(s) {
        let month = &caps[1];
        let day = &caps[2];
        let year = &caps[3];
        let hour = &caps[4];
        let min = &caps[5];
        let tz = &caps[6];

        // Map EST/EDT to offset and zone name
        let (offset, zone) = match tz {
            "EST" => ("-05:00", "America/New_York"),
            "EDT" => ("-04:00", "America/New_York"),
            _ => return None,
        };

        let iso = format!("{year}-{month}-{day}T{hour}:{min}:00{offset}[{zone}]");
        Some(iso.parse().unwrap())
    } else {
        None
    }
}

fn html_table(data: Vec<PeakLoadWithChange>) -> build_html::Table {
    let mut table = build_html::Table::new();
    table.add_header_row(vec!["As Of", "Peak Load, MW", "Change, MW"]);

    for record in data {
        let change_str = match record.change {
            Some(c) if c > dec!(0) => format!(r#"<span style="color:green;">+{}</span>"#, c),
            Some(c) if c < dec!(0) => format!(r#"<span style="color:red;">{}</span>"#, c),
            Some(c) => format!("{}", c),
            None => " ".to_string(),
        };
        table.add_body_row(vec![
            record
                .report_timestamp
                .strftime("%Y-%m-%d %H:%M:%S %Z")
                .to_string(),
            format!(
                "{}",
                record
                    .mw
                    .round()
                    .to_i64()
                    .unwrap()
                    .to_formatted_string(&Locale::en)
            ),
            change_str,
        ]);
    }

    table
}

async fn send_email_alert(changes: Vec<PeakLoadWithChange>) -> Result<(), Box<dyn Error>> {
    let html = make_content(changes)?;
    println!("Generated email content: {:?}", html);

    let for_date = Zoned::now().date().tomorrow()?;
    let response = send_email(
        env::var("EMAIL_FROM").unwrap(),
        vec![env::var("EMAIL_MAIN").unwrap()],
        format!("ISONE peak load forecast change for {}", for_date),
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

/// Run this job several times in the morning before noon
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();
    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    // remove file if it's from a previous day
    let metadata = metadata("path/to/file.txt").await;
    if metadata.is_ok() {
        let modified: SystemTime = metadata?.modified()?;
        let dt = Timestamp::from_second(
            modified.duration_since(SystemTime::UNIX_EPOCH)?.as_secs() as i64,
        )?
        .in_tz("America/New_York")?
        .date();
        if dt != Zoned::now().date() {
            remove_file(get_file_path())?;
        }
    }

    info!("Checking to see if ISONE peak load for next day has changed...");
    archive_current_peak_load().await?;

    let changes = check_change()?;
    match changes.as_slice() {
        [] | [_] => {
            info!("No changes in peak load forecast detected.");
        }
        _ => {
            info!("Detected changes in peak load forecast: {:?}", changes);
            // check if the last change is close to right now.
            let last_change = changes.last().unwrap();
            let now = Zoned::now();
            let duration_since_change = now.duration_since(&last_change.snapshot_as_of);
            if duration_since_change.as_secs() <= 30 {
                info!("Last change is within the minute, sending alert email...");
                send_email_alert(changes).await?;
            } else {
                info!("Last change is older, not sending alert email.");
            }
        }
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_check_changes() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let values = [
            PeakLoad {
                report_timestamp: "2025-12-07T09:07:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                snapshot_as_of: "2025-12-07T09:10:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                mw: dec!(18875),
            },
            PeakLoad {
                report_timestamp: "2025-12-07T09:07:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                snapshot_as_of: "2025-12-07T09:15:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                mw: dec!(18875),
            },
            PeakLoad {
                report_timestamp: "2025-12-07T09:07:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                snapshot_as_of: "2025-12-07T09:20:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                mw: dec!(18875),
            },
            PeakLoad {
                report_timestamp: "2025-12-07T09:55:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                snapshot_as_of: "2025-12-07T10:10:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                mw: dec!(18920),
            },
            PeakLoad {
                report_timestamp: "2025-12-07T09:55:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                snapshot_as_of: "2025-12-07T10:15:00-05:00[America/New_York]"
                    .parse()
                    .unwrap(),
                mw: dec!(18920),
            },
        ];

        let mut changes: Vec<PeakLoadWithChange> = Vec::new();
        for (i, value) in values.iter().enumerate() {
            let change = if i == 0 {
                None
            } else {
                Some(value.mw - values[i - 1].mw)
            };
            if change == Some(dec!(0)) {
                continue;
            }
            changes.push(PeakLoadWithChange {
                report_timestamp: value.report_timestamp.clone(),
                snapshot_as_of: value.snapshot_as_of.clone(),
                mw: value.mw,
                change,
            });
        }
        assert_eq!(changes.len(), 2);
        println!("Changes: {:?}", changes);

        Ok(())
    }

    #[tokio::test]
    async fn test_archive_current_peak_load() {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        // let value = extract_current_peak_load().unwrap();
        // println!("Extracted peak load: {}", value);
        archive_current_peak_load().await.unwrap();
    }
}
