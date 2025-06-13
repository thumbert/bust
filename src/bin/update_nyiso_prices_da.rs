use std::{collections::HashMap, env, error::Error, io::Write, path::Path};

use build_html::{Html, HtmlContainer, TableCell, TableRow};
use bust::{
    db::{nyiso::dalmp::*, prod_db::ProdDb},
    interval::month::{month, Month}, utils::send_email::*,
};
use clap::Parser;
use jiff::{
    civil::{date, Date},
    Zoned,
};
use log::info;
use rust_decimal::prelude::ToPrimitive;

#[derive(Debug, Clone)]
struct Cell {
    /// NM1/C, NM2/C, Fitz/C, C/A, .. NPX/G, A, G
    location_name: String,
    /// time band: 1x16, 1x8, 7x24, 0, 1, ... 23
    band: String,
    value: f64,
}

fn get_ptids() -> HashMap<String, i32> {
    let mut ptids: HashMap<String, i32> = HashMap::new();
    ptids.insert("A".to_string(), 61752);
    ptids.insert("B".to_string(), 61753);
    ptids.insert("C".to_string(), 61754);
    ptids.insert("D".to_string(), 61755);
    ptids.insert("E".to_string(), 61756);
    ptids.insert("F".to_string(), 61757);
    ptids.insert("G".to_string(), 61758);
    ptids.insert("H".to_string(), 61759);
    ptids.insert("I".to_string(), 61760);
    ptids.insert("J".to_string(), 61761);
    ptids.insert("K".to_string(), 61762);
    ptids.insert("H Q".to_string(), 61844);
    ptids.insert("NPX".to_string(), 61845);
    ptids.insert("PJM".to_string(), 61847);
    //
    ptids.insert("NM1".to_string(), 23575);
    ptids.insert("NM2".to_string(), 23744);
    ptids.insert("Fitz".to_string(), 23598);
    ptids.insert("Ginna".to_string(), 23603);
    ptids
}

fn calc_1x16(ts: &[(Zoned, f64)]) -> f64 {
    let data = ts
        .iter()
        .filter(|(ts, _)| ts.hour() >= 7 && ts.hour() < 23)
        .map(|(_, v)| v.to_owned())
        .collect::<Vec<f64>>();
    let sum: f64 = data.iter().sum();
    sum / data.len() as f64
}

fn calc_1x8(ts: &[(Zoned, f64)]) -> f64 {
    let data = ts
        .iter()
        .filter(|(ts, _)| ts.hour() < 7 || ts.hour() == 23)
        .map(|(_, v)| v.to_owned())
        .collect::<Vec<f64>>();
    let sum: f64 = data.iter().sum();
    sum / data.len() as f64
}

fn calc_1x24(ts: &[(Zoned, f64)]) -> f64 {
    let data = ts.iter().map(|(_, v)| v.to_owned()).collect::<Vec<f64>>();
    let sum: f64 = data.iter().sum();
    sum / data.len() as f64
}

fn calc_cells_simple(rows: &[Row], location_name: &str, ptids: &HashMap<String, i32>) -> Vec<Cell> {
    let ptid = ptids.get(location_name).unwrap().to_owned() as u32;
    let data = rows
        .iter()
        .filter(|row| row.ptid == ptid)
        .map(|row| (row.hour_beginning.clone(), row.value.to_f64().unwrap()))
        .collect::<Vec<(Zoned, f64)>>();

    let mut cells: Vec<Cell> = vec![
        Cell {
            location_name: location_name.to_owned(),
            band: "1x16".to_owned(),
            value: calc_1x16(&data),
        },
        Cell {
            location_name: location_name.to_owned(),
            band: "1x8".to_owned(),
            value: calc_1x8(&data),
        },
        Cell {
            location_name: location_name.to_owned(),
            band: "1x24".to_owned(),
            value: calc_1x24(&data),
        },
    ];
    for e in data {
        cells.push(Cell {
            location_name: location_name.to_owned(),
            band: format!("HB{}", e.0.hour()),
            value: e.1,
        });
    }

    cells
}

fn calc_cells_spread(source: &[Cell], sink: &[Cell]) -> Vec<Cell> {
    let name = format!("{}/{}", sink[0].location_name, source[0].location_name);
    let mut cells: Vec<Cell> = Vec::new();
    for i in 0..source.len() {
        cells.push(Cell {
            location_name: name.clone(),
            band: source[i].band.clone(),
            value: sink[i].value - source[i].value,
        });
    }
    cells
}

fn calc_cells(rows: &[Row], ptids: &HashMap<String, i32>) -> Vec<Vec<Cell>> {
    let mut table_cells: Vec<Vec<Cell>> = Vec::new();

    let nm1 = calc_cells_simple(rows, "NM1", ptids);
    let nm2 = calc_cells_simple(rows, "NM2", ptids);
    let fitz = calc_cells_simple(rows, "Fitz", ptids);
    let ginna = calc_cells_simple(rows, "Ginna", ptids);
    let a = calc_cells_simple(rows, "A", ptids);
    let b = calc_cells_simple(rows, "B", ptids);
    let c = calc_cells_simple(rows, "C", ptids);
    let g = calc_cells_simple(rows, "G", ptids);
    let npx = calc_cells_simple(rows, "NPX", ptids);

    let nm1_c = calc_cells_spread(&c, &nm1);
    let nm2_c = calc_cells_spread(&c, &nm2);
    let fitz_c = calc_cells_spread(&c, &fitz);
    let ginna_b = calc_cells_spread(&b, &ginna);
    let npx_g = calc_cells_spread(&g, &npx);
    let c_a = calc_cells_spread(&a, &c);
    let g_a = calc_cells_spread(&a, &g);

    table_cells.push(nm1_c);
    table_cells.push(nm2_c);
    table_cells.push(fitz_c);
    table_cells.push(ginna_b);
    table_cells.push(c_a);
    table_cells.push(g_a);
    table_cells.push(npx_g);
    table_cells.push(a);
    table_cells.push(c);
    table_cells.push(g);

    table_cells
}

/// Make an HTML table from the data
fn html_table(data: Vec<Vec<Cell>>) -> build_html::Table {
    let mut table = build_html::Table::new();
    let mut header = vec!["Location".to_string()];
    header.extend(data[0].iter().map(|cell| cell.band.clone()));
    table.add_header_row(header);

    for row in data {
        let mut trow = TableRow::new();
        // add the location name
        let mut tcell = TableCell::new(build_html::TableCellType::Data);
        if row[0].location_name.len() == 1 {
            tcell = tcell.with_attributes([("style", "background-color:#eceff4;")]);
        }
        tcell = tcell.with_raw(row[0].location_name.clone());
        trow.add_cell(tcell);
        // add the prices
        for cell in &row {
            let mut tcell = TableCell::new(build_html::TableCellType::Data);
            if cell.band == "1x24" {
                tcell = tcell.with_attributes([("class", "col-border")]);
            }
            if cell.location_name.len() == 1 {
                tcell = tcell.with_attributes([("style", "background-color:#eceff4;")]);
            }
            tcell = tcell.with_raw(format!("{:.2}", cell.value));
            trow.add_cell(tcell);
        }
        table.add_custom_body_row(trow);
    }
    table
}

/// Get the hourly data price data for all the ptids for tomorrow from DuckDB
fn make_table(
    asof: Date,
    component: LmpComponent,
    ptids: &HashMap<String, i32>,
    archive: &NyisoDalmpArchive,
) -> Result<Vec<Vec<Cell>>, Box<dyn Error>> {
    let conn = duckdb::Connection::open(archive.duckdb_path.clone())?;
    let rows = archive.get_data(
        &conn,
        asof,
        asof,
        component,
        Some(ptids.clone().into_values().collect()),
    )?;

    let cells = calc_cells(&rows, ptids);

    Ok(cells)
}

fn make_report(asof: Date) -> Result<String, Box<dyn Error>> {
    let ptids = get_ptids();

    let archive = ProdDb::nyiso_dalmp();
    let tbl_lmp = html_table(make_table(asof, LmpComponent::Lmp, &ptids, &archive)?);
    let tbl_mcc = html_table(make_table(asof, LmpComponent::Mcc, &ptids, &archive)?);
    let tbl_mcl = html_table(make_table(asof, LmpComponent::Mcc, &ptids, &archive)?);

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
            <h3>LMP, $/MWh</h3>
            {}
            <h3>MCC, $/MWh</h3>
            {}
            <h3>MCL, $/MWh</h3>
            {}
        </body>
    </html>"#,
        tbl_lmp.to_html_string(),
        tbl_mcc.to_html_string(),
        tbl_mcl.to_html_string(),
    );
    let mut file = std::fs::File::create("/home/adrian/Downloads/nyiso_dalmp.html")?;
    file.write_all(html.as_bytes())?;
    info!("Report written to /home/adrian/Downloads/nyiso_dalmp.html");

    Ok(html)
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

/// Run this job every day at 11:00AM
#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    // let mut asof = Zoned::now().date();
    let mut asof = date(2025, 6, 5);
    if Zoned::now().hour() >= 11 {
        asof = asof.tomorrow().unwrap();
    }

    let current_month = month(asof.year(), asof.month());
    let mut months: Vec<Month> = Vec::new();
    if asof.day() < 4 {
        months.push(current_month.previous());
    }
    months.push(current_month);

    let archive = ProdDb::nyiso_dalmp();
    for month in months {
        archive.download_file(month, NodeType::Gen)?;
        archive.download_file(month, NodeType::Zone)?;
        archive.update_duckdb(month)?;
    }

    let html = make_report(asof)?;

    let response = send_email(
        env::var("EMAIL_FROM").unwrap(),
        vec![env::var("GMAIL_BASE").unwrap()],
        format!("NYISO LMP report for {}", asof),
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
