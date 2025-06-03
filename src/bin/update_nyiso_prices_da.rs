use std::{collections::HashMap, error::Error, path::Path};

use build_html::Html;
use bust::{
    db::{nyiso::dalmp::*, prod_db::ProdDb},
    interval::month::{month, Month},
};
use clap::Parser;
use jiff::{civil::date, Zoned};
use rust_decimal::prelude::ToPrimitive;

struct Cell {
    /// NM1/C, NM2/C, Fitz/C, C/A, .. NPX/G, A, G
    location_name: String,
    /// time band: 1x16, 1x8, 7x24, 0, 1, ... 23
    band: String,
    value: f64,
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
    let data = ts
        .iter()
        .map(|(_, v)| v.to_owned())
        .collect::<Vec<f64>>();
    let sum: f64 = data.iter().sum();
    sum / data.len() as f64
}

fn calc_cells_simple(
    rows: &[Row],
    location_name: &str,
    ptids: &HashMap<String, i32>,
) -> Vec<Cell> {
    let ptid = ptids.get(location_name).unwrap().to_owned() as u32;
    let data = rows
        .iter()
        .filter(|row| row.ptid == ptid)
        .map(|row| (row.hour_beginning.clone(), row.value.to_f64().unwrap()))
        .collect::<Vec<(Zoned, f64)>>();

    let cells: Vec<Cell> = vec![
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

    cells
}

fn calc_cells(rows: &[Row], ptids: &HashMap<String, i32>) -> Vec<Vec<Cell>> {
    let mut table_cells: Vec<Vec<Cell>> = Vec::new();

    let nm1 = calc_cells_simple(rows, "NM1", ptids);
    let nm2 = calc_cells_simple(rows, "NM2", ptids);
    let fitz = calc_cells_simple(rows, "Fitz", ptids);
    let a = calc_cells_simple(rows, "A", ptids);
    let c = calc_cells_simple(rows, "C", ptids);
    let g = calc_cells_simple(rows, "G", ptids);

    let nm1_c = vec![
        Cell {
            location_name: "NM1/C".to_string(),
            band: "1x16".to_string(),
            value: nm1[0].value - c[0].value,
        },
        Cell {
            location_name: "NM1/C".to_string(),
            band: "1x8".to_string(),
            value: nm1[1].value - c[1].value,
        },
        Cell {
            location_name: "NM1/C".to_string(),
            band: "1x24".to_string(),
            value: nm1[2].value - c[2].value,
        },
    ];

    let nm2_c = vec![
        Cell {
            location_name: "NM2/C".to_string(),
            band: "1x16".to_string(),
            value: nm2[0].value - c[0].value,
        },
        Cell {
            location_name: "NM2/C".to_string(),
            band: "1x8".to_string(),
            value: nm2[1].value - c[1].value,
        },
        Cell {
            location_name: "NM2/C".to_string(),
            band: "1x24".to_string(),
            value: nm2[2].value - c[2].value,
        },
    ];

    let fitz_c = vec![
        Cell {
            location_name: "Fitz/C".to_string(),
            band: "1x16".to_string(),
            value: fitz[0].value - c[0].value,
        },
        Cell {
            location_name: "Fitz/C".to_string(),
            band: "1x8".to_string(),
            value: fitz[1].value - c[1].value,
        },
        Cell {
            location_name: "Fitz/C".to_string(),
            band: "1x24".to_string(),
            value: fitz[2].value - c[2].value,
        },
    ];

    table_cells.push(nm1_c);
    table_cells.push(nm2_c);
    table_cells.push(fitz_c);
    table_cells.push(a);
    table_cells.push(g);

    table_cells
}

/// Get the hourly data price data for all the ptids for tomorrow from DuckDB
fn get_data(ptids: &HashMap<String, i32>) -> Result<Vec<Row>, Box<dyn Error>> {
    let asof = Zoned::now().date().tomorrow().unwrap();

    let archive = ProdDb::nyiso_dalmp();
    let conn = duckdb::Connection::open(archive.duckdb_path.clone())?;
    let rows = archive.get_data(
        &conn,
        asof,
        asof,
        LmpComponent::Lmp,
        Some(ptids.clone().into_values().collect()),
    )?;

    Ok(rows)
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

/// Run this job every day at 11:00AM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let mut as_of = Zoned::now().date().tomorrow().unwrap();
    if Zoned::now().hour() < 11 {
        as_of = as_of.yesterday().unwrap();
    }

    let current_month = month(as_of.year(), as_of.month());
    let mut months: Vec<Month> = Vec::new();
    if as_of.day() < 6 {
        months.push(current_month.previous());
    }
    months.push(current_month);

    let archive = ProdDb::nyiso_dalmp();
    // for month in months {
    //     archive.download_file(month, NodeType::Gen)?;
    //     archive.download_file(month, NodeType::Zone)?;
    //     archive.update_duckdb(month)?;
    // }

    let mut ptids: HashMap<String, i32> = HashMap::new();
    ptids.insert("A".to_string(), 61752);
    ptids.insert("B".to_string(), 61753);
    ptids.insert("C".to_string(), 61754);
    ptids.insert("F".to_string(), 61757);
    ptids.insert("G".to_string(), 61758);
    ptids.insert("NPX".to_string(), 61745);
    //
    ptids.insert("NM1".to_string(), 23575);
    ptids.insert("NM2".to_string(), 23744);
    ptids.insert("Fitz".to_string(), 23598);
    ptids.insert("Ginna".to_string(), 23603);

    let rows = get_data(&ptids)?;
    let cells = calc_cells(&rows, &ptids);
    println!("{} cells", cells.len());



    Ok(())
}
