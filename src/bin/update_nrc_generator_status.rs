use std::{collections::HashMap, env, error::Error, fs, iter::Map, path::Path};

use bust::db::{nrc::generator_status_archive::DailyChangeResult, prod_db::ProdDb};
use clap::Parser;
use duckdb::{params, Connection};
use jiff::Zoned;
use log::{error, info};
use serde_json::Value;

struct EmailGroups {
    group_name: String,
    emails: Vec<String>,
}

/// Get the last change and email
fn notify(changes: Vec<DailyChangeResult>) -> Result<(), Box<dyn Error>> {
    let config_dir = env::var("CONFIG_DIR").unwrap();

    let archive = ProdDb::nrc_generator_status();
    let conn = Connection::open(archive.duckdb_path.clone())?;

    let sql = format!(
        r"
    CREATE TEMP TABLE Groups AS
    FROM '{}/update_nrc_generator_status/groups.csv';    
    ",
        config_dir
    );
    match conn.execute(&sql, params![]) {
        Ok(_) => info!("    created tmp table for groups"),
        Err(e) => error!("{:?}", e),
    }

    let mut rdr = csv::Reader::from_reader(fs::File::open(format!(
        "{}/update_nrc_generator_status/groups.csv",
        config_dir
    ))?);
    for result in rdr.records() {
        // The iterator yields Result<StringRecord, Error>, so we check the
        // error here.
        let record = result?;
        println!("{:?}", record);
    }

    let sql = format!(
        r"
    CREATE TEMP TABLE Emails AS
    FROM '{}/update_nrc_generator_status/groups.csv';    
    ",
        config_dir
    );
    match conn.execute(&sql, params![]) {
        Ok(_) => info!("    created tmp table for groups"),
        Err(e) => error!("{:?}", e),
    }



    // let config: String = fs::read_to_string("message.txt")?;
    // let v: Value = serde_json::from_str(&config)?;

    // // Group to unit names, e.g. ISONE -> ["Millstone 2", "Seabrook", ...]
    // let groups = match &v["groups"] {
    //     Value::Object(values) => values
    //         .iter()
    //         .map(|e| {
    //             (
    //                 e.0.to_owned(),
    //                 match e.1 {
    //                     Value::Array(values) => values.iter().map(|e| e.to_string()).collect(),
    //                     _ => panic!("Group values should be an array"),
    //                 },
    //             )
    //         })
    //         .collect::<HashMap<String, Vec<String>>>(),
    //     _ => panic!("Wrong config file format, no 'groups'!"),
    // };

    Ok(())
}

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

/// Run this job every day at 7AM
fn main() -> Result<(), Box<dyn Error>> {
    let args = Args::parse();

    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();

    let archive = ProdDb::nrc_generator_status();
    let yesterday = Zoned::now().date().yesterday().unwrap();
    let year = yesterday.year();
    match archive.download_years(vec![year.into()]) {
        Ok(_) => info!("Downloaded file successfully"),
        Err(e) => error!("{:?}", e),
    }

    match archive.update_duckdb(year.into()) {
        Ok(n) => info!("{} rows were updated", n),
        Err(e) => error!("{}", e),
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use bust::db::prod_db::ProdDb;

    #[test]
    fn notify_test() -> Result<(), Box<dyn Error>> {
        let archive = ProdDb::nrc_generator_status();
        Ok(())
    }
}
