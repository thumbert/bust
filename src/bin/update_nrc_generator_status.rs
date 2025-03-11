use std::{collections::HashMap, error::Error, fs, iter::Map};

use bust::db::{nrc::generator_status_archive::DailyChangeResult, prod_db::ProdDb};
use jiff::Zoned;
use log::{error, info};
use serde_json::Value;


/// Get the last change and email
fn notify(changes: Vec<DailyChangeResult>) -> Result<(), Box<dyn Error>> {
    let config: String = fs::read_to_string("message.txt")?;
    let v: Value = serde_json::from_str(&config)?;

    // Group to unit names, e.g. ISONE -> ["Millstone 2", "Seabrook", ...]
    let groups = match &v["groups"] {
        Value::Object(values) => values
            .iter()
            .map(|e| {
                (
                    e.0.to_owned(),
                    match e.1 {
                        Value::Array(values) => values.iter().map(|e| e.to_string()).collect(),
                        _ => panic!("Group values should be an array"),
                    },
                )
            })
            .collect::<HashMap<String, Vec<String>>>(),
        _ => panic!("Wrong config file format, no 'groups'!"),
    };



    Ok(())
}

/// Run this job every day at 7AM
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

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
