use std::{error::Error, thread, time::Duration};
use std::sync::{Arc, Mutex};

use bust::db::{isone::mis::lib_mis::MisArchiveDuckDB, prod_db::ProdDb};
use log::info;

/// Run this job every day at 10AM
fn main() -> Result<(), Box<dyn Error>> {
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    info!("Starting ...");

    let archives: Vec<Arc<Mutex<dyn MisArchiveDuckDB>>> = vec![
        Arc::new(Mutex::new(ProdDb::sd_daasdt())),
        Arc::new(Mutex::new(ProdDb::sd_rtload())),
    ];

    let mut handles = vec![];
    for archive in archives {
        let months = archive.lock().unwrap().get_months();
        for month in months {
            let archive = Arc::clone(&archive);
            let handle = thread::spawn(move || {
                let archive = archive.lock().unwrap();
                println!("Archive: {}, Month: {:?}", archive.report_name(), month);
                thread::sleep(Duration::from_millis(1000));
            });
            handles.push(handle);
        }
    }

    for handle in handles {
        handle.join().unwrap();
    }
    info!("Done");

    Ok(())
}
