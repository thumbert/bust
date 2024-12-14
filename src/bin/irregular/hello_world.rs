use std::error::Error;

use log::{info, warn};

fn main() -> Result<(), Box<dyn Error>> {
    println!("Hello world");
    
    env_logger::builder()
        .filter_level(log::LevelFilter::Info)
        .init();

    info!("This is informational!");

    warn!("I'm warning you!");

    Ok(())
}
