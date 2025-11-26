use std::time::Duration;

use duckdb::{AccessMode, Config};

pub trait WithRetry {
    /// Use this function when you have an `execute_batch` statement which tries
    /// to attach various duckdb instances.
    /// Suggested `max_attempts = 8`, `initial_wait = Duration::from_millis(25)`.
    fn execute_batch_with_retry(
        &self,
        sql: &str,
        max_attempts: u32,
        initial_wait: Duration,
    ) -> Result<(), duckdb::Error>;
}

impl WithRetry for duckdb::Connection {
    fn execute_batch_with_retry(
        &self,
        sql: &str,
        max_attempts: u32,
        initial_wait: Duration,
    ) -> Result<(), duckdb::Error> {
        let mut attempts = 0;
        let mut wait_duration = initial_wait;

        loop {
            match self.execute_batch(sql) {
                Ok(_) => return Ok(()),
                Err(e) => {
                    attempts += 1;
                    if attempts >= max_attempts {
                        return Err(e);
                    }
                    std::thread::sleep(wait_duration);
                    wait_duration *= 2;
                    println!(
                        "Retrying DuckDB execute_batch after error: {} (attempt {}/{})",
                        e, attempts, max_attempts
                    );
                }
            }
        }
    }
}


/// Use this function to open a DuckDB connection.
/// Suggested `max_attempts = 8`, `initial_wait = Duration::from_millis(25)`.
pub fn open_with_retry(
    duckdb_path: &str,
    max_attempts: u32,
    initial_wait: Duration,
    access_mode: AccessMode,
) -> Result<duckdb::Connection, duckdb::Error> {
    let mut attempts = 0;
    let mut wait_duration = initial_wait;

    loop {
        let config = Config::default().access_mode(access_mode.clone()).unwrap();
        match duckdb::Connection::open_with_flags(duckdb_path, config) {
            Ok(conn) => return Ok(conn),
            Err(e) => {
                attempts += 1;
                if attempts >= max_attempts {
                    return Err(e);
                }
                std::thread::sleep(wait_duration);
                wait_duration *= 2;
            }
        }
    }
}
