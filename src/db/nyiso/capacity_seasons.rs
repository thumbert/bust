// Auto-generated Rust stub for DuckDB table: capacity_seasons
// Created on 2026-07-06 with Dart package reduct

use duckdb::Connection;
use serde::{Deserialize, Serialize};

#[derive(Clone)]
pub struct NyisoCapacitySeasonsArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub id: i64,
    pub description: String,
}

pub fn get_data(
    conn: &Connection,
    limit: Option<usize>,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    id,
    description
FROM capacity_seasons WHERE 1=1"#,
    );
    match limit {
        Some(l) => {
            query.push_str(&format!(
                "
LIMIT {};",
                l
            ));
        }
        None => {
            query.push(';');
        }
    }

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let id: i64 = row.get::<usize, i64>(0)?;
        let description: String = row.get::<usize, String>(1)?;
        Ok(Record { id, description })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::db::prod_db::ProdDb;
    use duckdb::{AccessMode, Config, Connection};
    use std::error::Error;

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::nyiso_capacity_seasons().duckdb_path, config)
                .unwrap();
        let xs: Vec<Record> = get_data(&conn, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }
}
