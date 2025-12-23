use std::time::Duration;

use duckdb::{AccessMode, Connection, Result};
use serde::{Deserialize, Serialize};

use crate::{db::caiso::dalmp_archive::CaisoDaLmpArchive, utils::lib_duckdb::open_with_retry};
use actix_web::{get, web, HttpResponse, Responder};

#[get("/caiso/node_table/all")]
async fn api_get_all(db: web::Data<CaisoDaLmpArchive>) -> impl Responder {
    let conn = open_with_retry(
        &db.duckdb_path,
        8,
        Duration::from_millis(25),
        AccessMode::ReadOnly,
    );
    if conn.is_err() {
        return HttpResponse::InternalServerError().body(format!(
            "Error opening DuckDB database: {}",
            conn.err().unwrap()
        ));
    }

    let ids = get_all(&conn.unwrap());
    match ids {
        Ok(vs) => HttpResponse::Ok().json(vs),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

fn get_all(conn: &Connection) -> Result<Vec<Row>> {
    let query = r#"
SELECT DISTINCT node_id 
FROM lmp
WHERE hour_beginning = '2025-12-01T00:00:00-08:00'
ORDER BY node_id;    
    "#;
    let mut stmt = conn.prepare(query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        Ok(Row {
            name: row.get::<usize, String>(0).unwrap(),
        })
    })?;
    let res: Vec<Row> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

#[derive(Debug, Deserialize, Serialize)]
pub struct Row {
    // pub r#type: LocationType,
    pub name: String,
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Result};

    use crate::db::prod_db::ProdDb;

    use super::*;

    #[test]
    fn test_names() -> Result<()> {
        let conn = open_with_retry(
            &ProdDb::caiso_dalmp().duckdb_path,
            8,
            Duration::from_millis(25),
            AccessMode::ReadOnly,
        ).unwrap();
        let names = get_all(&conn).unwrap();
        assert!(names.len() >= 110);
        Ok(())
    }

    #[test]
    fn api_status() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!("{}/caiso/node_table/all", env::var("RUST_SERVER").unwrap(),);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
        assert!(vs.len() > 1000);
        // println!("{:?}", vs.iter().take(5).collect::<Vec<&Row>>());
        Ok(())
    }
}
