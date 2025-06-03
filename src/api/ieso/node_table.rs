use std::str::FromStr;

use duckdb::{AccessMode, Config, Connection, Result};

use crate::db::{
    ieso::node_table::{LocationType, Row},
    prod_db::ProdDb,
};
use actix_web::{get, HttpResponse, Responder};

#[get("/ieso/node_table/all")]
async fn api_get_all() -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();
    let ids = get_all(&conn);
    match ids {
        Ok(vs) => HttpResponse::Ok().json(vs),
        Err(e) => HttpResponse::InternalServerError().body(e.to_string()),
    }
}

fn get_all(conn: &Connection) -> Result<Vec<Row>> {
    let query = "SELECT type, name FROM Locations;";
    let mut stmt = conn.prepare(query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        Ok(Row {
            r#type: match row.get_ref_unwrap(0).to_owned() {
                duckdb::types::Value::Enum(e) => LocationType::from_str(e.as_str()).unwrap(),
                _ => panic!("Oops"),
            },
            name: row.get::<usize, String>(1).unwrap(),
        })
    })?;
    let res: Vec<Row> = res_iter.map(|e| e.unwrap()).collect();
    Ok(res)
}

fn get_path() -> String {
    ProdDb::ieso_node_table().duckdb_path.to_string()
}

#[cfg(test)]
mod tests {

    use std::{env, path::Path};

    use duckdb::{AccessMode, Config, Connection, Result};

    use crate::api::ieso::node_table::*;

    #[test]
    fn test_names() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let names = get_all(&conn).unwrap();
        assert!(names.len() >= 110);
        Ok(())
    }

    #[test]
    fn api_status() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!("{}/ieso/node_table/all", env::var("RUST_SERVER").unwrap(),);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
        assert!(vs.len() > 1000);
        println!("{:?}", vs.iter().take(5));
        Ok(())
    }
}
