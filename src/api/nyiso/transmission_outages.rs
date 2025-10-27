use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{AccessMode, Config, Connection};

use crate::db::nyiso::{scheduled_outages::QueryOutages, transmission_outages_da::*};

#[get("/nyiso/transmission_outages/da")]
async fn api_transmission_outages_da(
    query: web::Query<QueryOutages>,
    db: web::Data<NyisoTransmissionOutagesDaArchive>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(db.duckdb_path.clone(), config).unwrap();
    let rows = db.get_data(&conn, &query).unwrap();
    HttpResponse::Ok().json(rows)
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    use crate::db::nyiso::scheduled_outages::Row;

    #[test]
    fn api_outages_test() -> Result<(), reqwest::Error> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/nyiso/transmission_outages/da?as_of=2025-10-21&equipment_name_like=CLAY%",
            env::var("RUST_SERVER").unwrap(),
        );
        println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        print!("Response: {}", response);
        let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 1);
        Ok(())
    }
}
