use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{AccessMode, Config, Connection};

use crate::db::isone::participants_archive::*;

#[get("/isone/participant_list/current")]
async fn participants(
    db: web::Data<IsoneParticipantsArchive>,
    query: web::Query<QueryFilter>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(db.duckdb_path.clone(), config).unwrap();
    let rows = get_data(&conn, &query).unwrap();
    HttpResponse::Ok().json(rows)
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    use duckdb::Result;

    use crate::db::isone::participants_archive::Record;

    #[test]
    fn get_participants_test() -> Result<(), Box<dyn std::error::Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/participant_list/current",
            env::var("RUST_SERVER").unwrap(),
        );
        println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Record> = serde_json::from_str(&response).unwrap();
        assert!(vs.len() > 500);
        Ok(())
    }
}
