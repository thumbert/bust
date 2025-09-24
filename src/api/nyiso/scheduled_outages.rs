use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{
    AccessMode, Config, Connection,
};

use crate::db::{nyiso::scheduled_outages::*, prod_db::ProdDb};

#[get("/nyiso/transmission_outages/scheduled")]
async fn api_scheduled_outages(query: web::Query<QueryOutages>) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let archive = ProdDb::nyiso_scheduled_outages();
    let conn = Connection::open_with_flags(archive.duckdb_path.clone(), config).unwrap();

    let rows = archive.get_data(&conn, query.into_inner()).unwrap();
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
            "{}/nyiso/transmission_outages/scheduled?ptid=25858",
            env::var("RUST_SERVER").unwrap(),
        );
        println!("{}", url);
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Row> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 1);
        Ok(())
    }
}
