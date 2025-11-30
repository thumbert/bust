use std::time::Duration;

use actix_web::{get, web, HttpResponse, Responder};

use crate::{db::nyiso::scheduled_outages::*, utils::lib_duckdb::open_with_retry};

#[get("/nyiso/transmission_outages/scheduled")]
async fn api_scheduled_outages(query: web::Query<QueryOutages>, db: web::Data<NyisoScheduledOutagesArchive>) -> impl Responder {
    let conn = open_with_retry(&db.duckdb_path, 8, Duration::from_millis(25), duckdb::AccessMode::ReadOnly);
    if conn.is_err() {
        return HttpResponse::InternalServerError()
            .body(format!("Unable to open the DuckDB connection {}", conn.unwrap_err()));
    }
    let rows = db.get_data(&conn.unwrap(), query.into_inner()).unwrap();
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
