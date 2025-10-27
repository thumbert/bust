use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{AccessMode, Config, Connection};

use crate::db::isone::masked_data::mra_archive::{get_bids_offers, IsoneMraBidsOffersArchive};

#[get("/isone/capacity/mra/bids_offers/start/{start}/end/{end}")]
async fn bids_offers(
    path: web::Path<(String, String)>,
    db: web::Data<IsoneMraBidsOffersArchive>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(db.duckdb_path.clone(), config).unwrap();

    let start = match path.0.replace('-', "").parse::<u32>() {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest()
                .body(format!("Invalid start month, needs yyyy-mm format. {}", e))
        }
    };
    let end = match path.1.replace('-', "").parse::<u32>() {
        Ok(v) => v,
        Err(e) => {
            return HttpResponse::BadRequest()
                .body(format!("Invalid end month, needs yyyy-mm format. {}", e))
        }
    };
    let bids_offers = get_bids_offers(&conn, start, end).unwrap();
    HttpResponse::Ok().json(bids_offers)
}

#[cfg(test)]
mod tests {
    use std::{env, path::Path};

    use duckdb::Result;

    use crate::db::isone::masked_data::mra_archive::Record;

    #[test]
    fn get_bids_offers_test() -> Result<(), Box<dyn std::error::Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let url = format!(
            "{}/isone/capacity/mra/bids_offers/start/2025-06/end/2025-06",
            env::var("RUST_SERVER").unwrap(),
        );
        let response = reqwest::blocking::get(url)?.text()?;
        let vs: Vec<Record> = serde_json::from_str(&response).unwrap();
        assert_eq!(vs.len(), 400);
        Ok(())
    }
}
