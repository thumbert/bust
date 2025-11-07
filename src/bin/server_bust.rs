use std::path::Path;

use actix_cors::Cors;
use actix_web::middleware::{self, Logger};
use actix_web::web::Data;
use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use bust::api::{admin, epa, hq, ieso, isone, nrc, nyiso};
use bust::db::prod_db::ProdDb;
use clap::Parser;
use env_logger::Env;

// extern crate r2d2;
// extern crate r2d2_duckdb;
extern crate duckdb;

// use std::thread;
// use r2d2_duckdb::DuckDBConnectionManager;
// type DbPool = r2d2::Pool<DuckDBConnectionManager>;

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Environment name, e.g., test, prod
    #[arg(short, long, default_value = "prod")]
    env: String,
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!  This is a Rust server.")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();
    match args.env.as_str() {
        "prod" => env_logger::init_from_env(Env::default().default_filter_or("info")),
        "test" => env_logger::init_from_env(Env::default().default_filter_or("debug")),
        _ => panic!("Invalid environment"),
    }
    
    // See https://actix.rs/docs/databases/  Not working well with DuckDb (yet)
    // let manager = DuckDBConnectionManager::file("/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction/mra.duckdb");
    // let pool = r2d2::Pool::builder().build(manager).unwrap();

    let sd_daasdt = ProdDb::sd_daasdt();
    let sd_rtload = ProdDb::sd_rtload();
    let sr_rsvcharge2 = ProdDb::sr_rsvcharge2();
    let sr_rsvstl2 = ProdDb::sr_rsvstl2();

    dotenvy::from_path(Path::new(format!(".env/{}.env", args.env).as_str())).unwrap();
    let port = match args.env.as_str() {
        "prod" => 8111,
        "test" => 8112,
        _ => panic!("Invalid environment"),
    };

    HttpServer::new(move || {
        let cors = Cors::permissive();
        App::new()
            .wrap(cors)
            .wrap(Logger::default())
            .wrap(middleware::Compress::default())
            .app_data(Data::new((ProdDb::isone_dalmp(), ProdDb::isone_rtlmp(), ProdDb::buckets())))
            .app_data(Data::new(ProdDb::isone_mra_bids_offers()))
            .app_data(Data::new(ProdDb::isone_participants_archive()))
            .app_data(Data::new(sd_daasdt.clone()))
            .app_data(Data::new(sd_rtload.clone()))
            .app_data(Data::new(sr_rsvcharge2.clone()))
            .app_data(Data::new(sr_rsvstl2.clone()))
            .app_data(Data::new(ProdDb::nyiso_transmission_outages_da()))
            .service(hello)
            // Admin
            .service(admin::jobs::api_get_job_names)
            .service(admin::jobs::api_get_log)
            .service(admin::jobs::api_run_job)
            // EPA
            .service(epa::hourly_emissions::all_facilities)
            .service(epa::hourly_emissions::all_columns)
            .service(epa::hourly_emissions::api_data)
            // HQ
            .service(hq::hq_water_level::api_daily_level)
            // IESO
            .service(ieso::node_table::api_get_all)
            .service(ieso::dalmp::api_hourly_prices)
            .service(ieso::dalmp::api_daily_prices)
            // ISONE
            .service(isone::actual_interchange::api_actual_flows)
            .service(isone::capacity::monthly_capacity_results::results_interface)
            .service(isone::capacity::monthly_capacity_results::results_zone)
            .service(isone::capacity::monthly_capacity_bidsoffers::bids_offers)
            .service(isone::masked_daas_offers::api_offers)
            .service(isone::lmp::api_daily_prices)
            .service(isone::lmp::api_hourly_prices)
            .service(isone::lmp::api_monthly_prices)
            .service(isone::lmp::api_term_prices)
            .service(isone::masked_energy_offers::api_offers)
            .service(isone::masked_energy_offers::api_stack)
            .service(isone::mis::sd_daasdt::api_daily_charges)
            .service(isone::mis::sd_daasdt::api_daily_credits)
            .service(isone::mis::sd_daasdt::api_tab_data)
            .service(isone::mis::sr_rsvcharge2::api_daily_charges)
            .service(isone::mis::sr_rsvcharge2::api_tab_data)
            .service(isone::mis::sr_rsvstl2::api_daily_credits)
            .service(isone::mis::sr_rsvstl2::api_tab_data)
            .service(isone::participant_list::participants)
            .service(isone::ttc::api_ttc_data)
            // NRC
            .service(nrc::generator_status::api_get_names)
            .service(nrc::generator_status::api_status)
            // NYISO
            .service(nyiso::lmp::api_daily_prices)
            .service(nyiso::lmp::api_hourly_prices)
            .service(nyiso::lmp::api_monthly_prices)
            .service(nyiso::energy_offers::api_offers)
            .service(nyiso::energy_offers::api_stack)
            .service(nyiso::scheduled_outages::api_scheduled_outages)
            .service(nyiso::transmission_outages::api_transmission_outages_da)
    })
    .bind(("127.0.0.1", port))?
    // .bind(("0.0.0.0", args.port))? // use this if you want to allow all connections
    .run()
    .await
}
