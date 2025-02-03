use actix_web::middleware::{self, Logger};
use actix_web::{get, App, HttpResponse, HttpServer, Responder};
use bust::api::{epa, hq, isone, nyiso};
use clap::Parser;
use env_logger::Env;

// extern crate r2d2;
// extern crate r2d2_duckdb;
extern crate duckdb;

// use std::thread;
// use r2d2_duckdb::DuckDBConnectionManager;
// type DbPool = r2d2::Pool<DuckDBConnectionManager>;

// lazy_static! {
//     static ref CONN: Connection = Connection::open("/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction/mra.duckdb").unwrap();

// }

#[derive(Parser, Debug)]
#[command(version, about, long_about = None)]
struct Args {
    /// Port number
    #[arg(short, long, default_value = "8111")]
    port: u16,
}

#[get("/")]
async fn hello() -> impl Responder {
    HttpResponse::Ok().body("Hello world!  This is a Rust server.")
}

#[actix_web::main]
async fn main() -> std::io::Result<()> {
    let args = Args::parse();

    env_logger::init_from_env(Env::default().default_filter_or("info"));
    // See https://actix.rs/docs/databases/  Not working well with DuckDb (yet)
    // let manager = DuckDBConnectionManager::file("/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction/mra.duckdb");
    // let pool = r2d2::Pool::builder().build(manager).unwrap();

    HttpServer::new(move || {
        App::new()
            .wrap(Logger::default())
            .wrap(middleware::Compress::default())
            // .app_data(Data::new(pool.clone()))
            .service(hello)
            // EPA
            .service(epa::hourly_emissions::all_facilities)
            .service(epa::hourly_emissions::all_columns)
            .service(epa::hourly_emissions::api_data)
            // HQ
            .service(hq::hq_water_level::api_daily_level)
            // ISONE
            .service(isone::capacity::monthly_capacity_results::results_interface)
            .service(isone::capacity::monthly_capacity_results::results_zone)
            .service(isone::capacity::monthly_capacity_bidsoffers::bids_offers)
            .service(isone::energy_offers::api_offers)
            .service(isone::energy_offers::api_stack)
            .service(isone::mis::sr_rsvcharge2::api_tab_data)
            // NYISO
            .service(nyiso::energy_offers::api_offers)
            .service(nyiso::energy_offers::api_stack)
    })
    .bind(("127.0.0.1", args.port))?
    // .bind(("0.0.0.0", args.port))? // use this if you want to allow all connections
    .run()
    .await
}

// lazy_static! {
//     static ref PERSONS: Vec<Person> = vec![
//         Person {
//             name: "John".to_string(),
//             age: 42,
//         },
//         Person {
//             name: "Jane".to_string(),
//             age: 37,
//         },
//         Person {
//             name: "Taylor".to_string(),
//             age: 4,
//         },
//         Person {
//             name: "Luke".to_string(),
//             age: 4,
//         },
//         Person {
//             name: "Bob".to_string(),
//             age: 82,
//         }
//     ];
// }
// #[derive(Serialize, Clone)]
// struct Person {
//     name: String,
//     age: u8,
// }
// #[get("/name/{name}")]
// async fn person(name: web::Path<String>) -> impl Responder {
//     let person = PERSONS.clone().into_iter().find(|x| x.name == *name);
//     match person {
//         Some(p) => HttpResponse::Ok().body(serde_json::to_string(&p).unwrap()),
//         None => HttpResponse::Ok().body(json!({"Error": format!("Person {} not found", name)}).to_string()),
//     }
// }
// #[derive(Deserialize)]
// struct PersonQuery {
//     name: Option<String>,
//     age: Option<String>,
// }
// /// Example of a query with parameters
// /// http://127.0.0.1:8111/person?name=Taylor
// /// http://127.0.0.1:8111/person?age=42
// /// http://127.0.0.1:8111/person?age=37|42               <-- special separator
// /// http://127.0.0.1:8111/person?name=Bob&age=82         <-- multiple filters
// ///
// #[get("/person")]
// async fn query_person(query: web::Query<PersonQuery>) -> String {
//     format!("Person query name: {:?}, age: {:?}", query.name, query.age)
// }
