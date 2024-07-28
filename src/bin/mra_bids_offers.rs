extern crate plotly;

use std::error::Error;
use bust::{interval::month::Month, isone::monthly_capacity_auction_archive::*};
use chrono_tz::Tz;
use itertools::Itertools;
use plotly::{
    common::Title,
    layout::{Axis, AxisType},
    Layout, Plot, Scatter,
};

use duckdb::{params, Connection, Result};

// In your project, we need to keep the arrow version same as the version used in duckdb.
// Refer to https://github.com/wangfenjin/duckdb-rs/issues/92
// You can either:
use duckdb::arrow::record_batch::RecordBatch;
// Or in your Cargo.toml, use * as the version; features can be toggled according to your needs
// arrow = { version = "*", default-features = false, features = ["prettyprint"] }
// Then you can:
// use arrow::record_batch::RecordBatch;

use duckdb::arrow::util::pretty::print_batches;

/// CREATE TABLE mra AS FROM '/home/adrian/Documents/repos/git/thumbert/bust/mra_2024-01.csv';
/// SELECT COUNT(*) FROM mra;
/// DESCRIBE mra;
/// .mode line
/// SELECT * FROM mra LIMIT 1;
/// .mode duckdb
/// SELECT SUM(Quantity) FROM mra
/// 
/// 
fn try_duckdb(rs: Vec<MraRecord> ) -> Result<()> {
    // let conn = Connection::open_in_memory()?;
    // conn.execute_batch(
    //     r"CREATE TABLE mra AS FROM 'mra_2024-01.csv'"
    // )?;

    // let mut stmt = conn.prepare(r"
    //     SELECT COUNT(*) FROM mra;
    // ")?;    
    // // let res = stmt.query_map([], |row| {
        
    //         row.get(0)?,
    //         name: row.get(1)?,
    //         data: row.get(2)?,
    //     })
    // })?;




    Ok(())
}




fn main() -> Result<(), Box<dyn Error>> {
    let month = Month::new(2024, 1, Tz::UTC).unwrap();
    let archive = MraCapacityArchive::new();
    let file = archive.get_file(month).unwrap();
    let rs = archive.read_file(file).unwrap();
    println!("{:?}", rs[0]);
    println!("Found {} records", rs.len());


    let _ = try_duckdb(rs.clone());

    let r0 = rs.first().unwrap();

    let mut bids: Vec<MraRecord> = rs
        .clone()
        .into_iter()
        .filter(|x| x.bid_offer == BidOffer::Bid)
        .collect();
    let mut offers: Vec<MraRecord> = rs
        .clone()
        .into_iter()
        .filter(|x| x.bid_offer == BidOffer::Offer)
        .collect();
    // sort bids decreasingly and offers increasingly
    bids.sort_by(|a, b| b.price.partial_cmp(&a.price).unwrap());
    offers.sort_by(|a, b| a.price.partial_cmp(&b.price).unwrap());

    let mut plot = Plot::new();
    let trace0 = Scatter::new(
        bids.clone()
            .into_iter()
            .map(|e| e.quantity)
            .scan(0.0, |acc, e| {
                *acc += e;
                Some(*acc)
            })
            .collect_vec(),
        bids.clone().into_iter().map(|e| e.price).collect_vec(),
    )
    .name("Bids");
    plot.add_trace(trace0);
    let trace1 = Scatter::new(
        offers
            .clone()
            .into_iter()
            .map(|e| e.quantity)
            .scan(0.0, |acc, e| {
                *acc += e;
                Some(*acc)
            })
            .collect_vec(),
        offers.clone().into_iter().map(|e| e.price).collect_vec(),
    )
    .name("Offers");
    plot.add_trace(trace1);
    plot.set_layout(
        Layout::new()
            .x_axis(Axis::new().title( Title::with_text("Quantity, MW")))
            .y_axis(
                Axis::new()
                    .title(Title::with_text("Price, $/kW-month"))
                    .type_(AxisType::Log),
            )
            .width(900)
            .height(700),
    );
    plot.show();

    Ok(()) // plot.write_html("out.html");
}
