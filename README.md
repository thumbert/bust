# bust
Beginning Rust

 To run a development server:
 * Run `cargo test`
 * Run `cargo build`
 * Launch as `./target/debug/server --port=8112`

To release a new version:
 * Run `cargo test`
 * Run `cargo build --release`  Took 13m 44s!
 * cp ./target/release/server ~/Software
 * Launch `~/Software/server` 


To check which process uses the 8111 port:
`lsof -i :8111`


To release a bin file
* cargo build --bin update_isone_sevenday_solar_forecast_archive --release 
* cp ./target/release/update_isone_sevenday_solar_forecast_archive ~/Software


To release a bin file
* cargo run --package bust --bin rebuild_duckdbs
* RUST_LOG=debug ./target/debug/rebuild_duckdbs

