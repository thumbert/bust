# bust
Beginning Rust

```rust
cargo build --bin qplot

duckdb -csv -c "
ATTACH '~/Downloads/Archive/DuckDB/isone/ttc.duckdb' AS ttc;
SELECT hour_beginning, hq_phase2_import
FROM ttc.ttc_limits 
WHERE hour_beginning >= '2024-01-01 00:00:00-05:00'
AND hour_beginning < '2024-01-05 00:00:00-05:00'
ORDER BY hour_beginning;
" | qplot 
```



 To run a development server:
 * Run `cargo test`
 * Run `cargo build --bin server_bust`
 * Launch as `./target/debug/server_bust --env=test`

To release a new version:
 * Run `cargo test`
 * Run `cargo build --release`  Took 13m 44s!
 * cp ./target/release/server_bust ~/Software
 * Launch `~/Software/server_bust` 


To check which process uses the 8111 port:
`lsof -i :8111`


To release a bin file
* cargo build --bin update_isone_sevenday_solar_forecast_archive --release 
* cp ./target/release/update_isone_sevenday_solar_forecast_archive ~/Software

cargo build --bin update_hq_hydro_data --release 
cp ./target/release/update_hq_hydro_data ~/Software


To release a bin file
* cargo run --package bust --bin rebuild_duckdbs
* RUST_LOG=debug ./target/debug/rebuild_duckdbs

