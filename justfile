default:
    just --choose

release_server:
    cargo test
    cargo build --bin server_bust --release
    cp -f ./target/release/server_bust ~/Software

update_hq_hydro_data:
    cargo test --package bust --lib -- db::hq::hydrometeorological_data_archive::tests --show-output
    cargo build --bin update_hq_hydro_data --release 
    cp ./target/release/update_hq_hydro_data ~/Software

update_isone_actual_interchange:
    cargo build --bin update_isone_actual_interchange --release 
    cp ./target/release/update_isone_actual_interchange ~/Software

update_isone_prices_da:
    cargo build --bin update_isone_prices_da --release 
    cp ./target/release/update_isone_prices_da ~/Software

update_isone_prices_rt:
    cargo build --bin update_isone_prices_rt --release 
    cp ./target/release/update_isone_prices_rt ~/Software

update_isone_sevenday_solar_forecast_archive:
    cargo build --bin update_isone_sevenday_solar_forecast_archive --release 
    cp ./target/release/update_isone_sevenday_solar_forecast_archive ~/Software

update_nrc_generator_status:
    cargo test --package bust --lib -- db::nrc::update_nrc_generator_status::tests --show-output
    cargo build --bin update_nrc_generator_status --release 
    cp ./target/release/update_nrc_generator_status ~/Software

update_nyiso_prices_da:
    cargo test --package bust --lib -- db::nyiso::dam_prices_archive::tests --show-output
    cargo build --bin update_nyiso_prices_da --release 
    cp ./target/release/update_nyiso_prices_da ~/Software
    cargo build --bin email_nyiso_prices_da --release
    cp ./target/release/email_nyiso_prices_da ~/Software
    cp -r .env ~/Software

