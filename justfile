default:
    just --choose

release_server:
    cargo test
    cargo build --release
    cp -f ./target/release/server_bust ~/Software

update_hq_hydro_data:
    cargo test --package bust --lib -- db::hq::hydrometeorological_data_archive::tests --show-output
    cargo build --bin update_hq_hydro_data --release 
    cp ./target/release/update_hq_hydro_data ~/Software

update_isone_sevenday_solar_forecast_archive:
    cargo build --bin update_isone_sevenday_solar_forecast_archive --release 
    cp ./target/release/update_isone_sevenday_solar_forecast_archive ~/Software

update_nrc_generator_status:
    cargo test --package bust --lib -- db::nrc::update_nrc_generator_status::tests --show-output
    cargo build --bin update_nrc_generator_status --release 
    cp ./target/release/update_nrc_generator_status ~/Software