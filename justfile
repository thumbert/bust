default:
    just --choose

alerts_hq:
    cargo build --bin alerts_hq --release
    cp ./target/release/alerts_hq ~/Software

release_server:
    cargo test
    cargo build --bin server_bust --release
    cp -f ./target/release/server_bust ~/Software

update_canadian_energy_production:
    cargo build --bin update_canadian_energy_production --release
    cp ./target/release/update_canadian_energy_production ~/Software

update_hq_fuel_mix:
    cargo build --bin update_hq_fuel_mix --release
    cp ./target/release/update_hq_fuel_mix ~/Software

update_hq_hydro_data:
    cargo test --package bust --lib -- db::hq::hydrometeorological_data_archive::tests --show-output
    cargo build --bin update_hq_hydro_data --release 
    cp ./target/release/update_hq_hydro_data ~/Software

update_hq_total_demand_prelim:
    cargo build --bin update_hq_total_demand_prelim --release
    cp ./target/release/update_hq_total_demand_prelim ~/Software

update_ieso_prices_da:
    cargo build --bin update_ieso_prices_da --release 
    cp ./target/release/update_ieso_prices_da ~/Software

update_isone_actual_interchange:
    cargo build --bin update_isone_actual_interchange --release 
    cp ./target/release/update_isone_actual_interchange ~/Software

update_isone_daas_data:
    cargo build --bin update_isone_daas_data --release
    cp ./target/release/update_isone_daas_data ~/Software

update_isone_daas_strike_prices:
    cargo build --bin update_isone_daas_strike_prices --release
    cp ./target/release/update_isone_daas_strike_prices ~/Software

update_isone_prices_da:
    cargo build --bin update_isone_prices_da --release 
    cp ./target/release/update_isone_prices_da ~/Software

update_isone_prices_rt:
    cargo build --bin update_isone_prices_rt --release 
    cp ./target/release/update_isone_prices_rt ~/Software

update_isone_sevenday_capacity_report:
    cargo build --bin update_isone_sevenday_capacity_report --release
    cp ./target/release/update_isone_sevenday_capacity_report ~/Software

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

update_nyiso_scheduled_outages:
    cargo test --package bust --lib -- db::nyiso::scheduled_outages::tests --show-output
    cargo build --bin update_nyiso_scheduled_outages --release
    cp ./target/release/update_nyiso_scheduled_outages ~/Software

update_nyiso_transmission_outages_da:
    cargo test --package bust --lib -- db::nyiso::transmission_outages::tests --show-output
    cargo build --bin update_nyiso_transmission_outages_da --release
    cp ./target/release/update_nyiso_transmission_outages_da ~/Software

string_speed:
    cargo build --example string_speed --release 
    ./target/release/examples/string_speed
    hyperfine --warmup 3 --shell=none './target/release/examples/string_speed'

