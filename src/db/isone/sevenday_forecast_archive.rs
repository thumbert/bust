use reqwest::blocking::Client;
use reqwest::header::ACCEPT;
use reqwest::Error;
use std::{env, fs};

use chrono::NaiveDate;

struct SevendayForecastArchive {}

impl SevendayForecastArchive {
    fn base_dir() -> String {
        "/home/adrian/Downloads/Archive/IsoExpress/7dayCapacityForecast".to_string()
    }

    fn filename(date: NaiveDate) -> String {
        SevendayForecastArchive::base_dir()
            + "/Raw"
            + "/7dayforecast_"
            + &date.to_string()
            + ".json"
    }

    fn download_days(days: Vec<NaiveDate>) -> Result<(), Error> {
        let client = Client::new();
        let user_name = env::var("ISONE_WS_USER").unwrap();
        let password = env::var("ISONE_WS_PASSWORD").unwrap();

        for day in days.into_iter() {
            let yyyymmdd = day.to_string().replace('-', "");

            let url = "https://webservices.iso-ne.com/api/v1.1/sevendayforecast/day/".to_string()
                + &yyyymmdd
                + "/all";
            println!("url:{}", url);    
            let response = client
                .get(url)
                .basic_auth(&user_name, Some(&password))
                .header(ACCEPT, "application/json")
                .send();

            if let Ok(res) = response {
                // println!("{:?}", res.text());
                if let Ok(data) = res.text() {
                    fs::write(SevendayForecastArchive::filename(day), data).expect("Writing to file failed");
                }
            }
        }

        Ok(())
    }
}


struct DailyForecast {
    day_index: u8,
    market_date: NaiveDate,
    cso_mw: f32,
    cold_weather_outages_mw: f32,
    other_outages_mw: f32,
    delist_mw: f32,
    total_available_gen_mw: f32,
    peak_import_mw: f32, 
    total_available_gen_import_mw: f32,
    peak_load: f32,
    replacement_reserve_requirement_mw: f32,
    required_reserve_mw: f32, 
    required_reserve_incl_repl_mw: f32,
    total_load_plus_required_reserve_mw: f32,
    drr_mw: f32,
    surplus_deficiency_mw: f32,
    is_power_watch: bool,
    is_power_warn: bool,
    is_cold_weather_watch: bool,
    is_cold_weather_warn: bool,
    is_cold_weather_event: bool, 
    bos_high_temp: f32, 
    bos_dew_point: f32,
    bdl_high_temp: f32,
    bdl_dew_point: f32,
}

#[cfg(test)]
mod tests {
    use chrono::NaiveDate;
    use reqwest::Error;
    use serde_json::Value;
    use std::fs;

    use super::{DailyForecast, SevendayForecastArchive};


    #[test]
    fn download_day() -> Result<(), Error> {
        SevendayForecastArchive::download_days(vec![NaiveDate::from_ymd_opt(2024, 6, 17).unwrap()])
    }




    #[test]
    fn parse_file() -> Result<(), String> {
        let path = SevendayForecastArchive::filename(NaiveDate::from_ymd_opt(2024,6,17).unwrap());
        let json = fs::read_to_string(path).expect("Failed to read the file");
        let v: Value = serde_json::from_str(&json).unwrap();
        let fcsts = &v["SevenDayForecasts"]["SevenDayForecast"];

        if let Value::Array(arr) = fcsts {
            println!("{:?}", arr.first());
        }


        // let fcst = DailyForecast {
        //     day_index: todo!(), 
        //     market_date: todo!(), cso_mw: todo!(), cold_weather_outages_mw: todo!(), other_outages_mw: todo!(), delist_mw: todo!(), total_available_gen_mw: todo!(), peak_import_mw: todo!(), total_available_gen_import_mw: todo!(), peak_load: todo!(), replacement_reserve_requirement_mw: todo!(), required_reserve_mw: todo!(), required_reserve_incl_repl_mw: todo!(), total_load_plus_required_reserve_mw: todo!(), drr_mw: todo!(), surplus_deficiency_mw: todo!(), is_power_watch: todo!(), is_power_warn: todo!(), is_cold_weather_watch: todo!(), is_cold_weather_warn: todo!(), is_cold_weather_event: todo!(), bos_high_temp: todo!(), bos_dew_point: todo!(), bdl_high_temp: todo!(), bdl_dew_point: todo!() };



        Ok(())
    }
}
