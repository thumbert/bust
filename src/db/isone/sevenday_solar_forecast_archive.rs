use csv::StringRecord;
use jiff::civil::*;
use jiff::Zoned;
use reqwest::blocking::{get, Client};
use reqwest::header::{
    ACCEPT, ACCESS_CONTROL_ALLOW_CREDENTIALS, ACCESS_CONTROL_ALLOW_ORIGIN, COOKIE, DNT, SET_COOKIE,
    UPGRADE_INSECURE_REQUESTS,
};
use reqwest::Error;
use std::fs::{self, File};
use std::io::{self, Read};

#[derive(Debug)]
struct Row {
    report_date: Date,
    forecast_hour_beginning: Zoned,
    forecast_generation: usize,
}

struct SevendaySolarForecastArchive {}

impl SevendaySolarForecastArchive {
    fn base_dir() -> String {
        "/home/adrian/Downloads/Archive/IsoExpress/7daySolarForecast".to_string()
    }

    fn filename(date: Date) -> String {
        SevendaySolarForecastArchive::base_dir()
            + "/Raw"
            + "/solar_forecast_"
            + &date.to_string()
            + ".csv"
    }

    fn read_file(filename: String) {
        let timezone_name = "America/New_York";
        let mut file = File::open(filename).unwrap();
        let mut buffer = String::new();
        file.read_to_string(&mut buffer).unwrap();

        let mut rdr = csv::ReaderBuilder::new()
            .flexible(true)
            .from_reader(buffer.as_bytes());
        // get only the rows with data
        let rows: Vec<StringRecord> = rdr
            .records()
            .filter(|x| x.as_ref().unwrap().get(0) == Some("D"))
            .map(|x| x.unwrap())
            .collect();
        let report_date = date(2024, 10, 19);
        let future_dates: Vec<Date> = rows[1]
            .iter()
            .skip(3)
            .map(|e| Date::strptime("%m/%d/%Y", e).unwrap())
            .collect();

        for row in rows.iter().skip(2) {
            let n = row.len();
            let hour = row.get(2).unwrap().parse::<i8>().expect("hour value") - 1;
            for j in 3..n {
                if row.get(j) == Some("") {
                    continue;
                }
                let forecast_hour_beginning =
                    future_dates[j - 3].at(hour, 0, 0, 0).intz(timezone_name).unwrap();
                let forecast_generation = row.get(j).unwrap().parse::<usize>().expect("generation value");    
                let one = Row {report_date, forecast_hour_beginning, forecast_generation};
                println!("{:?}", one);
            }
        }
    }

    /// https://crates.io/crates/reqwest_cookie_store
    fn download_days(days: Vec<Date>) -> Result<(), Error> {
        let client = Client::builder()
            .cookie_store(true)
            .user_agent("Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36")
            .build()?;
        for day in days.into_iter() {
            let yyyymmdd = day.to_string().replace('-', "");
            let url = "https://www.iso-ne.com/transform/csv/sphf?start=".to_string() + &yyyymmdd;
            println!("url:{}", url);
            // let response = get(url).expect("request failed");
            // let body = response.text().expect("body invalid");
            // let mut out = File::create(SevendaySolarForecastArchive::filename(day)).expect("failed to create file");
            // io::copy(&mut body.as_bytes(), &mut out).expect("failed to copy content");

            // NEED to set a cookie with the right token.
            // Not sure how long it will be valid (about 1 day)
            let response = client
                .get(url)
                // .header(DNT, 1)
                // .header(ACCESS_CONTROL_ALLOW_ORIGIN, "*")
                // .header(ACCESS_CONTROL_ALLOW_CREDENTIALS, "cross-origin")
                .header(COOKIE, "isox_token=\"8C7Vl2RfY4h/S49Vg4E5lYcfAGfy/S+CWLiP3rvk5pjXqYineAJYPgKT63zIUYSG43m7y0tKtI555aRc49hgHHuRfy1I58blzu5P4yfSdanJmn+AQAGUhvG+GbCtxQubJAHqiRhjL1Tcdy2KJCNIxb6uBNCD/XyMNhlyFWmm2+k=\"")
                // .header(UPGRADE_INSECURE_REQUESTS, 1)
                .send();
            // println!("{:?}", response.as_ref().unwrap().headers().get_all(SET_COOKIE));

            // response.inspect(|e| e.headers().get_all(SET_COOKIE));

            if let Ok(res) = response {
                if let Ok(data) = res.text() {
                    fs::write(SevendaySolarForecastArchive::filename(day), data)
                        .expect("Writing to file failed");
                }
            }
        }

        Ok(())
    }
}

// struct DailyForecast {
//     day_index: u8,
//     market_date: NaiveDate,
//     cso_mw: f32,
//     cold_weather_outages_mw: f32,
//     other_outages_mw: f32,
//     delist_mw: f32,
//     total_available_gen_mw: f32,
//     peak_import_mw: f32,
//     total_available_gen_import_mw: f32,
//     peak_load: f32,
//     replacement_reserve_requirement_mw: f32,
//     required_reserve_mw: f32,
//     required_reserve_incl_repl_mw: f32,
//     total_load_plus_required_reserve_mw: f32,
//     drr_mw: f32,
//     surplus_deficiency_mw: f32,
//     is_power_watch: bool,
//     is_power_warn: bool,
//     is_cold_weather_watch: bool,
//     is_cold_weather_warn: bool,
//     is_cold_weather_event: bool,
//     bos_high_temp: f32,
//     bos_dew_point: f32,
//     bdl_high_temp: f32,
//     bdl_dew_point: f32,
// }

#[cfg(test)]
mod tests {
    use jiff::civil::date;
    use reqwest::Error;
    use serde_json::Value;
    use std::fs;

    use super::*;

    #[test]
    fn download_day() -> Result<(), Error> {
        SevendaySolarForecastArchive::download_days(vec![date(2024, 10, 19)])
    }

    #[test]
    fn read_file() -> Result<(), String> {
        let path = SevendaySolarForecastArchive::filename(date(2024, 10, 19));
        let out = SevendaySolarForecastArchive::read_file(path);

        Ok(())
    }
}
