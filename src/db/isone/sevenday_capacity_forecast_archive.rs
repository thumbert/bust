use jiff::civil::Date;
use log::{error, info};
use std::error::Error;
use std::process::Command;
use std::path::Path;

use crate::interval::month::Month;

pub struct SevendayCapacityForecastArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl SevendayCapacityForecastArchive {
    pub fn filename(&self, date: &Date) -> String {
        self.base_dir.clone()
            + "/Raw/"
            + date.year().to_string().as_str()
            + "/7dayforecast_"
            + &date.to_string()
            + ".json"
    }

    /// Upload one month to DuckDB.
    pub fn update_duckdb(&self, month: &Month) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting 7 day capacity forecast files for month {} ...",
            month
        );
        let create_stmt = r#"
CREATE TABLE IF NOT EXISTS capacity_forecast (
    for_day DATE NOT NULL,
    day_index UINT8 NOT NULL,
    cso_mw INT,
    cold_weather_outages_mw INT,
    other_gen_outages_mw INT,
    delist_mw INT,
    total_available_gen_mw INT,
    peak_import_mw INT,
    total_available_gen_import_mw INT,
    peak_load_mw INT,
    replacement_reserve_req_mw INT,
    required_reserve_mw INT,
    required_reserve_incl_replacement_mw INT,
    total_load_plus_required_reserve_mw INT,
    drr_mw INT,
    surplus_deficiency_mw INT,
    is_power_watch BOOLEAN,
    is_power_warn BOOLEAN, 
    is_cold_weather_watch BOOLEAN,
    is_cold_weather_warn BOOLEAN,
    is_cold_weather_event BOOLEAN,
    boston_high_temp_f INT1,
    boston_dew_point_f INT1,
    hartford_high_temp_f INT1,
    hartford_dew_point_f INT1,
);
        "#;

        // ISO changed the file format on 6/17/2024!
        let create_tmp = if month < &"2024-06".parse::<Month>().unwrap()
            && month >= &"2023-01".parse::<Month>().unwrap()
        {
            format!(
                r#"
CREATE TEMPORARY TABLE tmp
AS
    PIVOT (
        SELECT 
            * EXCLUDE (city_weather),
            CAST(city_weather ->> 'CityName' AS STRING) AS city_name,
            CAST(city_weather -> 'HighTempF' AS INT1) AS high_temp_f,
            CAST(city_weather -> 'DewPointF' AS INT1) AS dew_point_f
        FROM (
        SELECT 
            CAST(aux -> 'MarketDate' AS DATE) AS for_day,
            CAST(aux -> '@Day' AS INTEGER) AS day_index,
            CAST(aux -> 'CsoMw' AS INTEGER) AS cso_mw,
            CAST(aux -> 'ColdWeatherOutagesMw' AS INTEGER) AS cold_weather_outages_mw,
            CAST(aux -> 'OtherGenOutagesMw' AS INTEGER) AS other_gen_outages_mw,
            CAST(aux -> 'DelistMw' AS INTEGER) AS delist_mw,
            CAST(aux -> 'TotAvailGenMw' AS INTEGER) AS total_available_gen_mw,
            CAST(aux -> 'PeakImportMw' AS INTEGER) AS peak_import_mw,    
            CAST(aux -> 'TotAvailGenImportMw' AS INTEGER) AS total_available_gen_import_mw,
            CAST(aux -> 'PeakLoadMw' AS INTEGER) AS peak_load_mw,
            CAST(aux -> 'ReplReserveReqMw' AS INTEGER) AS replacement_reserve_req_mw,
            CAST(aux -> 'ReqdReserveMw' AS INTEGER) AS required_reserve_mw,
            CAST(aux -> 'ReqdReserveInclReplMw' AS INTEGER) AS required_reserve_incl_replacement_mw,
            CAST(aux -> 'TotLoadPlusReqdReserveMw' AS INTEGER) AS total_load_plus_required_reserve_mw,
            CAST(aux -> 'DrrMw' AS INTEGER) AS drr_mw,
            CAST(aux -> 'SurplusDeficiencyMw' AS INTEGER) AS surplus_deficiency_mw,
            CASE 
                WHEN aux ->> 'PowerWatch' = 'Y' THEN TRUE 
                WHEN aux ->> 'PowerWatch' = 'N' THEN FALSE
                ELSE NULL 
                END AS is_power_watch,
            CASE 
                WHEN aux ->> 'PowerWarn' = 'Y' THEN TRUE 
                WHEN aux ->> 'PowerWarn' = 'N' THEN FALSE        
                ELSE NULL 
                END AS is_power_warn,
            CASE 
                WHEN aux ->> 'ColdWeatherWatch' = 'Y' THEN TRUE 
                WHEN aux ->> 'ColdWeatherWatch' = 'N' THEN FALSE
                ELSE NULL 
                END AS is_cold_weather_watch,
            CASE 
                WHEN aux ->> 'ColdWeatherWarn' = 'Y' THEN TRUE 
                WHEN aux ->> 'ColdWeatherWarn' = 'N' THEN FALSE
                ELSE NULL 
                END AS is_cold_weather_warn,
            CASE 
                WHEN aux ->> 'ColdWeatherEvent' = 'Y' THEN TRUE 
                WHEN aux ->> 'ColdWeatherEvent' = 'N' THEN FALSE
                ELSE NULL    
                END AS is_cold_weather_event,
            unnest(aux -> '$.Weather' -> '$.CityWeather[*]', recursive := true) AS city_weather,  
            FROM (
                SELECT 
                    unnest(CAST(sevendayforecasts.Sevendayforecast AS JSON) -> '$[0]' -> '$.MarketDay[*]') as aux
                FROM read_json('{}/Raw/{}/7dayforecast_{}-*.json.gz')
            )
        ) 
    ) ON city_name 
    USING 
        FIRST(high_temp_f) as high_temp_f, 
        FIRST(dew_point_f) as dew_point_f
    ORDER BY for_day    
;        
        "#,
                self.base_dir,
                month.start_date().year(),
                month.start_date().strftime("%Y-%m")
            )
        } else {
            format!(
                r#"
CREATE TEMPORARY TABLE tmp
AS
    PIVOT (
        SELECT 
            * EXCLUDE (city_weather),
            CAST(city_weather ->> 'CityName' AS STRING) AS city_name,
            CAST(city_weather -> 'HighTempF' AS INT1) AS high_temp_f,
            CAST(city_weather -> 'DewPointF' AS INT1) AS dew_point_f
        FROM (
        SELECT 
            CAST(aux -> 'MarketDate' AS DATE) AS for_day,
            CAST(aux -> 'Day' AS INTEGER) AS day_index,
            CAST(aux -> 'CsoMw' AS INTEGER) AS cso_mw,
            CAST(aux -> 'ColdWeatherOutagesMw' AS INTEGER) AS cold_weather_outages_mw,
            CAST(aux -> 'OtherGenOutagesMw' AS INTEGER) AS other_gen_outages_mw,
            CAST(aux -> 'DelistMw' AS INTEGER) AS delist_mw,
            CAST(aux -> 'TotAvailGenMw' AS INTEGER) AS total_available_gen_mw,
            CAST(aux -> 'PeakImportMw' AS INTEGER) AS peak_import_mw,    
            CAST(aux -> 'TotAvailGenImportMw' AS INTEGER) AS total_available_gen_import_mw,
            CAST(aux -> 'PeakLoadMw' AS INTEGER) AS peak_load_mw,
            CAST(aux -> 'ReplReserveReqMw' AS INTEGER) AS replacement_reserve_req_mw,
            CAST(aux -> 'ReqdReserveMw' AS INTEGER) AS required_reserve_mw,
            CAST(aux -> 'ReqdReserveInclReplMw' AS INTEGER) AS required_reserve_incl_replacement_mw,
            CAST(aux -> 'TotLoadPlusReqdReserveMw' AS INTEGER) AS total_load_plus_required_reserve_mw,
            CAST(aux -> 'DrrMw' AS INTEGER) AS drr_mw,
            CAST(aux -> 'SurplusDeficiencyMw' AS INTEGER) AS surplus_deficiency_mw,
            CASE 
                WHEN aux ->> 'PowerWatch' = 'Y' THEN TRUE 
                WHEN aux ->> 'PowerWatch' = 'N' THEN FALSE
                ELSE NULL 
                END AS is_power_watch,
            CASE 
                WHEN aux ->> 'PowerWarn' = 'Y' THEN TRUE 
                WHEN aux ->> 'PowerWarn' = 'N' THEN FALSE        
                ELSE NULL 
                END AS is_power_warn,
            CASE 
                WHEN aux ->> 'ColdWeatherWatch' = 'Y' THEN TRUE 
                WHEN aux ->> 'ColdWeatherWatch' = 'N' THEN FALSE
                ELSE NULL 
                END AS is_cold_weather_watch,
            CASE 
                WHEN aux ->> 'ColdWeatherWarn' = 'Y' THEN TRUE 
                WHEN aux ->> 'ColdWeatherWarn' = 'N' THEN FALSE
                ELSE NULL 
                END AS is_cold_weather_warn,
            CASE 
                WHEN aux ->> 'ColdWeatherEvent' = 'Y' THEN TRUE 
                WHEN aux ->> 'ColdWeatherEvent' = 'N' THEN FALSE
                ELSE NULL    
                END AS is_cold_weather_event,
            unnest(aux -> '$.Weather' -> '$.CityWeather[*]', recursive := true) AS city_weather,  
            FROM (
                SELECT 
                    unnest(CAST(sevendayforecasts.Sevendayforecast AS JSON) -> '$[0]' -> '$.MarketDay[*]') as aux
                FROM read_json('{}/Raw/{}/7dayforecast_{}-*.json.gz')
            )
        ) 
    ) ON city_name 
    USING 
        FIRST(high_temp_f) as high_temp_f, 
        FIRST(dew_point_f) as dew_point_f
    ORDER BY for_day    
;        
        "#,
                self.base_dir,
                month.start_date().year(),
                month.start_date().strftime("%Y-%m"),
            )
        };

        let sql = format!(
            r#"
        {}

        {}

INSERT INTO capacity_forecast
    SELECT * FROM tmp
EXCEPT
    SELECT * FROM capacity_forecast
ORDER BY for_day, day_index;
"#,
            create_stmt, create_tmp,
        );
        // println!("{}", sql);

        let output = Command::new("duckdb")
            .arg("-c")
            .arg(&sql)
            .arg(&self.duckdb_path)
            .output()
            .expect("Failed to invoke duckdb command");

        let stdout = String::from_utf8_lossy(&output.stdout);
        let stderr = String::from_utf8_lossy(&output.stderr);
        if output.status.success() {
            info!("{}", stdout);
            info!("done");
        } else {
            error!("Failed to update duckdb for month {}: {}", month, stderr);
        }

        Ok(())
    }

    pub fn download_file(&self, day: Date) -> Result<(), Box<dyn Error>> {
        let yyyymmdd = day.strftime("%Y%m%d").to_string();
        super::lib_isoexpress::download_file(
            "https://webservices.iso-ne.com/api/v1.1/sevendayforecast/day/".to_string()
                + &yyyymmdd
                + "/all",
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(&day)),
            true,
        )
    }
}

struct DailyForecast {
    day_index: u8,
    market_date: Date,
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
    use jiff::civil::date;
    use log::info;
    use serde_json::Value;
    use std::{error::Error, fs, path::Path};

    use crate::{
        db::prod_db::ProdDb,
        interval::term::Term,
    };

    #[ignore]
    #[test]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_sevenday_capacity_forecast();

        let term = "Jul24-Sep25".parse::<Term>()?;
        for month in term.months() {
            info!("Working on month {}", month);
            archive.update_duckdb(&month)?;
        }
        Ok(())
    }

    #[test]
    fn download_day() -> Result<(), Box<dyn std::error::Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_sevenday_capacity_forecast();
        let days = "17Sep23".parse::<Term>()?.days();
        for day in &days {
            println!("Downloading for day {}", day);
            archive.download_file(*day)?;
        }
        Ok(())
    }

    #[test]
    fn parse_file() -> Result<(), String> {
        let archive = ProdDb::isone_sevenday_capacity_forecast();
        let path = archive.filename(&date(2024, 6, 17));
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
