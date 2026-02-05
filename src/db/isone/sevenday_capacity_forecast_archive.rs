// Auto-generated Rust stub for DuckDB table: capacity_forecast
// Created on 2025-10-31 with elec_server/utils/lib_duckdb_builder.dart

use log::{error, info};
use std::collections::HashMap;
use std::error::Error;
use std::path::Path;
use std::process::Command;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use jiff::Timestamp;
use jiff::{civil::Date, ToSpan};
use jiff::{tz::TimeZone, Zoned};

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
    creation_time TIMESTAMPTZ NOT NULL,
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
            creation_time,
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
                    unnest(CAST(sevendayforecasts.Sevendayforecast AS JSON) -> '$[0]' -> '$.MarketDay[*]') as aux,
                    CAST(CAST(sevendayforecasts.Sevendayforecast AS JSON) -> '$[0]' -> '$.CreationDate' as TIMESTAMPTZ) as creation_time 
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
            creation_time,
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
                    unnest(CAST(sevendayforecasts.Sevendayforecast AS JSON) -> '$[0]' -> '$.MarketDay[*]') as aux,
                    CAST(CAST(sevendayforecasts.Sevendayforecast AS JSON) -> '$[0]' -> '$.CreationDate' as TIMESTAMPTZ) as creation_time 
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

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub creation_time: Zoned,
    pub for_day: Date,
    pub day_index: u8,
    pub cso_mw: Option<i32>,
    pub cold_weather_outages_mw: Option<i32>,
    pub other_gen_outages_mw: Option<i32>,
    pub delist_mw: Option<i32>,
    pub total_available_gen_mw: Option<i32>,
    pub peak_import_mw: Option<i32>,
    pub total_available_gen_import_mw: Option<i32>,
    pub peak_load_mw: Option<i32>,
    pub replacement_reserve_req_mw: Option<i32>,
    pub required_reserve_mw: Option<i32>,
    pub required_reserve_incl_replacement_mw: Option<i32>,
    pub total_load_plus_required_reserve_mw: Option<i32>,
    pub drr_mw: Option<i32>,
    pub surplus_deficiency_mw: Option<i32>,
    pub is_power_watch: Option<bool>,
    pub is_power_warn: Option<bool>,
    pub is_cold_weather_watch: Option<bool>,
    pub is_cold_weather_warn: Option<bool>,
    pub is_cold_weather_event: Option<bool>,
    pub boston_high_temp_f: Option<i8>,
    pub boston_dew_point_f: Option<i8>,
    pub hartford_high_temp_f: Option<i8>,
    pub hartford_dew_point_f: Option<i8>,
}

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
    limit: Option<usize>,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    creation_time,
    for_day,
    day_index,
    cso_mw,
    cold_weather_outages_mw,
    other_gen_outages_mw,
    delist_mw,
    total_available_gen_mw,
    peak_import_mw,
    total_available_gen_import_mw,
    peak_load_mw,
    replacement_reserve_req_mw,
    required_reserve_mw,
    required_reserve_incl_replacement_mw,
    total_load_plus_required_reserve_mw,
    drr_mw,
    surplus_deficiency_mw,
    is_power_watch,
    is_power_warn,
    is_cold_weather_watch,
    is_cold_weather_warn,
    is_cold_weather_event,
    boston_high_temp_f,
    boston_dew_point_f,
    hartford_high_temp_f,
    hartford_dew_point_f
FROM capacity_forecast WHERE 1=1"#,
    );
    if let Some(creation_time) = &query_filter.creation_time {
        query.push_str(&format!(
            "
    AND creation_time = '{}'",
            creation_time.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(creation_time_gte) = &query_filter.creation_time_gte {
        query.push_str(&format!(
            "
    AND creation_time >= '{}'",
            creation_time_gte.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(creation_time_lt) = &query_filter.creation_time_lt {
        query.push_str(&format!(
            "
    AND creation_time < '{}'",
            creation_time_lt.strftime("%Y-%m-%d %H:%M:%S.000%:z")
        ));
    }
    if let Some(for_day) = &query_filter.for_day {
        query.push_str(&format!(
            "
    AND for_day = '{}'",
            for_day
        ));
    }
    if let Some(for_day_in) = &query_filter.for_day_in {
        query.push_str(&format!(
            "
    AND for_day IN ('{}')",
            for_day_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(for_day_gte) = &query_filter.for_day_gte {
        query.push_str(&format!(
            "
    AND for_day >= '{}'",
            for_day_gte
        ));
    }
    if let Some(for_day_lte) = &query_filter.for_day_lte {
        query.push_str(&format!(
            "
    AND for_day <= '{}'",
            for_day_lte
        ));
    }
    if let Some(day_index) = &query_filter.day_index {
        query.push_str(&format!(
            "
    AND day_index = {}",
            day_index
        ));
    }
    if let Some(day_index_in) = &query_filter.day_index_in {
        query.push_str(&format!(
            "
    AND day_index IN ({})",
            day_index_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(day_index_gte) = &query_filter.day_index_gte {
        query.push_str(&format!(
            "
    AND day_index >= {}",
            day_index_gte
        ));
    }
    if let Some(day_index_lte) = &query_filter.day_index_lte {
        query.push_str(&format!(
            "
    AND day_index <= {}",
            day_index_lte
        ));
    }
    if let Some(cso_mw) = &query_filter.cso_mw {
        query.push_str(&format!(
            "
    AND cso_mw = {}",
            cso_mw
        ));
    }
    if let Some(cso_mw_in) = &query_filter.cso_mw_in {
        query.push_str(&format!(
            "
    AND cso_mw IN ({})",
            cso_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(cso_mw_gte) = &query_filter.cso_mw_gte {
        query.push_str(&format!(
            "
    AND cso_mw >= {}",
            cso_mw_gte
        ));
    }
    if let Some(cso_mw_lte) = &query_filter.cso_mw_lte {
        query.push_str(&format!(
            "
    AND cso_mw <= {}",
            cso_mw_lte
        ));
    }
    if let Some(cold_weather_outages_mw) = &query_filter.cold_weather_outages_mw {
        query.push_str(&format!(
            "
    AND cold_weather_outages_mw = {}",
            cold_weather_outages_mw
        ));
    }
    if let Some(cold_weather_outages_mw_in) = &query_filter.cold_weather_outages_mw_in {
        query.push_str(&format!(
            "
    AND cold_weather_outages_mw IN ({})",
            cold_weather_outages_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(cold_weather_outages_mw_gte) = &query_filter.cold_weather_outages_mw_gte {
        query.push_str(&format!(
            "
    AND cold_weather_outages_mw >= {}",
            cold_weather_outages_mw_gte
        ));
    }
    if let Some(cold_weather_outages_mw_lte) = &query_filter.cold_weather_outages_mw_lte {
        query.push_str(&format!(
            "
    AND cold_weather_outages_mw <= {}",
            cold_weather_outages_mw_lte
        ));
    }
    if let Some(other_gen_outages_mw) = &query_filter.other_gen_outages_mw {
        query.push_str(&format!(
            "
    AND other_gen_outages_mw = {}",
            other_gen_outages_mw
        ));
    }
    if let Some(other_gen_outages_mw_in) = &query_filter.other_gen_outages_mw_in {
        query.push_str(&format!(
            "
    AND other_gen_outages_mw IN ({})",
            other_gen_outages_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(other_gen_outages_mw_gte) = &query_filter.other_gen_outages_mw_gte {
        query.push_str(&format!(
            "
    AND other_gen_outages_mw >= {}",
            other_gen_outages_mw_gte
        ));
    }
    if let Some(other_gen_outages_mw_lte) = &query_filter.other_gen_outages_mw_lte {
        query.push_str(&format!(
            "
    AND other_gen_outages_mw <= {}",
            other_gen_outages_mw_lte
        ));
    }
    if let Some(delist_mw) = &query_filter.delist_mw {
        query.push_str(&format!(
            "
    AND delist_mw = {}",
            delist_mw
        ));
    }
    if let Some(delist_mw_in) = &query_filter.delist_mw_in {
        query.push_str(&format!(
            "
    AND delist_mw IN ({})",
            delist_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(delist_mw_gte) = &query_filter.delist_mw_gte {
        query.push_str(&format!(
            "
    AND delist_mw >= {}",
            delist_mw_gte
        ));
    }
    if let Some(delist_mw_lte) = &query_filter.delist_mw_lte {
        query.push_str(&format!(
            "
    AND delist_mw <= {}",
            delist_mw_lte
        ));
    }
    if let Some(total_available_gen_mw) = &query_filter.total_available_gen_mw {
        query.push_str(&format!(
            "
    AND total_available_gen_mw = {}",
            total_available_gen_mw
        ));
    }
    if let Some(total_available_gen_mw_in) = &query_filter.total_available_gen_mw_in {
        query.push_str(&format!(
            "
    AND total_available_gen_mw IN ({})",
            total_available_gen_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(total_available_gen_mw_gte) = &query_filter.total_available_gen_mw_gte {
        query.push_str(&format!(
            "
    AND total_available_gen_mw >= {}",
            total_available_gen_mw_gte
        ));
    }
    if let Some(total_available_gen_mw_lte) = &query_filter.total_available_gen_mw_lte {
        query.push_str(&format!(
            "
    AND total_available_gen_mw <= {}",
            total_available_gen_mw_lte
        ));
    }
    if let Some(peak_import_mw) = &query_filter.peak_import_mw {
        query.push_str(&format!(
            "
    AND peak_import_mw = {}",
            peak_import_mw
        ));
    }
    if let Some(peak_import_mw_in) = &query_filter.peak_import_mw_in {
        query.push_str(&format!(
            "
    AND peak_import_mw IN ({})",
            peak_import_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(peak_import_mw_gte) = &query_filter.peak_import_mw_gte {
        query.push_str(&format!(
            "
    AND peak_import_mw >= {}",
            peak_import_mw_gte
        ));
    }
    if let Some(peak_import_mw_lte) = &query_filter.peak_import_mw_lte {
        query.push_str(&format!(
            "
    AND peak_import_mw <= {}",
            peak_import_mw_lte
        ));
    }
    if let Some(total_available_gen_import_mw) = &query_filter.total_available_gen_import_mw {
        query.push_str(&format!(
            "
    AND total_available_gen_import_mw = {}",
            total_available_gen_import_mw
        ));
    }
    if let Some(total_available_gen_import_mw_in) = &query_filter.total_available_gen_import_mw_in {
        query.push_str(&format!(
            "
    AND total_available_gen_import_mw IN ({})",
            total_available_gen_import_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(total_available_gen_import_mw_gte) = &query_filter.total_available_gen_import_mw_gte
    {
        query.push_str(&format!(
            "
    AND total_available_gen_import_mw >= {}",
            total_available_gen_import_mw_gte
        ));
    }
    if let Some(total_available_gen_import_mw_lte) = &query_filter.total_available_gen_import_mw_lte
    {
        query.push_str(&format!(
            "
    AND total_available_gen_import_mw <= {}",
            total_available_gen_import_mw_lte
        ));
    }
    if let Some(peak_load_mw) = &query_filter.peak_load_mw {
        query.push_str(&format!(
            "
    AND peak_load_mw = {}",
            peak_load_mw
        ));
    }
    if let Some(peak_load_mw_in) = &query_filter.peak_load_mw_in {
        query.push_str(&format!(
            "
    AND peak_load_mw IN ({})",
            peak_load_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(peak_load_mw_gte) = &query_filter.peak_load_mw_gte {
        query.push_str(&format!(
            "
    AND peak_load_mw >= {}",
            peak_load_mw_gte
        ));
    }
    if let Some(peak_load_mw_lte) = &query_filter.peak_load_mw_lte {
        query.push_str(&format!(
            "
    AND peak_load_mw <= {}",
            peak_load_mw_lte
        ));
    }
    if let Some(replacement_reserve_req_mw) = &query_filter.replacement_reserve_req_mw {
        query.push_str(&format!(
            "
    AND replacement_reserve_req_mw = {}",
            replacement_reserve_req_mw
        ));
    }
    if let Some(replacement_reserve_req_mw_in) = &query_filter.replacement_reserve_req_mw_in {
        query.push_str(&format!(
            "
    AND replacement_reserve_req_mw IN ({})",
            replacement_reserve_req_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(replacement_reserve_req_mw_gte) = &query_filter.replacement_reserve_req_mw_gte {
        query.push_str(&format!(
            "
    AND replacement_reserve_req_mw >= {}",
            replacement_reserve_req_mw_gte
        ));
    }
    if let Some(replacement_reserve_req_mw_lte) = &query_filter.replacement_reserve_req_mw_lte {
        query.push_str(&format!(
            "
    AND replacement_reserve_req_mw <= {}",
            replacement_reserve_req_mw_lte
        ));
    }
    if let Some(required_reserve_mw) = &query_filter.required_reserve_mw {
        query.push_str(&format!(
            "
    AND required_reserve_mw = {}",
            required_reserve_mw
        ));
    }
    if let Some(required_reserve_mw_in) = &query_filter.required_reserve_mw_in {
        query.push_str(&format!(
            "
    AND required_reserve_mw IN ({})",
            required_reserve_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(required_reserve_mw_gte) = &query_filter.required_reserve_mw_gte {
        query.push_str(&format!(
            "
    AND required_reserve_mw >= {}",
            required_reserve_mw_gte
        ));
    }
    if let Some(required_reserve_mw_lte) = &query_filter.required_reserve_mw_lte {
        query.push_str(&format!(
            "
    AND required_reserve_mw <= {}",
            required_reserve_mw_lte
        ));
    }
    if let Some(required_reserve_incl_replacement_mw) =
        &query_filter.required_reserve_incl_replacement_mw
    {
        query.push_str(&format!(
            "
    AND required_reserve_incl_replacement_mw = {}",
            required_reserve_incl_replacement_mw
        ));
    }
    if let Some(required_reserve_incl_replacement_mw_in) =
        &query_filter.required_reserve_incl_replacement_mw_in
    {
        query.push_str(&format!(
            "
    AND required_reserve_incl_replacement_mw IN ({})",
            required_reserve_incl_replacement_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(required_reserve_incl_replacement_mw_gte) =
        &query_filter.required_reserve_incl_replacement_mw_gte
    {
        query.push_str(&format!(
            "
    AND required_reserve_incl_replacement_mw >= {}",
            required_reserve_incl_replacement_mw_gte
        ));
    }
    if let Some(required_reserve_incl_replacement_mw_lte) =
        &query_filter.required_reserve_incl_replacement_mw_lte
    {
        query.push_str(&format!(
            "
    AND required_reserve_incl_replacement_mw <= {}",
            required_reserve_incl_replacement_mw_lte
        ));
    }
    if let Some(total_load_plus_required_reserve_mw) =
        &query_filter.total_load_plus_required_reserve_mw
    {
        query.push_str(&format!(
            "
    AND total_load_plus_required_reserve_mw = {}",
            total_load_plus_required_reserve_mw
        ));
    }
    if let Some(total_load_plus_required_reserve_mw_in) =
        &query_filter.total_load_plus_required_reserve_mw_in
    {
        query.push_str(&format!(
            "
    AND total_load_plus_required_reserve_mw IN ({})",
            total_load_plus_required_reserve_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(total_load_plus_required_reserve_mw_gte) =
        &query_filter.total_load_plus_required_reserve_mw_gte
    {
        query.push_str(&format!(
            "
    AND total_load_plus_required_reserve_mw >= {}",
            total_load_plus_required_reserve_mw_gte
        ));
    }
    if let Some(total_load_plus_required_reserve_mw_lte) =
        &query_filter.total_load_plus_required_reserve_mw_lte
    {
        query.push_str(&format!(
            "
    AND total_load_plus_required_reserve_mw <= {}",
            total_load_plus_required_reserve_mw_lte
        ));
    }
    if let Some(drr_mw) = &query_filter.drr_mw {
        query.push_str(&format!(
            "
    AND drr_mw = {}",
            drr_mw
        ));
    }
    if let Some(drr_mw_in) = &query_filter.drr_mw_in {
        query.push_str(&format!(
            "
    AND drr_mw IN ({})",
            drr_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(drr_mw_gte) = &query_filter.drr_mw_gte {
        query.push_str(&format!(
            "
    AND drr_mw >= {}",
            drr_mw_gte
        ));
    }
    if let Some(drr_mw_lte) = &query_filter.drr_mw_lte {
        query.push_str(&format!(
            "
    AND drr_mw <= {}",
            drr_mw_lte
        ));
    }
    if let Some(surplus_deficiency_mw) = &query_filter.surplus_deficiency_mw {
        query.push_str(&format!(
            "
    AND surplus_deficiency_mw = {}",
            surplus_deficiency_mw
        ));
    }
    if let Some(surplus_deficiency_mw_in) = &query_filter.surplus_deficiency_mw_in {
        query.push_str(&format!(
            "
    AND surplus_deficiency_mw IN ({})",
            surplus_deficiency_mw_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(surplus_deficiency_mw_gte) = &query_filter.surplus_deficiency_mw_gte {
        query.push_str(&format!(
            "
    AND surplus_deficiency_mw >= {}",
            surplus_deficiency_mw_gte
        ));
    }
    if let Some(surplus_deficiency_mw_lte) = &query_filter.surplus_deficiency_mw_lte {
        query.push_str(&format!(
            "
    AND surplus_deficiency_mw <= {}",
            surplus_deficiency_mw_lte
        ));
    }
    if let Some(is_power_watch) = &query_filter.is_power_watch {
        query.push_str(&format!(
            "
    AND is_power_watch = {}",
            is_power_watch
        ));
    }
    if let Some(is_power_warn) = &query_filter.is_power_warn {
        query.push_str(&format!(
            "
    AND is_power_warn = {}",
            is_power_warn
        ));
    }
    if let Some(is_cold_weather_watch) = &query_filter.is_cold_weather_watch {
        query.push_str(&format!(
            "
    AND is_cold_weather_watch = {}",
            is_cold_weather_watch
        ));
    }
    if let Some(is_cold_weather_warn) = &query_filter.is_cold_weather_warn {
        query.push_str(&format!(
            "
    AND is_cold_weather_warn = {}",
            is_cold_weather_warn
        ));
    }
    if let Some(is_cold_weather_event) = &query_filter.is_cold_weather_event {
        query.push_str(&format!(
            "
    AND is_cold_weather_event = {}",
            is_cold_weather_event
        ));
    }
    if let Some(boston_high_temp_f) = &query_filter.boston_high_temp_f {
        query.push_str(&format!(
            "
    AND boston_high_temp_f = {}",
            boston_high_temp_f
        ));
    }
    if let Some(boston_high_temp_f_in) = &query_filter.boston_high_temp_f_in {
        query.push_str(&format!(
            "
    AND boston_high_temp_f IN ({})",
            boston_high_temp_f_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(boston_high_temp_f_gte) = &query_filter.boston_high_temp_f_gte {
        query.push_str(&format!(
            "
    AND boston_high_temp_f >= {}",
            boston_high_temp_f_gte
        ));
    }
    if let Some(boston_high_temp_f_lte) = &query_filter.boston_high_temp_f_lte {
        query.push_str(&format!(
            "
    AND boston_high_temp_f <= {}",
            boston_high_temp_f_lte
        ));
    }
    if let Some(boston_dew_point_f) = &query_filter.boston_dew_point_f {
        query.push_str(&format!(
            "
    AND boston_dew_point_f = {}",
            boston_dew_point_f
        ));
    }
    if let Some(boston_dew_point_f_in) = &query_filter.boston_dew_point_f_in {
        query.push_str(&format!(
            "
    AND boston_dew_point_f IN ({})",
            boston_dew_point_f_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(boston_dew_point_f_gte) = &query_filter.boston_dew_point_f_gte {
        query.push_str(&format!(
            "
    AND boston_dew_point_f >= {}",
            boston_dew_point_f_gte
        ));
    }
    if let Some(boston_dew_point_f_lte) = &query_filter.boston_dew_point_f_lte {
        query.push_str(&format!(
            "
    AND boston_dew_point_f <= {}",
            boston_dew_point_f_lte
        ));
    }
    if let Some(hartford_high_temp_f) = &query_filter.hartford_high_temp_f {
        query.push_str(&format!(
            "
    AND hartford_high_temp_f = {}",
            hartford_high_temp_f
        ));
    }
    if let Some(hartford_high_temp_f_in) = &query_filter.hartford_high_temp_f_in {
        query.push_str(&format!(
            "
    AND hartford_high_temp_f IN ({})",
            hartford_high_temp_f_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(hartford_high_temp_f_gte) = &query_filter.hartford_high_temp_f_gte {
        query.push_str(&format!(
            "
    AND hartford_high_temp_f >= {}",
            hartford_high_temp_f_gte
        ));
    }
    if let Some(hartford_high_temp_f_lte) = &query_filter.hartford_high_temp_f_lte {
        query.push_str(&format!(
            "
    AND hartford_high_temp_f <= {}",
            hartford_high_temp_f_lte
        ));
    }
    if let Some(hartford_dew_point_f) = &query_filter.hartford_dew_point_f {
        query.push_str(&format!(
            "
    AND hartford_dew_point_f = {}",
            hartford_dew_point_f
        ));
    }
    if let Some(hartford_dew_point_f_in) = &query_filter.hartford_dew_point_f_in {
        query.push_str(&format!(
            "
    AND hartford_dew_point_f IN ({})",
            hartford_dew_point_f_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(hartford_dew_point_f_gte) = &query_filter.hartford_dew_point_f_gte {
        query.push_str(&format!(
            "
    AND hartford_dew_point_f >= {}",
            hartford_dew_point_f_gte
        ));
    }
    if let Some(hartford_dew_point_f_lte) = &query_filter.hartford_dew_point_f_lte {
        query.push_str(&format!(
            "
    AND hartford_dew_point_f <= {}",
            hartford_dew_point_f_lte
        ));
    }
    match limit {
        Some(l) => {
            query.push_str(&format!(
                "
LIMIT {};",
                l
            ));
        }
        None => {
            query.push(';');
        }
    }

    let mut stmt = conn.prepare(&query)?;
    let rows = stmt.query_map([], |row| {
        let _micros0: i64 = row.get::<usize, i64>(0)?;
        let creation_time = Zoned::new(
            Timestamp::from_microsecond(_micros0).unwrap(),
            TimeZone::get("America/New_York").unwrap(),
        );
        let _n1 = 719528 + row.get::<usize, i32>(1)?;
        let for_day = Date::ZERO + _n1.days();
        let day_index: u8 = row.get::<usize, u8>(2)?;
        let cso_mw: Option<i32> = row.get::<usize, Option<i32>>(3)?;
        let cold_weather_outages_mw: Option<i32> = row.get::<usize, Option<i32>>(4)?;
        let other_gen_outages_mw: Option<i32> = row.get::<usize, Option<i32>>(5)?;
        let delist_mw: Option<i32> = row.get::<usize, Option<i32>>(6)?;
        let total_available_gen_mw: Option<i32> = row.get::<usize, Option<i32>>(7)?;
        let peak_import_mw: Option<i32> = row.get::<usize, Option<i32>>(8)?;
        let total_available_gen_import_mw: Option<i32> = row.get::<usize, Option<i32>>(9)?;
        let peak_load_mw: Option<i32> = row.get::<usize, Option<i32>>(10)?;
        let replacement_reserve_req_mw: Option<i32> = row.get::<usize, Option<i32>>(11)?;
        let required_reserve_mw: Option<i32> = row.get::<usize, Option<i32>>(12)?;
        let required_reserve_incl_replacement_mw: Option<i32> =
            row.get::<usize, Option<i32>>(13)?;
        let total_load_plus_required_reserve_mw: Option<i32> = row.get::<usize, Option<i32>>(14)?;
        let drr_mw: Option<i32> = row.get::<usize, Option<i32>>(15)?;
        let surplus_deficiency_mw: Option<i32> = row.get::<usize, Option<i32>>(16)?;
        let is_power_watch: Option<bool> = row.get::<usize, Option<bool>>(17)?;
        let is_power_warn: Option<bool> = row.get::<usize, Option<bool>>(18)?;
        let is_cold_weather_watch: Option<bool> = row.get::<usize, Option<bool>>(19)?;
        let is_cold_weather_warn: Option<bool> = row.get::<usize, Option<bool>>(20)?;
        let is_cold_weather_event: Option<bool> = row.get::<usize, Option<bool>>(21)?;
        let boston_high_temp_f: Option<i8> = row.get::<usize, Option<i8>>(22)?;
        let boston_dew_point_f: Option<i8> = row.get::<usize, Option<i8>>(23)?;
        let hartford_high_temp_f: Option<i8> = row.get::<usize, Option<i8>>(24)?;
        let hartford_dew_point_f: Option<i8> = row.get::<usize, Option<i8>>(25)?;
        Ok(Record {
            creation_time,
            for_day,
            day_index,
            cso_mw,
            cold_weather_outages_mw,
            other_gen_outages_mw,
            delist_mw,
            total_available_gen_mw,
            peak_import_mw,
            total_available_gen_import_mw,
            peak_load_mw,
            replacement_reserve_req_mw,
            required_reserve_mw,
            required_reserve_incl_replacement_mw,
            total_load_plus_required_reserve_mw,
            drr_mw,
            surplus_deficiency_mw,
            is_power_watch,
            is_power_warn,
            is_cold_weather_watch,
            is_cold_weather_warn,
            is_cold_weather_event,
            boston_high_temp_f,
            boston_dew_point_f,
            hartford_high_temp_f,
            hartford_dew_point_f,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub creation_time: Option<Zoned>,
    pub creation_time_gte: Option<Zoned>,
    pub creation_time_lt: Option<Zoned>,
    pub for_day: Option<Date>,
    pub for_day_in: Option<Vec<Date>>,
    pub for_day_gte: Option<Date>,
    pub for_day_lte: Option<Date>,
    pub day_index: Option<u8>,
    pub day_index_in: Option<Vec<u8>>,
    pub day_index_gte: Option<u8>,
    pub day_index_lte: Option<u8>,
    pub cso_mw: Option<i32>,
    pub cso_mw_in: Option<Vec<i32>>,
    pub cso_mw_gte: Option<i32>,
    pub cso_mw_lte: Option<i32>,
    pub cold_weather_outages_mw: Option<i32>,
    pub cold_weather_outages_mw_in: Option<Vec<i32>>,
    pub cold_weather_outages_mw_gte: Option<i32>,
    pub cold_weather_outages_mw_lte: Option<i32>,
    pub other_gen_outages_mw: Option<i32>,
    pub other_gen_outages_mw_in: Option<Vec<i32>>,
    pub other_gen_outages_mw_gte: Option<i32>,
    pub other_gen_outages_mw_lte: Option<i32>,
    pub delist_mw: Option<i32>,
    pub delist_mw_in: Option<Vec<i32>>,
    pub delist_mw_gte: Option<i32>,
    pub delist_mw_lte: Option<i32>,
    pub total_available_gen_mw: Option<i32>,
    pub total_available_gen_mw_in: Option<Vec<i32>>,
    pub total_available_gen_mw_gte: Option<i32>,
    pub total_available_gen_mw_lte: Option<i32>,
    pub peak_import_mw: Option<i32>,
    pub peak_import_mw_in: Option<Vec<i32>>,
    pub peak_import_mw_gte: Option<i32>,
    pub peak_import_mw_lte: Option<i32>,
    pub total_available_gen_import_mw: Option<i32>,
    pub total_available_gen_import_mw_in: Option<Vec<i32>>,
    pub total_available_gen_import_mw_gte: Option<i32>,
    pub total_available_gen_import_mw_lte: Option<i32>,
    pub peak_load_mw: Option<i32>,
    pub peak_load_mw_in: Option<Vec<i32>>,
    pub peak_load_mw_gte: Option<i32>,
    pub peak_load_mw_lte: Option<i32>,
    pub replacement_reserve_req_mw: Option<i32>,
    pub replacement_reserve_req_mw_in: Option<Vec<i32>>,
    pub replacement_reserve_req_mw_gte: Option<i32>,
    pub replacement_reserve_req_mw_lte: Option<i32>,
    pub required_reserve_mw: Option<i32>,
    pub required_reserve_mw_in: Option<Vec<i32>>,
    pub required_reserve_mw_gte: Option<i32>,
    pub required_reserve_mw_lte: Option<i32>,
    pub required_reserve_incl_replacement_mw: Option<i32>,
    pub required_reserve_incl_replacement_mw_in: Option<Vec<i32>>,
    pub required_reserve_incl_replacement_mw_gte: Option<i32>,
    pub required_reserve_incl_replacement_mw_lte: Option<i32>,
    pub total_load_plus_required_reserve_mw: Option<i32>,
    pub total_load_plus_required_reserve_mw_in: Option<Vec<i32>>,
    pub total_load_plus_required_reserve_mw_gte: Option<i32>,
    pub total_load_plus_required_reserve_mw_lte: Option<i32>,
    pub drr_mw: Option<i32>,
    pub drr_mw_in: Option<Vec<i32>>,
    pub drr_mw_gte: Option<i32>,
    pub drr_mw_lte: Option<i32>,
    pub surplus_deficiency_mw: Option<i32>,
    pub surplus_deficiency_mw_in: Option<Vec<i32>>,
    pub surplus_deficiency_mw_gte: Option<i32>,
    pub surplus_deficiency_mw_lte: Option<i32>,
    pub is_power_watch: Option<bool>,
    pub is_power_warn: Option<bool>,
    pub is_cold_weather_watch: Option<bool>,
    pub is_cold_weather_warn: Option<bool>,
    pub is_cold_weather_event: Option<bool>,
    pub boston_high_temp_f: Option<i8>,
    pub boston_high_temp_f_in: Option<Vec<i8>>,
    pub boston_high_temp_f_gte: Option<i8>,
    pub boston_high_temp_f_lte: Option<i8>,
    pub boston_dew_point_f: Option<i8>,
    pub boston_dew_point_f_in: Option<Vec<i8>>,
    pub boston_dew_point_f_gte: Option<i8>,
    pub boston_dew_point_f_lte: Option<i8>,
    pub hartford_high_temp_f: Option<i8>,
    pub hartford_high_temp_f_in: Option<Vec<i8>>,
    pub hartford_high_temp_f_gte: Option<i8>,
    pub hartford_high_temp_f_lte: Option<i8>,
    pub hartford_dew_point_f: Option<i8>,
    pub hartford_dew_point_f_in: Option<Vec<i8>>,
    pub hartford_dew_point_f_gte: Option<i8>,
    pub hartford_dew_point_f_lte: Option<i8>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.creation_time {
            params.insert("creation_time", value.to_string());
        }
        if let Some(value) = &self.creation_time_gte {
            params.insert("creation_time_gte", value.to_string());
        }
        if let Some(value) = &self.creation_time_lt {
            params.insert("creation_time_lt", value.to_string());
        }
        if let Some(value) = &self.for_day {
            params.insert("for_day", value.to_string());
        }
        if let Some(value) = &self.for_day_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("for_day_in", joined);
        }
        if let Some(value) = &self.for_day_gte {
            params.insert("for_day_gte", value.to_string());
        }
        if let Some(value) = &self.for_day_lte {
            params.insert("for_day_lte", value.to_string());
        }
        if let Some(value) = &self.day_index {
            params.insert("day_index", value.to_string());
        }
        if let Some(value) = &self.day_index_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("day_index_in", joined);
        }
        if let Some(value) = &self.day_index_gte {
            params.insert("day_index_gte", value.to_string());
        }
        if let Some(value) = &self.day_index_lte {
            params.insert("day_index_lte", value.to_string());
        }
        if let Some(value) = &self.cso_mw {
            params.insert("cso_mw", value.to_string());
        }
        if let Some(value) = &self.cso_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("cso_mw_in", joined);
        }
        if let Some(value) = &self.cso_mw_gte {
            params.insert("cso_mw_gte", value.to_string());
        }
        if let Some(value) = &self.cso_mw_lte {
            params.insert("cso_mw_lte", value.to_string());
        }
        if let Some(value) = &self.cold_weather_outages_mw {
            params.insert("cold_weather_outages_mw", value.to_string());
        }
        if let Some(value) = &self.cold_weather_outages_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("cold_weather_outages_mw_in", joined);
        }
        if let Some(value) = &self.cold_weather_outages_mw_gte {
            params.insert("cold_weather_outages_mw_gte", value.to_string());
        }
        if let Some(value) = &self.cold_weather_outages_mw_lte {
            params.insert("cold_weather_outages_mw_lte", value.to_string());
        }
        if let Some(value) = &self.other_gen_outages_mw {
            params.insert("other_gen_outages_mw", value.to_string());
        }
        if let Some(value) = &self.other_gen_outages_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("other_gen_outages_mw_in", joined);
        }
        if let Some(value) = &self.other_gen_outages_mw_gte {
            params.insert("other_gen_outages_mw_gte", value.to_string());
        }
        if let Some(value) = &self.other_gen_outages_mw_lte {
            params.insert("other_gen_outages_mw_lte", value.to_string());
        }
        if let Some(value) = &self.delist_mw {
            params.insert("delist_mw", value.to_string());
        }
        if let Some(value) = &self.delist_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("delist_mw_in", joined);
        }
        if let Some(value) = &self.delist_mw_gte {
            params.insert("delist_mw_gte", value.to_string());
        }
        if let Some(value) = &self.delist_mw_lte {
            params.insert("delist_mw_lte", value.to_string());
        }
        if let Some(value) = &self.total_available_gen_mw {
            params.insert("total_available_gen_mw", value.to_string());
        }
        if let Some(value) = &self.total_available_gen_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("total_available_gen_mw_in", joined);
        }
        if let Some(value) = &self.total_available_gen_mw_gte {
            params.insert("total_available_gen_mw_gte", value.to_string());
        }
        if let Some(value) = &self.total_available_gen_mw_lte {
            params.insert("total_available_gen_mw_lte", value.to_string());
        }
        if let Some(value) = &self.peak_import_mw {
            params.insert("peak_import_mw", value.to_string());
        }
        if let Some(value) = &self.peak_import_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("peak_import_mw_in", joined);
        }
        if let Some(value) = &self.peak_import_mw_gte {
            params.insert("peak_import_mw_gte", value.to_string());
        }
        if let Some(value) = &self.peak_import_mw_lte {
            params.insert("peak_import_mw_lte", value.to_string());
        }
        if let Some(value) = &self.total_available_gen_import_mw {
            params.insert("total_available_gen_import_mw", value.to_string());
        }
        if let Some(value) = &self.total_available_gen_import_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("total_available_gen_import_mw_in", joined);
        }
        if let Some(value) = &self.total_available_gen_import_mw_gte {
            params.insert("total_available_gen_import_mw_gte", value.to_string());
        }
        if let Some(value) = &self.total_available_gen_import_mw_lte {
            params.insert("total_available_gen_import_mw_lte", value.to_string());
        }
        if let Some(value) = &self.peak_load_mw {
            params.insert("peak_load_mw", value.to_string());
        }
        if let Some(value) = &self.peak_load_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("peak_load_mw_in", joined);
        }
        if let Some(value) = &self.peak_load_mw_gte {
            params.insert("peak_load_mw_gte", value.to_string());
        }
        if let Some(value) = &self.peak_load_mw_lte {
            params.insert("peak_load_mw_lte", value.to_string());
        }
        if let Some(value) = &self.replacement_reserve_req_mw {
            params.insert("replacement_reserve_req_mw", value.to_string());
        }
        if let Some(value) = &self.replacement_reserve_req_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("replacement_reserve_req_mw_in", joined);
        }
        if let Some(value) = &self.replacement_reserve_req_mw_gte {
            params.insert("replacement_reserve_req_mw_gte", value.to_string());
        }
        if let Some(value) = &self.replacement_reserve_req_mw_lte {
            params.insert("replacement_reserve_req_mw_lte", value.to_string());
        }
        if let Some(value) = &self.required_reserve_mw {
            params.insert("required_reserve_mw", value.to_string());
        }
        if let Some(value) = &self.required_reserve_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("required_reserve_mw_in", joined);
        }
        if let Some(value) = &self.required_reserve_mw_gte {
            params.insert("required_reserve_mw_gte", value.to_string());
        }
        if let Some(value) = &self.required_reserve_mw_lte {
            params.insert("required_reserve_mw_lte", value.to_string());
        }
        if let Some(value) = &self.required_reserve_incl_replacement_mw {
            params.insert("required_reserve_incl_replacement_mw", value.to_string());
        }
        if let Some(value) = &self.required_reserve_incl_replacement_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("required_reserve_incl_replacement_mw_in", joined);
        }
        if let Some(value) = &self.required_reserve_incl_replacement_mw_gte {
            params.insert(
                "required_reserve_incl_replacement_mw_gte",
                value.to_string(),
            );
        }
        if let Some(value) = &self.required_reserve_incl_replacement_mw_lte {
            params.insert(
                "required_reserve_incl_replacement_mw_lte",
                value.to_string(),
            );
        }
        if let Some(value) = &self.total_load_plus_required_reserve_mw {
            params.insert("total_load_plus_required_reserve_mw", value.to_string());
        }
        if let Some(value) = &self.total_load_plus_required_reserve_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("total_load_plus_required_reserve_mw_in", joined);
        }
        if let Some(value) = &self.total_load_plus_required_reserve_mw_gte {
            params.insert("total_load_plus_required_reserve_mw_gte", value.to_string());
        }
        if let Some(value) = &self.total_load_plus_required_reserve_mw_lte {
            params.insert("total_load_plus_required_reserve_mw_lte", value.to_string());
        }
        if let Some(value) = &self.drr_mw {
            params.insert("drr_mw", value.to_string());
        }
        if let Some(value) = &self.drr_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("drr_mw_in", joined);
        }
        if let Some(value) = &self.drr_mw_gte {
            params.insert("drr_mw_gte", value.to_string());
        }
        if let Some(value) = &self.drr_mw_lte {
            params.insert("drr_mw_lte", value.to_string());
        }
        if let Some(value) = &self.surplus_deficiency_mw {
            params.insert("surplus_deficiency_mw", value.to_string());
        }
        if let Some(value) = &self.surplus_deficiency_mw_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("surplus_deficiency_mw_in", joined);
        }
        if let Some(value) = &self.surplus_deficiency_mw_gte {
            params.insert("surplus_deficiency_mw_gte", value.to_string());
        }
        if let Some(value) = &self.surplus_deficiency_mw_lte {
            params.insert("surplus_deficiency_mw_lte", value.to_string());
        }
        if let Some(value) = &self.is_power_watch {
            params.insert("is_power_watch", value.to_string());
        }
        if let Some(value) = &self.is_power_warn {
            params.insert("is_power_warn", value.to_string());
        }
        if let Some(value) = &self.is_cold_weather_watch {
            params.insert("is_cold_weather_watch", value.to_string());
        }
        if let Some(value) = &self.is_cold_weather_warn {
            params.insert("is_cold_weather_warn", value.to_string());
        }
        if let Some(value) = &self.is_cold_weather_event {
            params.insert("is_cold_weather_event", value.to_string());
        }
        if let Some(value) = &self.boston_high_temp_f {
            params.insert("boston_high_temp_f", value.to_string());
        }
        if let Some(value) = &self.boston_high_temp_f_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("boston_high_temp_f_in", joined);
        }
        if let Some(value) = &self.boston_high_temp_f_gte {
            params.insert("boston_high_temp_f_gte", value.to_string());
        }
        if let Some(value) = &self.boston_high_temp_f_lte {
            params.insert("boston_high_temp_f_lte", value.to_string());
        }
        if let Some(value) = &self.boston_dew_point_f {
            params.insert("boston_dew_point_f", value.to_string());
        }
        if let Some(value) = &self.boston_dew_point_f_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("boston_dew_point_f_in", joined);
        }
        if let Some(value) = &self.boston_dew_point_f_gte {
            params.insert("boston_dew_point_f_gte", value.to_string());
        }
        if let Some(value) = &self.boston_dew_point_f_lte {
            params.insert("boston_dew_point_f_lte", value.to_string());
        }
        if let Some(value) = &self.hartford_high_temp_f {
            params.insert("hartford_high_temp_f", value.to_string());
        }
        if let Some(value) = &self.hartford_high_temp_f_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("hartford_high_temp_f_in", joined);
        }
        if let Some(value) = &self.hartford_high_temp_f_gte {
            params.insert("hartford_high_temp_f_gte", value.to_string());
        }
        if let Some(value) = &self.hartford_high_temp_f_lte {
            params.insert("hartford_high_temp_f_lte", value.to_string());
        }
        if let Some(value) = &self.hartford_dew_point_f {
            params.insert("hartford_dew_point_f", value.to_string());
        }
        if let Some(value) = &self.hartford_dew_point_f_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("hartford_dew_point_f_in", joined);
        }
        if let Some(value) = &self.hartford_dew_point_f_gte {
            params.insert("hartford_dew_point_f_gte", value.to_string());
        }
        if let Some(value) = &self.hartford_dew_point_f_lte {
            params.insert("hartford_dew_point_f_lte", value.to_string());
        }
        form_urlencoded::Serializer::new(String::new())
            .extend_pairs(&params)
            .finish()
    }
}

#[derive(Default)]
pub struct QueryFilterBuilder {
    inner: QueryFilter,
}

impl QueryFilterBuilder {
    pub fn new() -> Self {
        Self {
            inner: QueryFilter::default(),
        }
    }

    pub fn build(self) -> QueryFilter {
        self.inner
    }

    pub fn creation_time(mut self, value: Zoned) -> Self {
        self.inner.creation_time = Some(value);
        self
    }

    pub fn creation_time_gte(mut self, value: Zoned) -> Self {
        self.inner.creation_time_gte = Some(value);
        self
    }

    pub fn creation_time_lt(mut self, value: Zoned) -> Self {
        self.inner.creation_time_lt = Some(value);
        self
    }

    pub fn for_day(mut self, value: Date) -> Self {
        self.inner.for_day = Some(value);
        self
    }

    pub fn for_day_in(mut self, values_in: Vec<Date>) -> Self {
        self.inner.for_day_in = Some(values_in);
        self
    }

    pub fn for_day_gte(mut self, value: Date) -> Self {
        self.inner.for_day_gte = Some(value);
        self
    }

    pub fn for_day_lte(mut self, value: Date) -> Self {
        self.inner.for_day_lte = Some(value);
        self
    }

    pub fn day_index(mut self, value: u8) -> Self {
        self.inner.day_index = Some(value);
        self
    }

    pub fn day_index_in(mut self, values_in: Vec<u8>) -> Self {
        self.inner.day_index_in = Some(values_in);
        self
    }

    pub fn day_index_gte(mut self, value: u8) -> Self {
        self.inner.day_index_gte = Some(value);
        self
    }

    pub fn day_index_lte(mut self, value: u8) -> Self {
        self.inner.day_index_lte = Some(value);
        self
    }

    pub fn cso_mw(mut self, value: i32) -> Self {
        self.inner.cso_mw = Some(value);
        self
    }

    pub fn cso_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.cso_mw_in = Some(values_in);
        self
    }

    pub fn cso_mw_gte(mut self, value: i32) -> Self {
        self.inner.cso_mw_gte = Some(value);
        self
    }

    pub fn cso_mw_lte(mut self, value: i32) -> Self {
        self.inner.cso_mw_lte = Some(value);
        self
    }

    pub fn cold_weather_outages_mw(mut self, value: i32) -> Self {
        self.inner.cold_weather_outages_mw = Some(value);
        self
    }

    pub fn cold_weather_outages_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.cold_weather_outages_mw_in = Some(values_in);
        self
    }

    pub fn cold_weather_outages_mw_gte(mut self, value: i32) -> Self {
        self.inner.cold_weather_outages_mw_gte = Some(value);
        self
    }

    pub fn cold_weather_outages_mw_lte(mut self, value: i32) -> Self {
        self.inner.cold_weather_outages_mw_lte = Some(value);
        self
    }

    pub fn other_gen_outages_mw(mut self, value: i32) -> Self {
        self.inner.other_gen_outages_mw = Some(value);
        self
    }

    pub fn other_gen_outages_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.other_gen_outages_mw_in = Some(values_in);
        self
    }

    pub fn other_gen_outages_mw_gte(mut self, value: i32) -> Self {
        self.inner.other_gen_outages_mw_gte = Some(value);
        self
    }

    pub fn other_gen_outages_mw_lte(mut self, value: i32) -> Self {
        self.inner.other_gen_outages_mw_lte = Some(value);
        self
    }

    pub fn delist_mw(mut self, value: i32) -> Self {
        self.inner.delist_mw = Some(value);
        self
    }

    pub fn delist_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.delist_mw_in = Some(values_in);
        self
    }

    pub fn delist_mw_gte(mut self, value: i32) -> Self {
        self.inner.delist_mw_gte = Some(value);
        self
    }

    pub fn delist_mw_lte(mut self, value: i32) -> Self {
        self.inner.delist_mw_lte = Some(value);
        self
    }

    pub fn total_available_gen_mw(mut self, value: i32) -> Self {
        self.inner.total_available_gen_mw = Some(value);
        self
    }

    pub fn total_available_gen_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.total_available_gen_mw_in = Some(values_in);
        self
    }

    pub fn total_available_gen_mw_gte(mut self, value: i32) -> Self {
        self.inner.total_available_gen_mw_gte = Some(value);
        self
    }

    pub fn total_available_gen_mw_lte(mut self, value: i32) -> Self {
        self.inner.total_available_gen_mw_lte = Some(value);
        self
    }

    pub fn peak_import_mw(mut self, value: i32) -> Self {
        self.inner.peak_import_mw = Some(value);
        self
    }

    pub fn peak_import_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.peak_import_mw_in = Some(values_in);
        self
    }

    pub fn peak_import_mw_gte(mut self, value: i32) -> Self {
        self.inner.peak_import_mw_gte = Some(value);
        self
    }

    pub fn peak_import_mw_lte(mut self, value: i32) -> Self {
        self.inner.peak_import_mw_lte = Some(value);
        self
    }

    pub fn total_available_gen_import_mw(mut self, value: i32) -> Self {
        self.inner.total_available_gen_import_mw = Some(value);
        self
    }

    pub fn total_available_gen_import_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.total_available_gen_import_mw_in = Some(values_in);
        self
    }

    pub fn total_available_gen_import_mw_gte(mut self, value: i32) -> Self {
        self.inner.total_available_gen_import_mw_gte = Some(value);
        self
    }

    pub fn total_available_gen_import_mw_lte(mut self, value: i32) -> Self {
        self.inner.total_available_gen_import_mw_lte = Some(value);
        self
    }

    pub fn peak_load_mw(mut self, value: i32) -> Self {
        self.inner.peak_load_mw = Some(value);
        self
    }

    pub fn peak_load_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.peak_load_mw_in = Some(values_in);
        self
    }

    pub fn peak_load_mw_gte(mut self, value: i32) -> Self {
        self.inner.peak_load_mw_gte = Some(value);
        self
    }

    pub fn peak_load_mw_lte(mut self, value: i32) -> Self {
        self.inner.peak_load_mw_lte = Some(value);
        self
    }

    pub fn replacement_reserve_req_mw(mut self, value: i32) -> Self {
        self.inner.replacement_reserve_req_mw = Some(value);
        self
    }

    pub fn replacement_reserve_req_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.replacement_reserve_req_mw_in = Some(values_in);
        self
    }

    pub fn replacement_reserve_req_mw_gte(mut self, value: i32) -> Self {
        self.inner.replacement_reserve_req_mw_gte = Some(value);
        self
    }

    pub fn replacement_reserve_req_mw_lte(mut self, value: i32) -> Self {
        self.inner.replacement_reserve_req_mw_lte = Some(value);
        self
    }

    pub fn required_reserve_mw(mut self, value: i32) -> Self {
        self.inner.required_reserve_mw = Some(value);
        self
    }

    pub fn required_reserve_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.required_reserve_mw_in = Some(values_in);
        self
    }

    pub fn required_reserve_mw_gte(mut self, value: i32) -> Self {
        self.inner.required_reserve_mw_gte = Some(value);
        self
    }

    pub fn required_reserve_mw_lte(mut self, value: i32) -> Self {
        self.inner.required_reserve_mw_lte = Some(value);
        self
    }

    pub fn required_reserve_incl_replacement_mw(mut self, value: i32) -> Self {
        self.inner.required_reserve_incl_replacement_mw = Some(value);
        self
    }

    pub fn required_reserve_incl_replacement_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.required_reserve_incl_replacement_mw_in = Some(values_in);
        self
    }

    pub fn required_reserve_incl_replacement_mw_gte(mut self, value: i32) -> Self {
        self.inner.required_reserve_incl_replacement_mw_gte = Some(value);
        self
    }

    pub fn required_reserve_incl_replacement_mw_lte(mut self, value: i32) -> Self {
        self.inner.required_reserve_incl_replacement_mw_lte = Some(value);
        self
    }

    pub fn total_load_plus_required_reserve_mw(mut self, value: i32) -> Self {
        self.inner.total_load_plus_required_reserve_mw = Some(value);
        self
    }

    pub fn total_load_plus_required_reserve_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.total_load_plus_required_reserve_mw_in = Some(values_in);
        self
    }

    pub fn total_load_plus_required_reserve_mw_gte(mut self, value: i32) -> Self {
        self.inner.total_load_plus_required_reserve_mw_gte = Some(value);
        self
    }

    pub fn total_load_plus_required_reserve_mw_lte(mut self, value: i32) -> Self {
        self.inner.total_load_plus_required_reserve_mw_lte = Some(value);
        self
    }

    pub fn drr_mw(mut self, value: i32) -> Self {
        self.inner.drr_mw = Some(value);
        self
    }

    pub fn drr_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.drr_mw_in = Some(values_in);
        self
    }

    pub fn drr_mw_gte(mut self, value: i32) -> Self {
        self.inner.drr_mw_gte = Some(value);
        self
    }

    pub fn drr_mw_lte(mut self, value: i32) -> Self {
        self.inner.drr_mw_lte = Some(value);
        self
    }

    pub fn surplus_deficiency_mw(mut self, value: i32) -> Self {
        self.inner.surplus_deficiency_mw = Some(value);
        self
    }

    pub fn surplus_deficiency_mw_in(mut self, values_in: Vec<i32>) -> Self {
        self.inner.surplus_deficiency_mw_in = Some(values_in);
        self
    }

    pub fn surplus_deficiency_mw_gte(mut self, value: i32) -> Self {
        self.inner.surplus_deficiency_mw_gte = Some(value);
        self
    }

    pub fn surplus_deficiency_mw_lte(mut self, value: i32) -> Self {
        self.inner.surplus_deficiency_mw_lte = Some(value);
        self
    }

    pub fn is_power_watch(mut self, value: bool) -> Self {
        self.inner.is_power_watch = Some(value);
        self
    }

    pub fn is_power_warn(mut self, value: bool) -> Self {
        self.inner.is_power_warn = Some(value);
        self
    }

    pub fn is_cold_weather_watch(mut self, value: bool) -> Self {
        self.inner.is_cold_weather_watch = Some(value);
        self
    }

    pub fn is_cold_weather_warn(mut self, value: bool) -> Self {
        self.inner.is_cold_weather_warn = Some(value);
        self
    }

    pub fn is_cold_weather_event(mut self, value: bool) -> Self {
        self.inner.is_cold_weather_event = Some(value);
        self
    }

    pub fn boston_high_temp_f(mut self, value: i8) -> Self {
        self.inner.boston_high_temp_f = Some(value);
        self
    }

    pub fn boston_high_temp_f_in(mut self, values_in: Vec<i8>) -> Self {
        self.inner.boston_high_temp_f_in = Some(values_in);
        self
    }

    pub fn boston_high_temp_f_gte(mut self, value: i8) -> Self {
        self.inner.boston_high_temp_f_gte = Some(value);
        self
    }

    pub fn boston_high_temp_f_lte(mut self, value: i8) -> Self {
        self.inner.boston_high_temp_f_lte = Some(value);
        self
    }

    pub fn boston_dew_point_f(mut self, value: i8) -> Self {
        self.inner.boston_dew_point_f = Some(value);
        self
    }

    pub fn boston_dew_point_f_in(mut self, values_in: Vec<i8>) -> Self {
        self.inner.boston_dew_point_f_in = Some(values_in);
        self
    }

    pub fn boston_dew_point_f_gte(mut self, value: i8) -> Self {
        self.inner.boston_dew_point_f_gte = Some(value);
        self
    }

    pub fn boston_dew_point_f_lte(mut self, value: i8) -> Self {
        self.inner.boston_dew_point_f_lte = Some(value);
        self
    }

    pub fn hartford_high_temp_f(mut self, value: i8) -> Self {
        self.inner.hartford_high_temp_f = Some(value);
        self
    }

    pub fn hartford_high_temp_f_in(mut self, values_in: Vec<i8>) -> Self {
        self.inner.hartford_high_temp_f_in = Some(values_in);
        self
    }

    pub fn hartford_high_temp_f_gte(mut self, value: i8) -> Self {
        self.inner.hartford_high_temp_f_gte = Some(value);
        self
    }

    pub fn hartford_high_temp_f_lte(mut self, value: i8) -> Self {
        self.inner.hartford_high_temp_f_lte = Some(value);
        self
    }

    pub fn hartford_dew_point_f(mut self, value: i8) -> Self {
        self.inner.hartford_dew_point_f = Some(value);
        self
    }

    pub fn hartford_dew_point_f_in(mut self, values_in: Vec<i8>) -> Self {
        self.inner.hartford_dew_point_f_in = Some(values_in);
        self
    }

    pub fn hartford_dew_point_f_gte(mut self, value: i8) -> Self {
        self.inner.hartford_dew_point_f_gte = Some(value);
        self
    }

    pub fn hartford_dew_point_f_lte(mut self, value: i8) -> Self {
        self.inner.hartford_dew_point_f_lte = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::{db::prod_db::ProdDb, interval::term::Term};
    use duckdb::{AccessMode, Config, Connection};
    use jiff::civil::date;
    use std::error::Error;

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(
            ProdDb::isone_sevenday_capacity_forecast().duckdb_path,
            config,
        )
        .unwrap();
        let filter = QueryFilterBuilder::new().for_day(date(2025, 10, 1)).build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 6);
        Ok(())
    }

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
}
