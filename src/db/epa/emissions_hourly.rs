use log::{error, info};
use std::error::Error;
use std::{path::Path, process::Command};

use crate::db::isone::lib_isoexpress::download_file;
use std::collections::HashMap;

use duckdb::Connection;
use serde::{Deserialize, Serialize};
use url::form_urlencoded;

use convert_case::{Case, Casing};
use jiff::{civil::Date, ToSpan};
use rust_decimal::Decimal;
use std::str::FromStr;

/// See https://campd.epa.gov/data/bulk-data-files
/// https://api.epa.gov/easey/bulk-files/emissions/hourly/state/emissions-hourly-2025-ny.csv?
///
///
///
#[derive(Clone)]
pub struct EpaHourlyEmissionsArchive {
    pub state: String,
    pub base_dir: String,
    pub duckdb_path: String,
}

impl EpaHourlyEmissionsArchive {
    fn filename(&self, year: u16) -> String {
        format!(
            "{}/Raw/emissions-hourly-{}-{}.csv",
            self.base_dir,
            year,
            self.state.to_lowercase()
        )
    }

    pub fn download_file(&self, year: u16) -> Result<(), Box<dyn Error>> {
        download_file(
            format!(
                "https://api.epa.gov/easey/bulk-files/emissions/hourly/state/emissions-hourly-{}-{}.csv?",
                year,
                self.state.to_lowercase()
            ),
            false,
            None,
            Path::new(&self.filename(year)),
            true,
        )
    }

    /// Upload one file to DuckDB.
    pub fn update_duckdb(&self, year: u16) -> Result<(), Box<dyn Error>> {
        info!(
            "inserting hourly emissions file for {} year {} ...",
            self.state, year
        );
        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS emissions (
    state VARCHAR(2) NOT NULL,
    facility_name VARCHAR NOT NULL,
    facility_id UINTEGER NOT NULL,
    unit_id VARCHAR,
    associated_stacks VARCHAR,
    date DATE NOT NULL,
    hour UTINYINT NOT NULL,
    -- Fraction of the hour that the unit was operating, from 0 to 1.
    operating_time DECIMAL(3, 2),
    gross_load USMALLINT,
    steam_load FLOAT,
    so2_mass DECIMAL(9, 4),
    so2_mass_measure_indicator ENUM(
        'Calculated',
        'Measured',
        'Substitute',
        'Measured and Substitute',
        'LME', 
        'Other'
    ),
    so2_rate DECIMAL(9, 4),
    so2_rate_measure_indicator ENUM('Calculated'),
    co2_mass DECIMAL(9, 5),
    co2_mass_measure_indicator ENUM(
        'Calculated',
        'Measured',
        'Substitute',
        'Measured and Substitute',
        'LME',
        'Other'
    ),
    co2_rate DECIMAL(9, 6),
    co2_rate_measure_indicator ENUM('Calculated'),
    nox_mass DECIMAL(9, 4),
    nox_mass_measure_indicator ENUM(
        'Calculated', 
        'Measured', 
        'Substitute', 
        'Measured and Substitute',
        'LME',
        'Other'
    ),
    nox_rate DECIMAL(9, 6),
    nox_rate_measure_indicator ENUM(
        'Measured',
        'Substitute',
        'Calculated',
        'Measured and Substitute',
        'LME',
        'Other'
    ),
    heat_input DECIMAL(9, 4),
    heat_input_measure_indicator ENUM(
        'Measured',
        'Substitute',
        'Calculated',
        'Measured and Substitute',
        'LME',
        'Other'
    ),
    primary_fuel_type VARCHAR,
    secondary_fuel_type VARCHAR,
    unit_type ENUM(
        'Arch-fired boiler',
        'Bubbling fluidized bed boiler',
        'Cyclone boiler',
        'Cell burner boiler',
        'Combined cycle',
        'Circulating fluidized bed boiler',
        'Combustion turbine',
        'Dry bottom wall-fired boiler',
        'Dry bottom turbo-fired boiler',
        'Dry bottom vertically-fired boiler',
        'Internal combustion engine',
        'Integrated gasification combined cycle',
        'Cement Kiln',
        'Other boiler',
        'Other turbine',
        'Pressurized fluidized bed boiler',
        'Process Heater',
        'Stoker',
        'Tangentially-fired',
        'Wet bottom wall-fired boiler',
        'Wet bottom turbo-fired boiler',
        'Wet bottom vertically-fired boiler',
    ),
    so2_controls VARCHAR,
    nox_controls VARCHAR,
    pm_controls VARCHAR,
    hg_controls VARCHAR,
    program_code VARCHAR,
);

CREATE TEMPORARY TABLE tmp AS
SELECT * EXCLUDE(
    so2_mass_measure_indicator, 
    so2_rate_measure_indicator,
    co2_mass_measure_indicator, 
    co2_rate_measure_indicator,
    nox_mass_measure_indicator, 
    nox_rate_measure_indicator,
    heat_input_measure_indicator,
    unit_type
    ),  
    CAST( case when so2_mass_measure_indicator = 'CALC' then 'Calculated' 
        when so2_mass_measure_indicator = 'MEASURE' then 'Measured' 
        else so2_mass_measure_indicator end AS ENUM(
        'Calculated',
        'Measured',
        'Substitute',
        'Measured and Substitute',
        'LME',
        'Other'
    )) as so2_mass_measure_indicator,
    CAST( case when so2_rate_measure_indicator = 'CALC' then 'Calculated' 
        else so2_rate_measure_indicator end AS ENUM(
        'Calculated',
    )) as so2_rate_measure_indicator,
    CAST( case when co2_mass_measure_indicator = 'CALC' then 'Calculated' 
        when co2_mass_measure_indicator = 'MEASURE' then 'Measured' 
        else co2_mass_measure_indicator end AS ENUM(
        'Calculated',
        'Measured', 
        'Substitute', 
        'Measured and Substitute',
        'LME',
        'Other'
    )) as co2_mass_measure_indicator,
    CAST( case when co2_rate_measure_indicator = 'CALC' then 'Calculated' 
        else co2_rate_measure_indicator end AS ENUM(
        'Calculated',
    )) as co2_rate_measure_indicator,
    CAST( case when nox_mass_measure_indicator = 'SUB' then 'Substitute' 
        when nox_mass_measure_indicator = 'MEASURE' then 'Measured' 
        when nox_mass_measure_indicator = 'MEASSUB' then 'Measured and Substitute' 
        else nox_mass_measure_indicator end AS ENUM(
        'Calculated', 
        'Measured', 
        'Substitute', 
        'Measured and Substitute',
        'LME',
        'Other'
    )) as nox_mass_measure_indicator, 
    CAST( case when nox_rate_measure_indicator = 'SUB' then 'Substitute' 
        when nox_rate_measure_indicator = 'MEASURE' then 'Measured' 
        else nox_rate_measure_indicator end AS ENUM(
        'Calculated', 
        'Measured', 
        'Substitute', 
        'Measured and Substitute',
        'LME',
        'Other'
    )) as nox_rate_measure_indicator, 
    CAST( case when heat_input_measure_indicator = 'MEASURE' then 'Measured' 
        else heat_input_measure_indicator end AS ENUM(
        'Calculated', 
        'Measured', 
        'Substitute', 
        'Measured and Substitute',
        'LME',
        'Other'
    )) as heat_input_measure_indicator, 
    CAST(case when unit_type  = 'Combustion turbine (Started Jan 12, 2024)' then 'Combustion turbine'
        else unit_type end AS ENUM(
            'Arch-fired boiler',
            'Bubbling fluidized bed boiler',
            'Cyclone boiler',
            'Cell burner boiler',
            'Combined cycle',
            'Circulating fluidized bed boiler',
            'Combustion turbine',
            'Dry bottom wall-fired boiler',
            'Dry bottom turbo-fired boiler',
            'Dry bottom vertically-fired boiler',
            'Internal combustion engine',
            'Integrated gasification combined cycle',
            'Cement Kiln',
            'Other boiler',
            'Other turbine',
            'Pressurized fluidized bed boiler',
            'Process Heater',
            'Stoker',
            'Tangentially-fired',
            'Wet bottom wall-fired boiler',
            'Wet bottom turbo-fired boiler',
            'Wet bottom vertically-fired boiler',
        )) as unit_type,    
FROM (
    SELECT 
        CAST("State" AS VARCHAR(2)) as state,
        CAST("Facility Name" AS VARCHAR) as facility_name,
        CAST("Facility ID" AS UINTEGER) as facility_id,
        CAST("Unit ID" AS VARCHAR) as unit_id,
        CAST("Associated Stacks" AS VARCHAR) as associated_stacks,
        CAST(Date AS DATE) as date,
        CAST(Hour AS UTINYINT) as hour,
        -- Fraction of the hour that the unit was operating, from 0 to 1.
        CAST("Operating Time" AS DECIMAL(3, 2)) as operating_time,
        CAST("Gross Load (MW)" AS USMALLINT) as gross_load,
        CAST("Steam Load (1000 lb/hr)" AS FLOAT) as steam_load,
        CAST("SO2 Mass (lbs)" AS DECIMAL(9, 4)) as so2_mass,
        CAST("SO2 Mass Measure Indicator" AS VARCHAR) as so2_mass_measure_indicator,
        CAST("SO2 Rate (lbs/mmBtu)" AS DECIMAL(9, 4)) as so2_rate,
        CAST("SO2 Rate Measure Indicator" AS VARCHAR) as so2_rate_measure_indicator,
        CAST("CO2 Mass (short tons)" AS DECIMAL(9, 5)) as co2_mass,
        CAST("CO2 Mass Measure Indicator" AS VARCHAR) as co2_mass_measure_indicator,
        CAST("CO2 Rate (short tons/mmBtu)" AS DECIMAL(9, 6)) as co2_rate,
        CAST("CO2 Rate Measure Indicator" AS VARCHAR) as co2_rate_measure_indicator,
        CAST("NOx Mass (lbs)" AS DECIMAL(9, 4)) as nox_mass,
        CAST("NOx Mass Measure Indicator" AS VARCHAR) as nox_mass_measure_indicator,
        CAST("NOx Rate (lbs/mmBtu)" AS DECIMAL(9, 6)) as nox_rate,
        CAST("NOx Rate Measure Indicator" AS VARCHAR) as nox_rate_measure_indicator,
        CAST("Heat Input (mmBtu)" AS DECIMAL(9, 4)) as heat_input,
        CAST("Heat Input Measure Indicator" AS VARCHAR) as heat_input_measure_indicator,
        CAST("Primary Fuel Type" AS VARCHAR) as primary_fuel_type,
        CAST("Secondary Fuel Type" AS VARCHAR) as secondary_fuel_type,
        CAST("Unit Type" AS VARCHAR) as unit_type,    
        CAST("SO2 Controls" AS VARCHAR) as so2_controls,
        CAST("NOx Controls" AS VARCHAR) as nox_controls,
        CAST("PM Controls" AS VARCHAR) as pm_controls,
        CAST("Hg Controls" AS VARCHAR) as hg_controls,
        CAST("Program Code" AS VARCHAR) as program_code
    FROM read_csv(
            '{}/Raw/emissions-hourly-{}-{}.csv.gz',
            header = true,
            types = {{ 'Unit ID': 'VARCHAR' }},
            dateformat = '%Y-%m-%d'
        )
);

INSERT INTO emissions BY NAME
(SELECT * FROM tmp
WHERE NOT EXISTS (
    SELECT 1
    FROM emissions e
    WHERE e.facility_id = tmp.facility_id
    AND e.unit_id = tmp.unit_id
    AND e.date = tmp.date
    AND e.hour = tmp.hour
))
ORDER BY facility_id, unit_id, date, hour;
"#,
            self.base_dir,
            year,
            self.state.to_lowercase(),
        );

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
            error!(
                "Failed to update duckdb for {} {}: {}",
                year, self.state, stderr
            );
        }

        Ok(())
    }
}

// Auto-generated Rust stub for DuckDB table: emissions
// Created on 2026-05-25 with Dart package reduct
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct Record {
    pub state: String,
    pub facility_name: String,
    pub facility_id: u32,
    pub unit_id: Option<String>,
    pub associated_stacks: Option<String>,
    pub date: Date,
    pub hour: u8,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub operating_time: Option<Decimal>,
    pub gross_load: Option<u16>,
    pub steam_load: Option<f32>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub so2_mass: Option<Decimal>,
    pub so2_mass_measure_indicator: Option<So2MassMeasureIndicator>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub so2_rate: Option<Decimal>,
    pub so2_rate_measure_indicator: Option<So2RateMeasureIndicator>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub co2_mass: Option<Decimal>,
    pub co2_mass_measure_indicator: Option<Co2MassMeasureIndicator>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub co2_rate: Option<Decimal>,
    pub co2_rate_measure_indicator: Option<Co2RateMeasureIndicator>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub nox_mass: Option<Decimal>,
    pub nox_mass_measure_indicator: Option<NoxMassMeasureIndicator>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub nox_rate: Option<Decimal>,
    pub nox_rate_measure_indicator: Option<NoxRateMeasureIndicator>,
    #[serde(with = "rust_decimal::serde::float_option")]
    pub heat_input: Option<Decimal>,
    pub heat_input_measure_indicator: Option<HeatInputMeasureIndicator>,
    pub primary_fuel_type: Option<String>,
    pub secondary_fuel_type: Option<String>,
    pub unit_type: Option<UnitType>,
    pub so2_controls: Option<String>,
    pub nox_controls: Option<String>,
    pub pm_controls: Option<String>,
    pub hg_controls: Option<String>,
    pub program_code: Option<String>,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum So2MassMeasureIndicator {
    Calculated,
    Lme,
    Measured,
    MeasuredAndSubstitute,
    Other,
    Substitute,
}

impl std::str::FromStr for So2MassMeasureIndicator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "CALCULATED" => Ok(So2MassMeasureIndicator::Calculated),
            "LME" => Ok(So2MassMeasureIndicator::Lme),
            "MEASURED" => Ok(So2MassMeasureIndicator::Measured),
            "MEASURED_AND_SUBSTITUTE" => Ok(So2MassMeasureIndicator::MeasuredAndSubstitute),
            "OTHER" => Ok(So2MassMeasureIndicator::Other),
            "SUBSTITUTE" => Ok(So2MassMeasureIndicator::Substitute),
            _ => Err(format!("Invalid value for So2MassMeasureIndicator: {}", s)),
        }
    }
}

impl std::fmt::Display for So2MassMeasureIndicator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            So2MassMeasureIndicator::Calculated => write!(f, "Calculated"),
            So2MassMeasureIndicator::Lme => write!(f, "LME"),
            So2MassMeasureIndicator::Measured => write!(f, "Measured"),
            So2MassMeasureIndicator::MeasuredAndSubstitute => write!(f, "Measured and Substitute"),
            So2MassMeasureIndicator::Other => write!(f, "Other"),
            So2MassMeasureIndicator::Substitute => write!(f, "Substitute"),
        }
    }
}

impl serde::Serialize for So2MassMeasureIndicator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            So2MassMeasureIndicator::Calculated => "Calculated",
            So2MassMeasureIndicator::Lme => "LME",
            So2MassMeasureIndicator::Measured => "Measured",
            So2MassMeasureIndicator::MeasuredAndSubstitute => "Measured and Substitute",
            So2MassMeasureIndicator::Other => "Other",
            So2MassMeasureIndicator::Substitute => "Substitute",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for So2MassMeasureIndicator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        So2MassMeasureIndicator::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum So2RateMeasureIndicator {
    Calculated,
}

impl std::str::FromStr for So2RateMeasureIndicator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "CALCULATED" => Ok(So2RateMeasureIndicator::Calculated),
            _ => Err(format!("Invalid value for So2RateMeasureIndicator: {}", s)),
        }
    }
}

impl std::fmt::Display for So2RateMeasureIndicator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            So2RateMeasureIndicator::Calculated => write!(f, "Calculated"),
        }
    }
}

impl serde::Serialize for So2RateMeasureIndicator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            So2RateMeasureIndicator::Calculated => "Calculated",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for So2RateMeasureIndicator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        So2RateMeasureIndicator::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Co2MassMeasureIndicator {
    Calculated,
    Lme,
    Measured,
    MeasuredAndSubstitute,
    Other,
    Substitute,
}

impl std::str::FromStr for Co2MassMeasureIndicator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "CALCULATED" => Ok(Co2MassMeasureIndicator::Calculated),
            "LME" => Ok(Co2MassMeasureIndicator::Lme),
            "MEASURED" => Ok(Co2MassMeasureIndicator::Measured),
            "MEASURED_AND_SUBSTITUTE" => Ok(Co2MassMeasureIndicator::MeasuredAndSubstitute),
            "OTHER" => Ok(Co2MassMeasureIndicator::Other),
            "SUBSTITUTE" => Ok(Co2MassMeasureIndicator::Substitute),
            _ => Err(format!("Invalid value for Co2MassMeasureIndicator: {}", s)),
        }
    }
}

impl std::fmt::Display for Co2MassMeasureIndicator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Co2MassMeasureIndicator::Calculated => write!(f, "Calculated"),
            Co2MassMeasureIndicator::Lme => write!(f, "LME"),
            Co2MassMeasureIndicator::Measured => write!(f, "Measured"),
            Co2MassMeasureIndicator::MeasuredAndSubstitute => write!(f, "Measured and Substitute"),
            Co2MassMeasureIndicator::Other => write!(f, "Other"),
            Co2MassMeasureIndicator::Substitute => write!(f, "Substitute"),
        }
    }
}

impl serde::Serialize for Co2MassMeasureIndicator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            Co2MassMeasureIndicator::Calculated => "Calculated",
            Co2MassMeasureIndicator::Lme => "LME",
            Co2MassMeasureIndicator::Measured => "Measured",
            Co2MassMeasureIndicator::MeasuredAndSubstitute => "Measured and Substitute",
            Co2MassMeasureIndicator::Other => "Other",
            Co2MassMeasureIndicator::Substitute => "Substitute",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for Co2MassMeasureIndicator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Co2MassMeasureIndicator::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum Co2RateMeasureIndicator {
    Calculated,
}

impl std::str::FromStr for Co2RateMeasureIndicator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "CALCULATED" => Ok(Co2RateMeasureIndicator::Calculated),
            _ => Err(format!("Invalid value for Co2RateMeasureIndicator: {}", s)),
        }
    }
}

impl std::fmt::Display for Co2RateMeasureIndicator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Co2RateMeasureIndicator::Calculated => write!(f, "Calculated"),
        }
    }
}

impl serde::Serialize for Co2RateMeasureIndicator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            Co2RateMeasureIndicator::Calculated => "Calculated",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for Co2RateMeasureIndicator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Co2RateMeasureIndicator::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NoxMassMeasureIndicator {
    Calculated,
    Lme,
    Measured,
    MeasuredAndSubstitute,
    Other,
    Substitute,
}

impl std::str::FromStr for NoxMassMeasureIndicator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "CALCULATED" => Ok(NoxMassMeasureIndicator::Calculated),
            "LME" => Ok(NoxMassMeasureIndicator::Lme),
            "MEASURED" => Ok(NoxMassMeasureIndicator::Measured),
            "MEASURED_AND_SUBSTITUTE" => Ok(NoxMassMeasureIndicator::MeasuredAndSubstitute),
            "OTHER" => Ok(NoxMassMeasureIndicator::Other),
            "SUBSTITUTE" => Ok(NoxMassMeasureIndicator::Substitute),
            _ => Err(format!("Invalid value for NoxMassMeasureIndicator: {}", s)),
        }
    }
}

impl std::fmt::Display for NoxMassMeasureIndicator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NoxMassMeasureIndicator::Calculated => write!(f, "Calculated"),
            NoxMassMeasureIndicator::Lme => write!(f, "LME"),
            NoxMassMeasureIndicator::Measured => write!(f, "Measured"),
            NoxMassMeasureIndicator::MeasuredAndSubstitute => write!(f, "Measured and Substitute"),
            NoxMassMeasureIndicator::Other => write!(f, "Other"),
            NoxMassMeasureIndicator::Substitute => write!(f, "Substitute"),
        }
    }
}

impl serde::Serialize for NoxMassMeasureIndicator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            NoxMassMeasureIndicator::Calculated => "Calculated",
            NoxMassMeasureIndicator::Lme => "LME",
            NoxMassMeasureIndicator::Measured => "Measured",
            NoxMassMeasureIndicator::MeasuredAndSubstitute => "Measured and Substitute",
            NoxMassMeasureIndicator::Other => "Other",
            NoxMassMeasureIndicator::Substitute => "Substitute",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for NoxMassMeasureIndicator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NoxMassMeasureIndicator::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum NoxRateMeasureIndicator {
    Calculated,
    Lme,
    Measured,
    MeasuredAndSubstitute,
    Other,
    Substitute,
}

impl std::str::FromStr for NoxRateMeasureIndicator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "CALCULATED" => Ok(NoxRateMeasureIndicator::Calculated),
            "LME" => Ok(NoxRateMeasureIndicator::Lme),
            "MEASURED" => Ok(NoxRateMeasureIndicator::Measured),
            "MEASURED_AND_SUBSTITUTE" => Ok(NoxRateMeasureIndicator::MeasuredAndSubstitute),
            "OTHER" => Ok(NoxRateMeasureIndicator::Other),
            "SUBSTITUTE" => Ok(NoxRateMeasureIndicator::Substitute),
            _ => Err(format!("Invalid value for NoxRateMeasureIndicator: {}", s)),
        }
    }
}

impl std::fmt::Display for NoxRateMeasureIndicator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            NoxRateMeasureIndicator::Calculated => write!(f, "Calculated"),
            NoxRateMeasureIndicator::Lme => write!(f, "LME"),
            NoxRateMeasureIndicator::Measured => write!(f, "Measured"),
            NoxRateMeasureIndicator::MeasuredAndSubstitute => write!(f, "Measured and Substitute"),
            NoxRateMeasureIndicator::Other => write!(f, "Other"),
            NoxRateMeasureIndicator::Substitute => write!(f, "Substitute"),
        }
    }
}

impl serde::Serialize for NoxRateMeasureIndicator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            NoxRateMeasureIndicator::Calculated => "Calculated",
            NoxRateMeasureIndicator::Lme => "LME",
            NoxRateMeasureIndicator::Measured => "Measured",
            NoxRateMeasureIndicator::MeasuredAndSubstitute => "Measured and Substitute",
            NoxRateMeasureIndicator::Other => "Other",
            NoxRateMeasureIndicator::Substitute => "Substitute",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for NoxRateMeasureIndicator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        NoxRateMeasureIndicator::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum HeatInputMeasureIndicator {
    Calculated,
    Lme,
    Measured,
    MeasuredAndSubstitute,
    Other,
    Substitute,
}

impl std::str::FromStr for HeatInputMeasureIndicator {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "CALCULATED" => Ok(HeatInputMeasureIndicator::Calculated),
            "LME" => Ok(HeatInputMeasureIndicator::Lme),
            "MEASURED" => Ok(HeatInputMeasureIndicator::Measured),
            "MEASURED_AND_SUBSTITUTE" => Ok(HeatInputMeasureIndicator::MeasuredAndSubstitute),
            "OTHER" => Ok(HeatInputMeasureIndicator::Other),
            "SUBSTITUTE" => Ok(HeatInputMeasureIndicator::Substitute),
            _ => Err(format!(
                "Invalid value for HeatInputMeasureIndicator: {}",
                s
            )),
        }
    }
}

impl std::fmt::Display for HeatInputMeasureIndicator {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            HeatInputMeasureIndicator::Calculated => write!(f, "Calculated"),
            HeatInputMeasureIndicator::Lme => write!(f, "LME"),
            HeatInputMeasureIndicator::Measured => write!(f, "Measured"),
            HeatInputMeasureIndicator::MeasuredAndSubstitute => {
                write!(f, "Measured and Substitute")
            }
            HeatInputMeasureIndicator::Other => write!(f, "Other"),
            HeatInputMeasureIndicator::Substitute => write!(f, "Substitute"),
        }
    }
}

impl serde::Serialize for HeatInputMeasureIndicator {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            HeatInputMeasureIndicator::Calculated => "Calculated",
            HeatInputMeasureIndicator::Lme => "LME",
            HeatInputMeasureIndicator::Measured => "Measured",
            HeatInputMeasureIndicator::MeasuredAndSubstitute => "Measured and Substitute",
            HeatInputMeasureIndicator::Other => "Other",
            HeatInputMeasureIndicator::Substitute => "Substitute",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for HeatInputMeasureIndicator {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        HeatInputMeasureIndicator::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum UnitType {
    ArchFiredBoiler,
    BubblingFluidizedBedBoiler,
    CellBurnerBoiler,
    CementKiln,
    CirculatingFluidizedBedBoiler,
    CombinedCycle,
    CombustionTurbine,
    CycloneBoiler,
    DryBottomTurboFiredBoiler,
    DryBottomVerticallyFiredBoiler,
    DryBottomWallFiredBoiler,
    IntegratedGasificationCombinedCycle,
    InternalCombustionEngine,
    OtherBoiler,
    OtherTurbine,
    PressurizedFluidizedBedBoiler,
    ProcessHeater,
    Stoker,
    TangentiallyFired,
    WetBottomTurboFiredBoiler,
    WetBottomVerticallyFiredBoiler,
    WetBottomWallFiredBoiler,
}

impl std::str::FromStr for UnitType {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_case(Case::UpperSnake).as_str() {
            "ARCH_FIRED_BOILER" => Ok(UnitType::ArchFiredBoiler),
            "BUBBLING_FLUIDIZED_BED_BOILER" => Ok(UnitType::BubblingFluidizedBedBoiler),
            "CELL_BURNER_BOILER" => Ok(UnitType::CellBurnerBoiler),
            "CEMENT_KILN" => Ok(UnitType::CementKiln),
            "CIRCULATING_FLUIDIZED_BED_BOILER" => Ok(UnitType::CirculatingFluidizedBedBoiler),
            "COMBINED_CYCLE" => Ok(UnitType::CombinedCycle),
            "COMBUSTION_TURBINE" => Ok(UnitType::CombustionTurbine),
            "CYCLONE_BOILER" => Ok(UnitType::CycloneBoiler),
            "DRY_BOTTOM_TURBO_FIRED_BOILER" => Ok(UnitType::DryBottomTurboFiredBoiler),
            "DRY_BOTTOM_VERTICALLY_FIRED_BOILER" => Ok(UnitType::DryBottomVerticallyFiredBoiler),
            "DRY_BOTTOM_WALL_FIRED_BOILER" => Ok(UnitType::DryBottomWallFiredBoiler),
            "INTEGRATED_GASIFICATION_COMBINED_CYCLE" => {
                Ok(UnitType::IntegratedGasificationCombinedCycle)
            }
            "INTERNAL_COMBUSTION_ENGINE" => Ok(UnitType::InternalCombustionEngine),
            "OTHER_BOILER" => Ok(UnitType::OtherBoiler),
            "OTHER_TURBINE" => Ok(UnitType::OtherTurbine),
            "PRESSURIZED_FLUIDIZED_BED_BOILER" => Ok(UnitType::PressurizedFluidizedBedBoiler),
            "PROCESS_HEATER" => Ok(UnitType::ProcessHeater),
            "STOKER" => Ok(UnitType::Stoker),
            "TANGENTIALLY_FIRED" => Ok(UnitType::TangentiallyFired),
            "WET_BOTTOM_TURBO_FIRED_BOILER" => Ok(UnitType::WetBottomTurboFiredBoiler),
            "WET_BOTTOM_VERTICALLY_FIRED_BOILER" => Ok(UnitType::WetBottomVerticallyFiredBoiler),
            "WET_BOTTOM_WALL_FIRED_BOILER" => Ok(UnitType::WetBottomWallFiredBoiler),
            _ => Err(format!("Invalid value for UnitType: {}", s)),
        }
    }
}

impl std::fmt::Display for UnitType {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            UnitType::ArchFiredBoiler => write!(f, "Arch-fired boiler"),
            UnitType::BubblingFluidizedBedBoiler => write!(f, "Bubbling fluidized bed boiler"),
            UnitType::CellBurnerBoiler => write!(f, "Cell burner boiler"),
            UnitType::CementKiln => write!(f, "Cement Kiln"),
            UnitType::CirculatingFluidizedBedBoiler => {
                write!(f, "Circulating fluidized bed boiler")
            }
            UnitType::CombinedCycle => write!(f, "Combined cycle"),
            UnitType::CombustionTurbine => write!(f, "Combustion turbine"),
            UnitType::CycloneBoiler => write!(f, "Cyclone boiler"),
            UnitType::DryBottomTurboFiredBoiler => write!(f, "Dry bottom turbo-fired boiler"),
            UnitType::DryBottomVerticallyFiredBoiler => {
                write!(f, "Dry bottom vertically-fired boiler")
            }
            UnitType::DryBottomWallFiredBoiler => write!(f, "Dry bottom wall-fired boiler"),
            UnitType::IntegratedGasificationCombinedCycle => {
                write!(f, "Integrated gasification combined cycle")
            }
            UnitType::InternalCombustionEngine => write!(f, "Internal combustion engine"),
            UnitType::OtherBoiler => write!(f, "Other boiler"),
            UnitType::OtherTurbine => write!(f, "Other turbine"),
            UnitType::PressurizedFluidizedBedBoiler => {
                write!(f, "Pressurized fluidized bed boiler")
            }
            UnitType::ProcessHeater => write!(f, "Process Heater"),
            UnitType::Stoker => write!(f, "Stoker"),
            UnitType::TangentiallyFired => write!(f, "Tangentially-fired"),
            UnitType::WetBottomTurboFiredBoiler => write!(f, "Wet bottom turbo-fired boiler"),
            UnitType::WetBottomVerticallyFiredBoiler => {
                write!(f, "Wet bottom vertically-fired boiler")
            }
            UnitType::WetBottomWallFiredBoiler => write!(f, "Wet bottom wall-fired boiler"),
        }
    }
}

impl serde::Serialize for UnitType {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = match self {
            UnitType::ArchFiredBoiler => "Arch-fired boiler",
            UnitType::BubblingFluidizedBedBoiler => "Bubbling fluidized bed boiler",
            UnitType::CellBurnerBoiler => "Cell burner boiler",
            UnitType::CementKiln => "Cement Kiln",
            UnitType::CirculatingFluidizedBedBoiler => "Circulating fluidized bed boiler",
            UnitType::CombinedCycle => "Combined cycle",
            UnitType::CombustionTurbine => "Combustion turbine",
            UnitType::CycloneBoiler => "Cyclone boiler",
            UnitType::DryBottomTurboFiredBoiler => "Dry bottom turbo-fired boiler",
            UnitType::DryBottomVerticallyFiredBoiler => "Dry bottom vertically-fired boiler",
            UnitType::DryBottomWallFiredBoiler => "Dry bottom wall-fired boiler",
            UnitType::IntegratedGasificationCombinedCycle => {
                "Integrated gasification combined cycle"
            }
            UnitType::InternalCombustionEngine => "Internal combustion engine",
            UnitType::OtherBoiler => "Other boiler",
            UnitType::OtherTurbine => "Other turbine",
            UnitType::PressurizedFluidizedBedBoiler => "Pressurized fluidized bed boiler",
            UnitType::ProcessHeater => "Process Heater",
            UnitType::Stoker => "Stoker",
            UnitType::TangentiallyFired => "Tangentially-fired",
            UnitType::WetBottomTurboFiredBoiler => "Wet bottom turbo-fired boiler",
            UnitType::WetBottomVerticallyFiredBoiler => "Wet bottom vertically-fired boiler",
            UnitType::WetBottomWallFiredBoiler => "Wet bottom wall-fired boiler",
        };
        serializer.serialize_str(s)
    }
}

impl<'de> serde::Deserialize<'de> for UnitType {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        UnitType::from_str(&s).map_err(serde::de::Error::custom)
    }
}

pub fn get_data(
    conn: &Connection,
    query_filter: &QueryFilter,
    limit: Option<usize>,
) -> Result<Vec<Record>, Box<dyn std::error::Error>> {
    let mut query = String::from(
        r#"
SELECT
    state,
    facility_name,
    facility_id,
    unit_id,
    associated_stacks,
    date,
    hour,
    operating_time,
    gross_load,
    steam_load,
    so2_mass,
    so2_mass_measure_indicator,
    so2_rate,
    so2_rate_measure_indicator,
    co2_mass,
    co2_mass_measure_indicator,
    co2_rate,
    co2_rate_measure_indicator,
    nox_mass,
    nox_mass_measure_indicator,
    nox_rate,
    nox_rate_measure_indicator,
    heat_input,
    heat_input_measure_indicator,
    primary_fuel_type,
    secondary_fuel_type,
    unit_type,
    so2_controls,
    nox_controls,
    pm_controls,
    hg_controls,
    program_code
FROM emissions WHERE 1=1"#,
    );
    if let Some(state) = &query_filter.state {
        query.push_str(&format!(
            "
    AND state = '{}'",
            state
        ));
    }
    if let Some(state_like) = &query_filter.state_like {
        query.push_str(&format!(
            "
    AND state LIKE '{}'",
            state_like
        ));
    }
    if let Some(state_in) = &query_filter.state_in {
        query.push_str(&format!(
            "
    AND state IN ('{}')",
            state_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(facility_id) = &query_filter.facility_id {
        query.push_str(&format!(
            "
    AND facility_id = {}",
            facility_id
        ));
    }
    if let Some(facility_id_in) = &query_filter.facility_id_in {
        query.push_str(&format!(
            "
    AND facility_id IN ({})",
            facility_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",")
        ));
    }
    if let Some(facility_id_gte) = &query_filter.facility_id_gte {
        query.push_str(&format!(
            "
    AND facility_id >= {}",
            facility_id_gte
        ));
    }
    if let Some(facility_id_lte) = &query_filter.facility_id_lte {
        query.push_str(&format!(
            "
    AND facility_id <= {}",
            facility_id_lte
        ));
    }
    if let Some(unit_id) = &query_filter.unit_id {
        query.push_str(&format!(
            "
    AND unit_id = '{}'",
            unit_id
        ));
    }
    if let Some(unit_id_like) = &query_filter.unit_id_like {
        query.push_str(&format!(
            "
    AND unit_id LIKE '{}'",
            unit_id_like
        ));
    }
    if let Some(unit_id_in) = &query_filter.unit_id_in {
        query.push_str(&format!(
            "
    AND unit_id IN ('{}')",
            unit_id_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(date) = &query_filter.date {
        query.push_str(&format!(
            "
    AND date = '{}'",
            date
        ));
    }
    if let Some(date_in) = &query_filter.date_in {
        query.push_str(&format!(
            "
    AND date IN ('{}')",
            date_in
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join("','")
        ));
    }
    if let Some(date_gte) = &query_filter.date_gte {
        query.push_str(&format!(
            "
    AND date >= '{}'",
            date_gte
        ));
    }
    if let Some(date_lte) = &query_filter.date_lte {
        query.push_str(&format!(
            "
    AND date <= '{}'",
            date_lte
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
        let state: String = row.get::<usize, String>(0)?;
        let facility_name: String = row.get::<usize, String>(1)?;
        let facility_id: u32 = row.get::<usize, u32>(2)?;
        let unit_id: Option<String> = row.get::<usize, Option<String>>(3)?;
        let associated_stacks: Option<String> = row.get::<usize, Option<String>>(4)?;
        let _n5 = 719528 + row.get::<usize, i32>(5)?;
        let date = Date::ZERO + _n5.days();
        let hour: u8 = row.get::<usize, u8>(6)?;
        let operating_time: Option<Decimal> = match row.get_ref_unwrap(7) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let gross_load: Option<u16> = row.get::<usize, Option<u16>>(8)?;
        let steam_load: Option<f32> = row.get::<usize, Option<f32>>(9)?;
        let so2_mass: Option<Decimal> = match row.get_ref_unwrap(10) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let _n11 = match row.get_ref_unwrap(11).to_owned() {
            duckdb::types::Value::Enum(v) => Some(v),
            duckdb::types::Value::Null => None,
            v => panic!("Unexpected value type {v:?} for enum so2_mass_measure_indicator"),
        };
        let so2_mass_measure_indicator =
            _n11.map(|s| So2MassMeasureIndicator::from_str(&s).unwrap());
        let so2_rate: Option<Decimal> = match row.get_ref_unwrap(12) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let _n13 = match row.get_ref_unwrap(13).to_owned() {
            duckdb::types::Value::Enum(v) => Some(v),
            duckdb::types::Value::Null => None,
            v => panic!("Unexpected value type {v:?} for enum so2_rate_measure_indicator"),
        };
        let so2_rate_measure_indicator =
            _n13.map(|s| So2RateMeasureIndicator::from_str(&s).unwrap());
        let co2_mass: Option<Decimal> = match row.get_ref_unwrap(14) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let _n15 = match row.get_ref_unwrap(15).to_owned() {
            duckdb::types::Value::Enum(v) => Some(v),
            duckdb::types::Value::Null => None,
            v => panic!("Unexpected value type {v:?} for enum co2_mass_measure_indicator"),
        };
        let co2_mass_measure_indicator =
            _n15.map(|s| Co2MassMeasureIndicator::from_str(&s).unwrap());
        let co2_rate: Option<Decimal> = match row.get_ref_unwrap(16) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let _n17 = match row.get_ref_unwrap(17).to_owned() {
            duckdb::types::Value::Enum(v) => Some(v),
            duckdb::types::Value::Null => None,
            v => panic!("Unexpected value type {v:?} for enum co2_rate_measure_indicator"),
        };
        let co2_rate_measure_indicator =
            _n17.map(|s| Co2RateMeasureIndicator::from_str(&s).unwrap());
        let nox_mass: Option<Decimal> = match row.get_ref_unwrap(18) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let _n19 = match row.get_ref_unwrap(19).to_owned() {
            duckdb::types::Value::Enum(v) => Some(v),
            duckdb::types::Value::Null => None,
            v => panic!("Unexpected value type {v:?} for enum nox_mass_measure_indicator"),
        };
        let nox_mass_measure_indicator =
            _n19.map(|s| NoxMassMeasureIndicator::from_str(&s).unwrap());
        let nox_rate: Option<Decimal> = match row.get_ref_unwrap(20) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let _n21 = match row.get_ref_unwrap(21).to_owned() {
            duckdb::types::Value::Enum(v) => Some(v),
            duckdb::types::Value::Null => None,
            v => panic!("Unexpected value type {v:?} for enum nox_rate_measure_indicator"),
        };
        let nox_rate_measure_indicator =
            _n21.map(|s| NoxRateMeasureIndicator::from_str(&s).unwrap());
        let heat_input: Option<Decimal> = match row.get_ref_unwrap(22) {
            duckdb::types::ValueRef::Decimal(v) => Some(v),
            duckdb::types::ValueRef::Null => None,
            _ => None,
        };
        let _n23 = match row.get_ref_unwrap(23).to_owned() {
            duckdb::types::Value::Enum(v) => Some(v),
            duckdb::types::Value::Null => None,
            v => panic!("Unexpected value type {v:?} for enum heat_input_measure_indicator"),
        };
        let heat_input_measure_indicator =
            _n23.map(|s| HeatInputMeasureIndicator::from_str(&s).unwrap());
        let primary_fuel_type: Option<String> = row.get::<usize, Option<String>>(24)?;
        let secondary_fuel_type: Option<String> = row.get::<usize, Option<String>>(25)?;
        let _n26 = match row.get_ref_unwrap(26).to_owned() {
            duckdb::types::Value::Enum(v) => Some(v),
            duckdb::types::Value::Null => None,
            v => panic!("Unexpected value type {v:?} for enum unit_type"),
        };
        let unit_type = _n26.map(|s| UnitType::from_str(&s).unwrap());
        let so2_controls: Option<String> = row.get::<usize, Option<String>>(27)?;
        let nox_controls: Option<String> = row.get::<usize, Option<String>>(28)?;
        let pm_controls: Option<String> = row.get::<usize, Option<String>>(29)?;
        let hg_controls: Option<String> = row.get::<usize, Option<String>>(30)?;
        let program_code: Option<String> = row.get::<usize, Option<String>>(31)?;
        Ok(Record {
            state,
            facility_name,
            facility_id,
            unit_id,
            associated_stacks,
            date,
            hour,
            operating_time,
            gross_load,
            steam_load,
            so2_mass,
            so2_mass_measure_indicator,
            so2_rate,
            so2_rate_measure_indicator,
            co2_mass,
            co2_mass_measure_indicator,
            co2_rate,
            co2_rate_measure_indicator,
            nox_mass,
            nox_mass_measure_indicator,
            nox_rate,
            nox_rate_measure_indicator,
            heat_input,
            heat_input_measure_indicator,
            primary_fuel_type,
            secondary_fuel_type,
            unit_type,
            so2_controls,
            nox_controls,
            pm_controls,
            hg_controls,
            program_code,
        })
    })?;
    let results: Vec<Record> = rows.collect::<Result<_, _>>()?;
    Ok(results)
}

#[derive(Debug, Default, Deserialize)]
pub struct QueryFilter {
    pub state: Option<String>,
    pub state_like: Option<String>,
    pub state_in: Option<Vec<String>>,
    pub facility_id: Option<u32>,
    pub facility_id_in: Option<Vec<u32>>,
    pub facility_id_gte: Option<u32>,
    pub facility_id_lte: Option<u32>,
    pub unit_id: Option<String>,
    pub unit_id_like: Option<String>,
    pub unit_id_in: Option<Vec<String>>,
    pub date: Option<Date>,
    pub date_in: Option<Vec<Date>>,
    pub date_gte: Option<Date>,
    pub date_lte: Option<Date>,
}

impl QueryFilter {
    pub fn to_query_url(&self) -> String {
        let mut params = HashMap::new();
        if let Some(value) = &self.state {
            params.insert("state", value.to_string());
        }
        if let Some(value) = &self.state_like {
            params.insert("state_like", value.to_string());
        }
        if let Some(value) = &self.state_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("state_in", joined);
        }
        if let Some(value) = &self.facility_id {
            params.insert("facility_id", value.to_string());
        }
        if let Some(value) = &self.facility_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("facility_id_in", joined);
        }
        if let Some(value) = &self.facility_id_gte {
            params.insert("facility_id_gte", value.to_string());
        }
        if let Some(value) = &self.facility_id_lte {
            params.insert("facility_id_lte", value.to_string());
        }
        if let Some(value) = &self.unit_id {
            params.insert("unit_id", value.to_string());
        }
        if let Some(value) = &self.unit_id_like {
            params.insert("unit_id_like", value.to_string());
        }
        if let Some(value) = &self.unit_id_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("unit_id_in", joined);
        }
        if let Some(value) = &self.date {
            params.insert("date", value.to_string());
        }
        if let Some(value) = &self.date_in {
            let joined = value
                .iter()
                .map(|v| v.to_string())
                .collect::<Vec<_>>()
                .join(",");
            params.insert("date_in", joined);
        }
        if let Some(value) = &self.date_gte {
            params.insert("date_gte", value.to_string());
        }
        if let Some(value) = &self.date_lte {
            params.insert("date_lte", value.to_string());
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

    pub fn state<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.state = Some(value.into());
        self
    }

    pub fn state_like(mut self, value_like: String) -> Self {
        self.inner.state_like = Some(value_like);
        self
    }

    pub fn state_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.state_in = Some(values_in);
        self
    }

    pub fn facility_id(mut self, value: u32) -> Self {
        self.inner.facility_id = Some(value);
        self
    }

    pub fn facility_id_in(mut self, values_in: Vec<u32>) -> Self {
        self.inner.facility_id_in = Some(values_in);
        self
    }

    pub fn facility_id_gte(mut self, value: u32) -> Self {
        self.inner.facility_id_gte = Some(value);
        self
    }

    pub fn facility_id_lte(mut self, value: u32) -> Self {
        self.inner.facility_id_lte = Some(value);
        self
    }

    pub fn unit_id<S: Into<String>>(mut self, value: S) -> Self {
        self.inner.unit_id = Some(value.into());
        self
    }

    pub fn unit_id_like(mut self, value_like: String) -> Self {
        self.inner.unit_id_like = Some(value_like);
        self
    }

    pub fn unit_id_in(mut self, values_in: Vec<String>) -> Self {
        self.inner.unit_id_in = Some(values_in);
        self
    }

    pub fn date(mut self, value: Date) -> Self {
        self.inner.date = Some(value);
        self
    }

    pub fn date_in(mut self, values_in: Vec<Date>) -> Self {
        self.inner.date_in = Some(values_in);
        self
    }

    pub fn date_gte(mut self, value: Date) -> Self {
        self.inner.date_gte = Some(value);
        self
    }

    pub fn date_lte(mut self, value: Date) -> Self {
        self.inner.date_lte = Some(value);
        self
    }
}

#[cfg(test)]
mod tests {
    use duckdb::{AccessMode, Config, Connection};
    use log::info;
    use std::{error::Error, path::Path};

    use super::*;
    use crate::db::prod_db::ProdDb;

    #[test]
    fn test_get_data() -> Result<(), Box<dyn Error>> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn =
            Connection::open_with_flags(ProdDb::epa_hourly_emissions("NY").duckdb_path, config)
                .unwrap();
        let filter = QueryFilterBuilder::new().build();
        let xs: Vec<Record> = get_data(&conn, &filter, Some(5)).unwrap();
        conn.close().unwrap();
        assert_eq!(xs.len(), 5);
        Ok(())
    }

    #[test]
    #[ignore]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();

        // this data updates once a year, in spring!
        // there are mats quarterly files that are published at the end of each quarter.  You can use that for
        // more recent data
        let year = [2021, 2022, 2023, 2024, 2025];
        // let states = ["NH", "RI", "VA"];
        let states = ["MA", "ME", "NH", "RI", "VT", "NY", "VA"];
        for state in states {
            let archive = ProdDb::epa_hourly_emissions(state);
            for y in &year {
                info!(
                    "Working on hourly emissions file for year {} and state {} ...",
                    y, state
                );
                archive.download_file(*y)?;
                archive.update_duckdb(*y)?;
            }
        }
        Ok(())
    }
}
