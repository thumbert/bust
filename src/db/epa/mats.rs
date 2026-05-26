use std::{path::Path, process::Command};

use std::error::Error;

use log::{error, info};

use crate::{db::isone::lib_isoexpress::download_file, interval::quarter::Quarter};

/// See https://campd.epa.gov/data/bulk-data-files
/// https://api.epa.gov/easey/bulk-files/mats/hourly/quarter/mats-hourly-2026-q1.csv?
///
/// https://api.epa.gov/easey/bulk-files/emissions/daily/quarter/emissions-daily-2026-q1.csv?
///
///

#[derive(Clone)]
pub struct EpaMatsArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl EpaMatsArchive {
    fn filename(&self, quarter: &Quarter) -> String {
        format!(
            "{}/Raw/mats-hourly-{}-q{}.csv",
            self.base_dir,
            quarter.year(),
            quarter.quarter()
        )
    }

    pub fn download_file(&self, quarter: &Quarter) -> Result<(), Box<dyn Error>> {
        download_file(
            format!(
                "https://api.epa.gov/easey/bulk-files/mats/hourly/quarter/mats-hourly-{}-q{}.csv?",
                quarter.year(),
                quarter.quarter()
            ),
            false,
            None,
            Path::new(&self.filename(quarter)),
            true,
        )
    }

    /// Upload one file to DuckDB.
    pub fn update_duckdb(&self, quarter: &Quarter) -> Result<(), Box<dyn Error>> {
        info!("inserting mats file for quarter {} ...", quarter);

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS mats (
    State VARCHAR(2) NOT NULL,
    facility_name VARCHAR NOT NULL,
    facility_id UINTEGER NOT NULL,
    unit_id VARCHAR,
    Date DATE NOT NULL,
    Hour UTINYINT NOT NULL,
    -- Fraction of the hour that the unit was operating, from 0 to 1.
    operating_time DECIMAL(3, 2),
    gross_load USMALLINT,
    heat_input DECIMAL(9, 4),
    hg_output_rate DECIMAL(9, 4),
    hg_input_rate DECIMAL(9, 4),
    hg_mass DECIMAL(9, 4),
    hg_mass_measure_indicator ENUM(
        'Measured',
        'Unavailable',
        'Startup or Shutdown',
        'Calculated',
    ),
    hcl_output_rate DECIMAL(18, 6),
    hcl_input_rate DECIMAL(18, 6),
    hcl_mass DECIMAL(18, 6),
    hcl_mass_measure_indicator ENUM(
        'Measured',
        'Unavailable',
        'Startup or Shutdown',
        'Calculated',
    ),
    hf_output_rate DECIMAL(9, 4),
    hf_input_rate DECIMAL(9, 4),
    hf_mass DECIMAL(9, 4),
    hf_mass_measure_indicator ENUM(
        'Measured'
    ),
    associated_stacks VARCHAR,
    steam_load FLOAT,
    so2_mass DECIMAL(9, 4),
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
);

CREATE TEMPORARY TABLE tmp AS
SELECT * EXCLUDE(
        hg_mass_measure_indicator, 
        hcl_mass_measure_indicator,
        hf_mass_measure_indicator,
        unit_type,
    ),
    CAST (case when hg_mass_measure_indicator = 'MEASURE' then 'Measured' 
        when hg_mass_measure_indicator = 'CALC' then 'Calculated' 
        when hg_mass_measure_indicator = 'UNAVAIL' then 'Unavailable'
        when hg_mass_measure_indicator = 'Manually Calculated' then 'Calculated'
        when hg_mass_measure_indicator = 'UPDOWN' then 'Startup or Shutdown'
        else hg_mass_measure_indicator end AS ENUM('Measured', 'Calculated', 'Unavailable', 'Startup or Shutdown')) 
        as hg_mass_measure_indicator,
    CAST (case when hcl_mass_measure_indicator = 'MEASURE' then 'Measured' 
        when hcl_mass_measure_indicator = 'CALC' then 'Calculated' 
        when hcl_mass_measure_indicator = 'UNAVAIL' then 'Unavailable'
        when hcl_mass_measure_indicator = 'UPDOWN' then 'Startup or Shutdown'
        when hcl_mass_measure_indicator = 'Manually Calculated' then 'Calculated'
        else hcl_mass_measure_indicator end AS ENUM('Measured', 'Calculated', 'Unavailable', 'Startup or Shutdown')) 
        as hcl_mass_measure_indicator,
    CAST (case when hf_mass_measure_indicator = 'MEASURE' then 'Measured' 
        when hf_mass_measure_indicator = 'CALC' then 'Calculated' 
        when hf_mass_measure_indicator = 'UNAVAIL' then 'Unavailable'
        when hf_mass_measure_indicator = 'UPDOWN' then 'Startup or Shutdown'
        else hf_mass_measure_indicator end AS ENUM('Measured', 'Calculated', 'Unavailable', 'Startup or Shutdown'))
        as hf_mass_measure_indicator,
    CAST (case when unit_type  = 'Combustion turbine (Started Jan 12, 2024)' then 'Combustion turbine'
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
        CAST(Date AS DATE) as date,
        CAST(Hour AS UTINYINT) as hour,
        -- Fraction of the hour that the unit was operating, from 0 to 1.
        CAST("Operating Time" AS DECIMAL(3, 2)) as operating_time,
        CAST("MATS Gross Load (MW)" AS USMALLINT) as gross_load,
        CAST("MATS Heat Input (mmBtu)" AS DECIMAL(9, 4)) as heat_input,
        CAST("Hg Output Rate (lb/GWh)" AS DECIMAL(9, 4)) as hg_output_rate,
        CAST("Hg Input Rate (lb/TBtu)" AS DECIMAL(9, 4)) as hg_input_rate,
        CAST("Hg Mass (lbs)" AS DECIMAL(9, 4)) as hg_mass,
        CAST("Hg Mass Measure Indicator" AS VARCHAR) as hg_mass_measure_indicator,
        CAST("HCl Output Rate (lb/MWh)" AS DECIMAL(18, 6)) as hcl_output_rate,
        CAST("HCl Input Rate (lb/mmBtu)" AS DECIMAL(18, 6)) as hcl_input_rate,
        CAST("HCl Mass (lbs)" AS DECIMAL(18, 6)) as hcl_mass,
        CAST("HCl Mass Measure Indicator" AS VARCHAR) as hcl_mass_measure_indicator,
        CAST("HF Output Rate (lb/MWh)" AS DECIMAL(9, 4)) as hf_output_rate,
        CAST("HF Input Rate (lb/mmBtu)" AS DECIMAL(9, 4)) as hf_input_rate, 
        CAST("HF Mass (lbs)" AS DECIMAL(9, 4)) as hf_mass,
        CAST("HF Mass Measure Indicator" AS VARCHAR) as hf_mass_measure_indicator,
        CAST("Associated Stacks" AS VARCHAR) as associated_stacks,
        CAST("Steam Load (1000 lb/hr)" AS FLOAT) as steam_load,
        CAST("Primary Fuel Type" AS VARCHAR) as primary_fuel_type,
        CAST("Secondary Fuel Type" AS VARCHAR) as secondary_fuel_type,
        CAST("Unit Type" AS VARCHAR) as unit_type,
        CAST("SO2 Controls" AS VARCHAR) as so2_controls,
        CAST("NOx Controls" AS VARCHAR) as nox_controls,
        CAST("PM Controls" AS VARCHAR) as pm_controls,
        CAST("Hg Controls" AS VARCHAR) as hg_controls,
    FROM read_csv(
            '{}/Raw/mats-hourly-{}-q{}.csv.gz',
            header = true,
            types = {{ 'Unit ID': 'VARCHAR' }},
            dateformat = '%Y-%m-%d'
        )
);

INSERT INTO mats BY NAME
(SELECT * FROM tmp
WHERE NOT EXISTS (
    SELECT 1
    FROM mats e
    WHERE e.facility_id = tmp.facility_id
    AND e.unit_id = tmp.unit_id
    AND e.date = tmp.date
    AND e.hour = tmp.hour
))
ORDER BY facility_id, unit_id, date, hour;
            "#,
            self.base_dir,
            quarter.year(),
            quarter.quarter(),
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
                "Failed to update duckdb for quarter {}: {}",
                quarter, stderr
            );
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use log::info;
    use std::{error::Error, path::Path};

    use crate::{db::prod_db::ProdDb, interval::quarter::quarter};

    #[test]
    #[ignore]
    fn update_db() -> Result<(), Box<dyn Error>> {
        let _ = env_logger::builder()
            .filter_level(log::LevelFilter::Info)
            .is_test(true)
            .try_init();
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::epa_mats();

        let quarters = quarter(2022, 1).up_to(quarter(2026, 1));
        for quarter in quarters.unwrap() {
            info!("Working on quarter {}", quarter);
            // archive.download_file(&quarter)?;
            archive.update_duckdb(&quarter)?;
        }
        Ok(())
    }
}
