use std::{path::Path, process::Command};

use std::error::Error;

use log::{error, info};

use crate::{db::isone::lib_isoexpress::download_file, interval::quarter::Quarter};

/// See https://campd.epa.gov/data/bulk-data-files
/// https://api.epa.gov/easey/bulk-files/emissions/daily/quarter/emissions-daily-2025-q2.csv?
///
///
///
#[derive(Clone)]
pub struct EpaDailyEmissionsArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

impl EpaDailyEmissionsArchive {
    fn filename(&self, quarter: &Quarter) -> String {
        format!(
            "{}/Raw/emissions-daily-{}-q{}.csv",
            self.base_dir,
            quarter.year(),
            quarter.quarter()
        )
    }

    pub fn download_file(&self, quarter: &Quarter) -> Result<(), Box<dyn Error>> {
        download_file(
            format!(
                "https://api.epa.gov/easey/bulk-files/emissions/daily/quarter/emissions-daily-{}-q{}.csv?",
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
        info!("inserting emissions daily file for quarter {} ...", quarter);

        let sql = format!(
            r#"
CREATE TABLE IF NOT EXISTS emissions (
    state VARCHAR(2) NOT NULL,
    facility_name VARCHAR NOT NULL,
    facility_id UINTEGER NOT NULL,
    unit_id VARCHAR,
    associated_stacks VARCHAR,
    date DATE NOT NULL,
    -- How many hours the unit ran
    hour_count UTINYINT NOT NULL,
    -- Fraction of the hour that the unit was operating, from 0 to 1.
    day_fraction DECIMAL(4, 2),
    gross_load USMALLINT,
    steam_load FLOAT,
    so2_mass DECIMAL(9, 4),
    so2_rate DECIMAL(9, 4),
    co2_mass DECIMAL(18, 6),
    co2_rate DECIMAL(9, 6),
    nox_mass DECIMAL(9, 4),
    nox_rate DECIMAL(9, 6),
    heat_input DECIMAL(18, 6),
    primary_fuel_type VARCHAR,
    secondary_fuel_type VARCHAR,
    so2_controls VARCHAR,
    nox_controls VARCHAR,
    pm_controls VARCHAR,
    hg_controls VARCHAR,
    program_code VARCHAR,
    -- unit_type VARCHAR
);

CREATE TEMPORARY TABLE tmp AS
SELECT * EXCLUDE(
        unit_type
    ),  
FROM (
    SELECT 
        CAST("State" AS VARCHAR(2)) as state,
        CAST("Facility Name" AS VARCHAR) as facility_name,
        CAST("Facility ID" AS UINTEGER) as facility_id,
        CAST("Unit ID" AS VARCHAR) as unit_id,
        CAST("Associated Stacks" AS VARCHAR) as associated_stacks,
        CAST(Date AS DATE) as date,
        CAST("Operating Time Count" AS UTINYINT) as hour_count,
        CAST("Sum of the Operating Time" AS DECIMAL(4, 2)) as day_fraction,
        CAST("Gross Load (MWh)" AS USMALLINT) as gross_load,
        CAST("Steam Load (1000 lb)" AS FLOAT) as steam_load,
        CAST("SO2 Mass (short tons)" AS DECIMAL(9, 4)) as so2_mass,
        CAST("SO2 Rate (lbs/mmBtu)" AS DECIMAL(9, 4)) as so2_rate,
        CAST("CO2 Mass (short tons)" AS DECIMAL(18, 6)) as co2_mass,
        CAST("CO2 Rate (short tons/mmBtu)" AS DECIMAL(9, 6)) as co2_rate,
        CAST("NOx Mass (short tons)" AS DECIMAL(9, 4)) as nox_mass,
        CAST("NOx Rate (lbs/mmBtu)" AS DECIMAL(9, 6)) as nox_rate,
        CAST("Heat Input (mmBtu)" AS DECIMAL(18, 6)) as heat_input,
        CAST("Primary Fuel Type" AS VARCHAR) as primary_fuel_type,
        CAST("Secondary Fuel Type" AS VARCHAR) as secondary_fuel_type,
        CAST("Unit Type" AS VARCHAR) as unit_type,    
        CAST("SO2 Controls" AS VARCHAR) as so2_controls,
        CAST("NOx Controls" AS VARCHAR) as nox_controls,
        CAST("PM Controls" AS VARCHAR) as pm_controls,
        CAST("Hg Controls" AS VARCHAR) as hg_controls,
        CAST("Program Code" AS VARCHAR) as program_code
    FROM read_csv(
            '{}/Raw/emissions-daily-{}-q{}.csv.gz',
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
))
ORDER BY facility_id, unit_id, date;
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
        let archive = ProdDb::epa_daily_emissions();

        let quarters = quarter(2022, 1).up_to(quarter(2026, 1));
        for quarter in quarters.unwrap() {
            info!("Working on quarter {}", quarter);
            // archive.download_file(&quarter)?;
            archive.update_duckdb(&quarter)?;
        }
        Ok(())
    }
}
