use super::{hq::hydrometeorological_data_archive::HqHydroDataArchive, isone::sevenday_solar_forecast_archive::SevendaySolarForecastArchive, nrc::generator_status_archive::GeneratorStatusArchive};

pub struct ProdDb {}

impl ProdDb {
    pub fn isone_sevenday_solar_forecast() -> SevendaySolarForecastArchive {
        SevendaySolarForecastArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/7daySolarForecast".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/IsoExpress/sevenday_solar_forecast.duckdb"
                .to_string(),
        }
    }

    pub fn hq_hydro_data() -> HqHydroDataArchive {
        HqHydroDataArchive {
            base_dir: "/home/adrian/Downloads/Archive/HQ/HydroMeteorologicalData".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/hq_water_level.duckdb"
                .to_string(),
        }
    }

    pub fn nrc_generator_status() -> GeneratorStatusArchive {
        GeneratorStatusArchive {
            base_dir: "/home/adrian/Downloads/Archive/NRC/ReactorStatus".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/nrc_generation_status.duckdb"
                .to_string(),
        }
    }
}
