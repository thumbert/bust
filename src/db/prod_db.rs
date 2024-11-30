use super::{isone::sevenday_solar_forecast_archive::SevendaySolarForecastArchive, nrc::generator_status_archive::GeneratorStatusArchive};

pub struct ProdDb {}

impl ProdDb {
    pub fn isone_sevenday_solar_forecast() -> SevendaySolarForecastArchive {
        SevendaySolarForecastArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/7daySolarForecast".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/IsoExpress/sevenday_solar_forecast.duckdb"
                .to_string(),
        }
    }

    pub fn nrc_generator_status() -> GeneratorStatusArchive {
        GeneratorStatusArchive {
            base_dir: "/home/adrian/Downloads/Archive/NRC/ReactorStatus".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/nrc_genearator_status.duckdb"
                .to_string(),
        }
    }
}
