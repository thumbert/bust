use super::{hq::hydrometeorological_data_archive::HqHydroDataArchive, isone::{mis::{sd_daasdt::SdDaasdtArchive, sd_rtload::SdRtloadArchive, sr_rsvcharge2::SrRsvcharge2Archive, sr_rsvstl2::SrRsvstl2Archive}, sevenday_solar_forecast_archive::SevendaySolarForecastArchive}, nrc::generator_status_archive::GeneratorStatusArchive};

pub struct ProdDb {}

impl ProdDb {
    // pub fn isone_single_source_contingency() -> SingleSourceContingencyArchive {
    //     SingleSourceContingencyArchive {
    //         base_dir: "/home/adrian/Downloads/Archive/IsoExpress/SingleSourceContingency".to_string(),
    //         duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone_single_source_contingency.duckdb"
    //             .to_string(),
    //     }
    // }

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

    pub fn sd_daasdt() -> SdDaasdtArchive {
        SdDaasdtArchive {
            base_dir: "/home/adrian/Downloads/Archive/Mis/SD_DAASDT".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/sd_daasdt.duckdb"
                .to_string(),
        }
    }

    pub fn sd_rtload() -> SdRtloadArchive {
        SdRtloadArchive {
            base_dir: "/home/adrian/Downloads/Archive/Mis/SD_RTLOAD".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/sd_rtload.duckdb"
                .to_string(),
        }
    }

    pub fn sr_rsvcharge2() -> SrRsvcharge2Archive {
        SrRsvcharge2Archive {
            base_dir: "/home/adrian/Downloads/Archive/Mis/SR_RSVCHARGE2".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/sr_rsvcharge2.duckdb"
                .to_string(),
        }
    }

    pub fn sr_rsvstl2() -> SrRsvstl2Archive {
        SrRsvstl2Archive {
            base_dir: "/home/adrian/Downloads/Archive/Mis/SR_RSVSTL2".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/sr_rsvstl2.duckdb"
                .to_string(),
        }
    }


}
