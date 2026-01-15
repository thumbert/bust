use crate::db::{
    caiso::{
        dalmp_archive::CaisoDaLmpArchive, public_bids_archive::CaisoPublicBidsArchive,
        rtlmp_archive::CaisoRtLmpArchive,
    },
    calendar::buckets::BucketsArchive,
    hq::{
        electricity_demand_final::HqFinalizedTotalDemandArchive,
        electricity_demand_prelim::HqPrelimTotalDemandArchive, fuel_mix::HqFuelMixArchive,
    },
    ieso::{
        da_lmp_area::IesoDaLmpAreaArchive, generation_output_by_fuel::IesoGenOutputByFuelArchive,
        vgforecast_summary::IesoVGForecastSummaryArchive,
    },
    isone::{
        actual_interchange_archive::IsoneActualInterchangeArchive,
        dalmp_archive::IsoneDaLmpArchive,
        ftr_prices_archive::IsoneFtrPricesArchive,
        fuelmix_archive::IsoneFuelMixArchive,
        masked_data::{
            ara_archive::IsoneAraBidsOffersArchive, da_energy_offers_archive::IsoneDaEnergyOffersArchive, daas_offers_archive::DaasOffersArchive, demand_bids_archive::DemandBidsArchive, import_export_archive::ImportExportArchive, mra_archive::IsoneMraBidsOffersArchive
        },
        participants_archive::IsoneParticipantsArchive,
        rtlmp_archive::IsoneRtLmpArchive,
        sevenday_capacity_forecast_archive::SevendayCapacityForecastArchive,
        total_transfer_capability_archive::TotalTransferCapabilityArchive,
    },
    nyiso::{
        energy_offers::NyisoEnergyOffersArchive, rtlmp::NyisoRtlmpArchive,
        scheduled_outages::NyisoScheduledOutagesArchive,
        transmission_outages_da::NyisoTransmissionOutagesDaArchive,
    },
    statistics_canada::electricity_production::StatisticsCanadaGenerationArchive,
};

use super::{
    hq::hydrometeorological_data_archive::HqHydroDataArchive,
    ieso::{
        da_lmp_nodes::IesoDaLmpNodalArchive, da_lmp_zones::IesoDaLmpZonalArchive,
        node_table::IesoNodeTableArchive,
    },
    isone::{
        daas_reserve_data_archive::DaasReserveDataArchive,
        daas_strike_prices_archive::DaasStrikePricesArchive,
        mis::{
            sd_daasdt::SdDaasdtArchive, sd_rtload::SdRtloadArchive,
            sr_rsvcharge2::SrRsvcharge2Archive, sr_rsvstl2::SrRsvstl2Archive,
        },
        sevenday_solar_forecast_archive::SevendaySolarForecastArchive,
        single_source_contingency_archive::SingleSourceContingencyArchive,
    },
    nrc::generator_status_archive::GeneratorStatusArchive,
    nyiso::dalmp::NyisoDalmpArchive,
};

pub struct ProdDb {}

impl ProdDb {
    pub fn buckets() -> BucketsArchive {
        BucketsArchive {
            base_dir: "/home/adrian/Downloads/Archive/Calendars".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/calendars/buckets.duckdb"
                .to_string(),
        }
    }

    pub fn caiso_dalmp() -> CaisoDaLmpArchive {
        CaisoDaLmpArchive {
            base_dir: "/home/adrian/Downloads/Archive/Caiso/DaLmp".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/caiso/dalmp.duckdb".to_string(),
        }
    }

    pub fn caiso_rtlmp() -> CaisoRtLmpArchive {
        CaisoRtLmpArchive {
            base_dir: "/home/adrian/Downloads/Archive/Caiso/RtLmp".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/caiso/rtlmp.duckdb".to_string(),
        }
    }

    pub fn caiso_public_bids() -> CaisoPublicBidsArchive {
        CaisoPublicBidsArchive {
            base_dir: "/home/adrian/Downloads/Archive/Caiso/PublicBids".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/caiso/public_bids.duckdb"
                .to_string(),
        }
    }

    pub fn ieso_dalmp_area() -> IesoDaLmpAreaArchive {
        IesoDaLmpAreaArchive {
            base_dir: "/home/adrian/Downloads/Archive/Ieso/DaLmp/Area".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/ieso/da_lmp.duckdb".to_string(),
        }
    }

    pub fn ieso_dalmp_nodes() -> IesoDaLmpNodalArchive {
        IesoDaLmpNodalArchive {
            base_dir: "/home/adrian/Downloads/Archive/Ieso/DaLmp/Node".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/ieso/da_lmp.duckdb".to_string(),
        }
    }

    pub fn ieso_dalmp_zonal() -> IesoDaLmpZonalArchive {
        IesoDaLmpZonalArchive {
            base_dir: "/home/adrian/Downloads/Archive/Ieso/DaLmp/Zone".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/ieso/da_lmp.duckdb".to_string(),
        }
    }

    pub fn ieso_generation_output_by_fuel() -> IesoGenOutputByFuelArchive {
        IesoGenOutputByFuelArchive {
            base_dir: "/home/adrian/Downloads/Archive/Ieso/GenerationOutputByFuel".to_string(),
            duckdb_path:
                "/home/adrian/Downloads/Archive/DuckDB/ieso/generation_output_by_fuel.duckdb"
                    .to_string(),
        }
    }

    pub fn ieso_node_table() -> IesoNodeTableArchive {
        IesoNodeTableArchive {
            base_dir: "/home/adrian/Downloads/Archive/Ieso/NodeTable".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/ieso/node_table.duckdb".to_string(),
        }
    }

    pub fn ieso_vgforecast_summary() -> IesoVGForecastSummaryArchive {
        IesoVGForecastSummaryArchive {
            base_dir: "/home/adrian/Downloads/Archive/Ieso/VGForecastSummary".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/ieso/vgforecast_summary.duckdb"
                .to_string(),
        }
    }

    pub fn isone_actual_interchange() -> IsoneActualInterchangeArchive {
        IsoneActualInterchangeArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/ActualInterchange".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/actual_interchange.duckdb"
                .to_string(),
        }
    }

    pub fn isone_daas_reserve_data() -> DaasReserveDataArchive {
        DaasReserveDataArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/DASI/ReserveData".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/daas_reserve_data.duckdb"
                .to_string(),
        }
    }

    pub fn isone_daas_strike_prices() -> DaasStrikePricesArchive {
        DaasStrikePricesArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/DASI/StrikePrices".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/daas_strike_prices.duckdb"
                .to_string(),
        }
    }

    pub fn isone_dalmp() -> IsoneDaLmpArchive {
        IsoneDaLmpArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/PricingReports/DaLmpHourly"
                .to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/dalmp.duckdb".to_string(),
        }
    }

    pub fn isone_ftr_cleared_prices() -> IsoneFtrPricesArchive {
        IsoneFtrPricesArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/FTR/ClearedPrices".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/ftr_prices.duckdb"
                .to_string(),
        }
    }

    pub fn isone_fuel_mix() -> IsoneFuelMixArchive {
        IsoneFuelMixArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/GridReports/FuelMix".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/fuelmix.duckdb".to_string(),
        }
    }

    pub fn isone_masked_ara_bids_offers() -> IsoneAraBidsOffersArchive {
        IsoneAraBidsOffersArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/AnnualReconfigurationAuction"
                .to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/ara.duckdb".to_string(),
        }
    }

    pub fn isone_masked_da_energy_offers() -> IsoneDaEnergyOffersArchive {
        IsoneDaEnergyOffersArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/PricingReports/DaEnergyOffer"
                .to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/masked_energy_offers.duckdb"
                .to_string(),
        }
    }

    pub fn isone_masked_daas_offers() -> DaasOffersArchive {
        DaasOffersArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/PricingReports/DaasOffers"
                .to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/masked_daas_offers.duckdb"
                .to_string(),
        }
    }

    pub fn isone_masked_demand_bids() -> DemandBidsArchive {
        DemandBidsArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/PricingReports/DaDemandBid"
                .to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/masked_demand_bids.duckdb"
                .to_string(),
        }
    }

    pub fn isone_masked_import_export() -> ImportExportArchive {
        ImportExportArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/PricingReports/ImportExport"
                .to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/masked_import_export.duckdb"
                .to_string(),
        }
    }

    pub fn isone_participants_archive() -> IsoneParticipantsArchive {
        IsoneParticipantsArchive {
            base_dir: "/home/adrian/Downloads/Archive/Isone/Participants".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/participants.duckdb"
                .to_string(),
        }
    }

    pub fn isone_single_source_contingency() -> SingleSourceContingencyArchive {
        SingleSourceContingencyArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/SingleSourceContingency"
                .to_string(),
            duckdb_path:
                "/home/adrian/Downloads/Archive/DuckDB/isone/single_source_contingency.duckdb"
                    .to_string(),
        }
    }

    pub fn isone_sevenday_solar_forecast() -> SevendaySolarForecastArchive {
        SevendaySolarForecastArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/7daySolarForecast".to_string(),
            duckdb_path:
                "/home/adrian/Downloads/Archive/DuckDB/isone/sevenday_solar_forecast.duckdb"
                    .to_string(),
        }
    }

    pub fn isone_sevenday_capacity_forecast() -> SevendayCapacityForecastArchive {
        SevendayCapacityForecastArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/7dayCapacityForecast".to_string(),
            duckdb_path:
                "/home/adrian/Downloads/Archive/DuckDB/isone/sevenday_capacity_forecast.duckdb"
                    .to_string(),
        }
    }

    pub fn isone_mra_bids_offers() -> IsoneMraBidsOffersArchive {
        IsoneMraBidsOffersArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction"
                .to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/mra.duckdb".to_string(),
        }
    }

    pub fn isone_rtlmp() -> IsoneRtLmpArchive {
        IsoneRtLmpArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/PricingReports/RtLmpHourly"
                .to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/rtlmp.duckdb".to_string(),
        }
    }

    pub fn isone_ttc() -> TotalTransferCapabilityArchive {
        TotalTransferCapabilityArchive {
            base_dir: "/home/adrian/Downloads/Archive/IsoExpress/Ttc".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/isone/ttc.duckdb".to_string(),
        }
    }

    pub fn hq_hydro_data() -> HqHydroDataArchive {
        HqHydroDataArchive {
            base_dir: "/home/adrian/Downloads/Archive/HQ/HydroMeteorologicalData".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/hq_water_level.duckdb".to_string(),
        }
    }

    pub fn hq_total_demand_final() -> HqFinalizedTotalDemandArchive {
        HqFinalizedTotalDemandArchive {
            base_dir: "/home/adrian/Downloads/Archive/HQ/TotalDemandFinal".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/hq/total_demand.duckdb".to_string(),
        }
    }

    pub fn hq_total_demand_prelim() -> HqPrelimTotalDemandArchive {
        HqPrelimTotalDemandArchive {
            base_dir: "/home/adrian/Downloads/Archive/HQ/TotalDemandPrelim".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/hq/total_demand.duckdb".to_string(),
        }
    }

    pub fn hq_fuel_mix() -> HqFuelMixArchive {
        HqFuelMixArchive {
            base_dir: "/home/adrian/Downloads/Archive/HQ/FuelMix".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/hq/fuel_mix.duckdb".to_string(),
        }
    }

    pub fn nrc_generator_status() -> GeneratorStatusArchive {
        GeneratorStatusArchive {
            base_dir: "/home/adrian/Downloads/Archive/NRC/ReactorStatus".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/nrc_generation_status.duckdb"
                .to_string(),
        }
    }

    pub fn nyiso_dalmp() -> NyisoDalmpArchive {
        NyisoDalmpArchive {
            base_dir: "/home/adrian/Downloads/Archive/Nyiso/DaLmpHourly".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/nyiso/dalmp.duckdb".to_string(),
        }
    }

    pub fn nyiso_energy_offers() -> NyisoEnergyOffersArchive {
        NyisoEnergyOffersArchive {
            base_dir: "/home/adrian/Downloads/Archive/Nyiso/EnergyOffers".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/nyiso/nyiso_energy_offers.duckdb"
                .to_string(),
        }
    }

    pub fn nyiso_rtlmp() -> NyisoRtlmpArchive {
        NyisoRtlmpArchive {
            base_dir: "/home/adrian/Downloads/Archive/Nyiso/RtLmpHourly".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/nyiso/rtlmp.duckdb".to_string(),
        }
    }

    pub fn nyiso_scheduled_outages() -> NyisoScheduledOutagesArchive {
        NyisoScheduledOutagesArchive {
            base_dir: "/home/adrian/Downloads/Archive/Nyiso/TransmissionOutages/Scheduled"
                .to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/nyiso/scheduled_outages.duckdb"
                .to_string(),
        }
    }

    pub fn nyiso_transmission_outages_da() -> NyisoTransmissionOutagesDaArchive {
        NyisoTransmissionOutagesDaArchive {
            base_dir: "/home/adrian/Downloads/Archive/Nyiso/TransmissionOutages/DA".to_string(),
            duckdb_path:
                "/home/adrian/Downloads/Archive/DuckDB/nyiso/transmission_outages_da.duckdb"
                    .to_string(),
        }
    }

    pub fn scratch() -> ScratchArchive {
        ScratchArchive {
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/scratch.duckdb".to_string(),
        }
    }

    pub fn statistics_canada_generation() -> StatisticsCanadaGenerationArchive {
        StatisticsCanadaGenerationArchive {
            base_dir:
                "/home/adrian/Downloads/Archive/Canada/StatisticsCanada/ElectricPowerGeneration"
                    .to_string(),
            duckdb_path:
                "/home/adrian/Downloads/Archive/DuckDB/statistics_canada/energy_generation.duckdb"
                    .to_string(),
        }
    }

    pub fn sd_daasdt() -> SdDaasdtArchive {
        SdDaasdtArchive {
            base_dir: "/home/adrian/Downloads/Archive/Mis/SD_DAASDT".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/sd_daasdt.duckdb".to_string(),
        }
    }

    pub fn sd_rtload() -> SdRtloadArchive {
        SdRtloadArchive {
            base_dir: "/home/adrian/Downloads/Archive/Mis/SD_RTLOAD".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/sd_rtload.duckdb".to_string(),
        }
    }

    pub fn sr_rsvcharge2() -> SrRsvcharge2Archive {
        SrRsvcharge2Archive {
            base_dir: "/home/adrian/Downloads/Archive/Mis/SR_RSVCHARGE2".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/sr_rsvcharge2.duckdb".to_string(),
        }
    }

    pub fn sr_rsvstl2() -> SrRsvstl2Archive {
        SrRsvstl2Archive {
            base_dir: "/home/adrian/Downloads/Archive/Mis/SR_RSVSTL2".to_string(),
            duckdb_path: "/home/adrian/Downloads/Archive/DuckDB/sr_rsvstl2.duckdb".to_string(),
        }
    }
}


#[derive(Clone)]
pub struct ScratchArchive {
    pub duckdb_path: String,
}
