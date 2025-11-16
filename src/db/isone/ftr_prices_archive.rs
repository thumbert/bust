use std::error::Error;
use std::path::Path;

use crate::interval::month::Month;

#[derive(Clone)]
pub struct IsoneFtrPricesArchive {
    pub base_dir: String,
    pub duckdb_path: String,
}

pub enum AuctionType {
    LongTerm1,
    LongTerm2,
    Monthly,
}

impl IsoneFtrPricesArchive {
    /// Return the json filename for this auction.  
    pub fn filename(&self, auction_type: AuctionType, month: &Month) -> String {
        self.base_dir.to_owned()
            + "/Raw"
            + "/ftr_clearing_prices_"
            + &month.to_string()
            + match auction_type {
                AuctionType::LongTerm1 => "_longterm1",
                AuctionType::LongTerm2 => "_longterm2",
                AuctionType::Monthly => "_monthly",
            }
            + ".json"
    }

        pub fn filename_year(&self, auction_type: AuctionType, year: i32) -> String {
        self.base_dir.to_owned()
            + "/Raw"
            + "/ftr_clearing_prices_"
            + &year.to_string()
            + match auction_type {
                AuctionType::LongTerm1 => "_longterm1",
                AuctionType::LongTerm2 => "_longterm2",
                AuctionType::Monthly => "_monthly",
            }
            + ".json"
    }



    pub fn download_file_month(&self, auction_type: AuctionType, month: Month) -> Result<(), Box<dyn Error>> {
        super::lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/ftrauctionclearingprices/{}/month/{}",
                match auction_type {
                    AuctionType::LongTerm1 => "long_term_1",
                    AuctionType::LongTerm2 => "long_term_2",
                    AuctionType::Monthly => "monthly",
                },
                month
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename(auction_type, &month)),
            true,
        )
    }

    pub fn download_file_year(&self, auction_type: AuctionType, year: i32) -> Result<(), Box<dyn Error>> {
        super::lib_isoexpress::download_file(
            format!(
                "https://webservices.iso-ne.com/api/v1.1/ftrauctionclearingprices/{}/year/{}",
                match auction_type {
                    AuctionType::LongTerm1 => "long_term_1",
                    AuctionType::LongTerm2 => "long_term_2",
                    AuctionType::Monthly => "monthly",
                },
                year
            ),
            true,
            Some("application/json".to_string()),
            Path::new(&self.filename_year(auction_type, year)),
            true,
        )
    }
}

#[cfg(test)]
mod tests {

    use std::{error::Error, path::Path};

    use crate::db::{isone::ftr_prices_archive::*, prod_db::ProdDb};

    #[ignore]
    #[test]
    fn download_file() -> Result<(), Box<dyn Error>> {
        dotenvy::from_path(Path::new(".env/test.env")).unwrap();
        let archive = ProdDb::isone_ftr_cleared_prices();

        for year in 2019..=2025 {
            archive.download_file_year(AuctionType::LongTerm1, year)?;
            archive.download_file_year(AuctionType::LongTerm2, year)?;
            // archive.download_file_year(AuctionType::Monthly, year)?;
        }

        Ok(())
    }
}
