use std::{env, error::Error, fmt::Display, fs::File, io, str::FromStr};
use serde::Serialize;
use crate::interval::month::Month;

#[derive(Debug, PartialEq, Clone, Copy, Eq, Ord, PartialOrd, Serialize)]
pub enum ResourceType {
    Generating,
    Demand,
    Import,
}

impl FromStr for ResourceType {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Generating" => Ok(ResourceType::Generating),
            "Demand" => Ok(ResourceType::Demand),
            "Import" => Ok(ResourceType::Import),
            _ => Err(format!("Failed to parse {s} as ResourceType")),
        }
    }
}

#[derive(Debug, PartialEq, Clone, Copy, Eq, PartialOrd, Ord, Serialize)]
pub enum BidOffer {
    Bid,
    Offer,
}

impl FromStr for BidOffer {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "Supply_Offer" => Ok(BidOffer::Offer),
            "Demand_Bid" => Ok(BidOffer::Bid),
            _ => Err(format!("Failed to parse {s} as BidOffer")),
        }
    }
}

#[derive(PartialEq, Debug, Clone, PartialOrd, Serialize)]
#[serde(rename_all = "PascalCase")]
pub struct MraRecord {
    pub month: usize,
    pub masked_resource_id: usize,
    pub masked_lead_participant_id: usize,
    pub masked_capacity_zone_id: usize,
    pub resource_type: ResourceType,
    pub masked_external_interface_id: Option<usize>,
    pub bid_offer: BidOffer,
    pub segment: u8,
    pub quantity: f32,
    pub price: f32,
}

pub struct MraCapacityArchive {
    path: String,
}

impl MraCapacityArchive {
    pub fn new() -> Self {
        MraCapacityArchive {
            path: "/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction"
                .to_string(),
        }
    }

    // https://www.iso-ne.com/isoexpress/web/reports/auctions/-/tree/forward-capacity-mkt
    pub fn get_file(&self, month: Month) -> io::Result<File> {
        let path = format!("{}/Raw/hbfcmmra_{}.csv", self.path, month);
        // println!("{}", path);
        File::open(path)
    }

    /// Get a csv ready for insertion into DuckDb
    pub fn prep_csv(&self, month: Month) -> io::Result<()> {
        let path = format!("{}/tmp/mra_{}.csv", self.path, month);
        let mut wtr = csv::Writer::from_path(path).unwrap();
        let file = self.get_file(month).unwrap();
        let rs = self.read_file(file).unwrap();
        for r in rs.clone() {
            // wtr.write_record()?;
            wtr.serialize(r)?;
        }
        wtr.flush()?;
        Ok(())
    }

    pub fn read_file(&self, file: File) -> Result<Vec<MraRecord>, Box<dyn Error>> {
        let mut rdr = csv::ReaderBuilder::new()
            .has_headers(false)
            .flexible(true)
            .from_reader(file);

        let mut res: Vec<MraRecord> = Vec::new();
        for result in rdr.records() {
            let record = result?;
            if record.get(0).unwrap() != "D" {
                continue;
            }
            let month = 202401; // todo!
            for i in 0..4 {
                match record.get(9 + usize::from(i)*2).unwrap().parse::<f32>() {
                    Ok(quantity) => {
                        let one = MraRecord {
                            month,
                            masked_resource_id: record.get(3).unwrap().parse::<usize>().unwrap(),
                            masked_lead_participant_id: record.get(4).unwrap().parse::<usize>().unwrap(),
                            masked_capacity_zone_id: record.get(5).unwrap().parse::<usize>().unwrap(),
                            resource_type: record.get(6).unwrap().parse::<ResourceType>().unwrap(),
                            masked_external_interface_id: if record.get(7).unwrap() == "" {
                                None
                            } else {
                                Some(record.get(7).unwrap().parse::<usize>().unwrap())
                            },
                            bid_offer: record.get(8).unwrap().parse::<BidOffer>().unwrap(),
                            segment: i,
                            quantity, 
                            price: record
                            .get(10 + usize::from(i)*2)
                            .unwrap()
                            .parse::<f32>()
                            .unwrap(),
                        };
                        res.push(one);
                    },
                    Err(_) => continue,
                }
            }
            // println!("{:?}", record);
        }
        Ok(res)
    }

}

#[cfg(test)]
mod tests {
    use chrono_tz::Tz;

    use super::MraCapacityArchive;
    use crate::{interval::month::Month, isone::monthly_capacity_auction_archive::{BidOffer, MraRecord}};

    #[test]
    fn read_file() {
        let month = Month::new(2024, 1, Tz::UTC).unwrap();
        let archive = MraCapacityArchive::new();
        let file = archive.get_file(month).unwrap();
        let rs = archive.read_file(file).unwrap();
        println!("{:?}", rs[0]);
        println!("Found {} records", rs.len());
        let mut bids: Vec<MraRecord> = rs.clone().into_iter()
            .filter(|x| x.bid_offer == BidOffer::Bid).collect();
        let mut offers: Vec<MraRecord> = rs.clone().into_iter()
            .filter(|x| x.bid_offer == BidOffer::Offer).collect();
        // sort bids decreasingly and offers increasingly
        bids.sort_by(|a,b| b.price.partial_cmp(&a.price).unwrap());
        offers.sort_by(|a,b| a.price.partial_cmp(&b.price).unwrap());
        // for b in bids {
        //     println!("{:?}", b);
        // }

    }
}
