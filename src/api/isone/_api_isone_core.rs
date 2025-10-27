use std::{
    fmt::{self},
    str::FromStr,
};

use serde::{Deserialize, Deserializer, Serialize};



#[derive(Copy, Clone, Debug, PartialEq, Serialize)]
pub enum Market {
    DA,
    RT,
}

// You want this so the Serde serializer doesn't print 'Da', etc. 
impl fmt::Display for Market {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Market::DA => write!(f, "DA"),
            Market::RT => write!(f, "RT"),
        }
    }
}

impl FromStr for Market {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "DA" => Ok(Market::DA),
            "RT" => Ok(Market::RT),
            _ => Err(format!("Can't parse market: {}", s)),
        }
    }
}

// Custom deserializer using FromStr so that Actix path path can parse different casing, e.g.
// "da" and "Da", not only the canonical one "DA".
impl<'de> Deserialize<'de> for Market {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        Market::from_str(&s).map_err(serde::de::Error::custom)
    }
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum BidOffer {
    Bid,
    Offer,
}

#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum ResourceType {
    Generating,
    Demand,
    Import,
}


#[derive(Debug, PartialEq, Serialize, Deserialize)]
pub enum UnitStatus {
    Economic,
    Unavailable,
    MustRun,
}

impl FromStr for UnitStatus {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "ECONOMIC" => Ok(UnitStatus::Economic),
            "UNAVAILABLE" => Ok(UnitStatus::Unavailable),
            "MUST_RUN" => Ok(UnitStatus::MustRun),
            _ => Err(format!("Can't parse unit status: {}", s)),
        }
    }
}

