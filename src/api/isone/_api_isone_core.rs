use std::{
    fmt::{self},
    str::FromStr,
};

use jiff::Zoned;
use serde::{
    de::{self, Visitor},
    Deserialize, Deserializer, Serialize, Serializer,
};

pub fn serialize_zoned_as_offset<S>(z: &Zoned, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&z.strftime("%Y-%m-%d %H:%M:%S%:z").to_string())
}

// Custom deserialization function for the Zoned field
pub fn deserialize_zoned_assume_ny<'de, D>(deserializer: D) -> Result<Zoned, D::Error>
where
    D: Deserializer<'de>,
{
    struct ZonedVisitor;

    impl Visitor<'_> for ZonedVisitor {
        type Value = Zoned;

        fn expecting(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
            formatter.write_str("a timestamp string with or without a zone name")
        }

        fn visit_str<E>(self, v: &str) -> Result<Zoned, E>
        where
            E: de::Error,
        {
            // Otherwise, append the assumed zone
            let s = format!("{v}[America/New_York]");
            Zoned::strptime("%F %T%:z[%Q]", &s).map_err(E::custom)
        }
    }

    deserializer.deserialize_str(ZonedVisitor)
}

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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn test_market_from_str() {
        assert_eq!(Market::from_str("DA").unwrap(), Market::DA);
        assert_eq!(Market::from_str("RT").unwrap(), Market::RT);
        assert!(Market::from_str("INVALID").is_err());

        assert_eq!(Market::from_str("da").unwrap(), Market::DA);
        assert_eq!("da".parse::<Market>().unwrap(), Market::DA);
    }

    #[test]
    fn test_market_serde() {
        use serde_json;

        // Test serialization
        let da = Market::DA;
        let rt = Market::RT;
        let da_json = serde_json::to_string(&da).unwrap();
        let rt_json = serde_json::to_string(&rt).unwrap();
        assert_eq!(da_json, "\"DA\"");
        assert_eq!(rt_json, "\"RT\"");

        // Test deserialization (case-insensitive)
        let da2: Market = serde_json::from_str("\"DA\"").unwrap();
        let rt2: Market = serde_json::from_str("\"rt\"").unwrap();
        assert_eq!(da2, Market::DA);
        assert_eq!(rt2, Market::RT);

        // Test invalid deserialization
        let invalid: Result<Market, _> = serde_json::from_str("\"foo\"");
        assert!(invalid.is_err());
    }
}
