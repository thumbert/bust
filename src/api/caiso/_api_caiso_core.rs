use std::{fmt::{self}, str::FromStr};

use serde::{
    Deserialize, Deserializer, Serialize,
};

#[derive(Debug, Serialize, Clone, PartialEq, Copy)]
pub enum LmpComponent {
    // locational marginal price
    Lmp,
    // marginal cost losses
    Mcl,
    // marginal cost congestion
    Mcc,
    /// marginal cost green house gases
    Mghg,
}

impl fmt::Display for LmpComponent {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        match self {
            LmpComponent::Lmp => write!(f, "lmp"),
            LmpComponent::Mcl => write!(f, "mcl"),
            LmpComponent::Mcc => write!(f, "mcc"),
            LmpComponent::Mghg => write!(f, "mghg"),
        }
    }
}

fn parse_component(s: &str) -> Result<LmpComponent, String> {
    match s.to_lowercase().as_str() {
        "lmp" => Ok(LmpComponent::Lmp),
        "mcl" => Ok(LmpComponent::Mcl),
        "mlc" => Ok(LmpComponent::Mcl), // alias for Mcl
        "mcc" => Ok(LmpComponent::Mcc),
        "mghg" => Ok(LmpComponent::Mghg),
        _ => Err(format!("Unknown LMP component: {}", s)),
    }
}

impl FromStr for LmpComponent {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match parse_component(s) {
            Ok(component) => Ok(component),
            Err(_) => Err(format!("Failed parsing {} as an Lmp component", s)),
        }
    }
}

// Custom deserializer using FromStr so that Actix path path can parse different casing, e.g.
// "lmp" and "LMP", not only the canonical one "Lmp".
impl<'de> Deserialize<'de> for LmpComponent {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        LmpComponent::from_str(&s).map_err(serde::de::Error::custom)
    }
}
