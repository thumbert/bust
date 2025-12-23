use std::fmt::{self};

use jiff::Zoned;
use serde::{
    de::{self, Visitor},
    Deserializer, Serializer,
};

pub fn serialize_zoned_as_offset<S>(z: &Zoned, serializer: S) -> Result<S::Ok, S::Error>
where
    S: Serializer,
{
    serializer.serialize_str(&z.strftime("%Y-%m-%d %H:%M:%S%:z").to_string())
}

// Custom deserialization function for the Zoned field
pub fn deserialize_zoned_assume_la<'de, D>(deserializer: D) -> Result<Zoned, D::Error>
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
            let s = format!("{v}[America/Los_Angeles]");
            Zoned::strptime("%F %T%:z[%Q]", &s).map_err(E::custom)
        }
    }

    deserializer.deserialize_str(ZonedVisitor)
}
