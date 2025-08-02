
use once_cell::sync::Lazy;

pub static ISONE: Lazy<Iso> = Lazy::new(|| Iso {
    name: "ISONE",
    tz: jiff::tz::TimeZone::get("America/New_York").unwrap(),
});

pub struct Iso {
    pub name: &'static str,
    pub tz: jiff::tz::TimeZone,
}

