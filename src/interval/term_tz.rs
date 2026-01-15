use std::{fmt, str::FromStr};

use jiff::{ToSpan, Zoned, civil::date, tz::TimeZone};
use serde::{Deserialize, Deserializer};

use crate::interval::{
    date_tz::DateTz,
    hour_tz::HourTz,
    interval_base::{DateExt, IntervalTzLike},
    month_tz::MonthTz,
    term::{Term, TermType},
};


#[derive(Clone, Debug, PartialEq)]
pub struct TermTz {
    pub start_date: DateTz,
    pub end_date: DateTz,
}

impl TermTz {
    pub fn new(start: DateTz, end: DateTz) -> Option<TermTz> {
        if start.start().time_zone() != end.end().time_zone() || end < start {
            return None;
        }
        Some(TermTz {
            start_date: start,
            end_date: end,
        })
    }

    /// Determining a TermType is a pretty expensive operation.
    pub fn term_type(&self) -> TermType {
        Term::new(self.start().date(), self.end().date())
            .unwrap()
            .term_type()
    }

    /// Return the hours in the term
    pub fn hours(&self) -> Vec<HourTz> {
        let mut hours = Vec::new();
        let mut current = self.start();
        let end_dt = self.end_date.end();
        while current < end_dt {
            hours.push(HourTz::containing(&current));
            current = current.saturating_add(1.hour());
        }
        hours
    }

    /// Return the days in the term
    pub fn days(&self) -> Vec<DateTz> {
        let mut days = Vec::new();
        let mut current = self.start_date.start().date();
        let end_date = self.end_date.end().date();
        while current <= end_date {
            days.push(current.with_tz(self.start().time_zone()));
            current = current.saturating_add(1.days());
        }
        days
    }

    /// Returns the months in this term.  If the term is not an exact month or
    /// month range, return the minimal vector of months that cover the term.   
    pub fn months(&self) -> Vec<MonthTz> {
        let mut months = Vec::new();
        let mut current = MonthTz::containing(self.start());
        let end_month = MonthTz::containing(self.end());
        while current < end_month {
            months.push(MonthTz::containing(current.start()));
            let next = date(current.start().year(), current.start().month(), 1)
                .saturating_add(1.months())
                .with_tz(self.start().time_zone());
            current = MonthTz::containing(next.start());
        }
        months
    }

    pub fn to_term(&self) -> Term {
        Term::new(self.start_date.to_date(), self.end_date.to_date()).unwrap()
    }
}

impl IntervalTzLike for TermTz {
    fn start(&self) -> Zoned {
        self.start_date.start()
    }
    fn end(&self) -> Zoned {
        self.end_date.end()
    }
}

impl FromStr for TermTz {
    type Err = String;
    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let ps = s.split('[').collect::<Vec<&str>>();
        let term = ps[0].parse::<Term>();
        if term.is_err() {
            return Err(format!("Failed parsing {} as a Term", s));
        }
        let term = term.unwrap();
        let tz_str = if ps.len() > 1 {
            ps[1].trim_end_matches(']')
        } else {
            return Err(format!("No time zone found in TermTz string {}", s));
        };
        let tz = TimeZone::get(tz_str);
        if tz.is_err() {
            return Err(format!(
                "Failed getting time zone {} in TermTz string {}",
                tz_str, s
            ));
        }
        Ok(term.with_tz(&tz.unwrap()))
    }
}


impl fmt::Display for TermTz {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let binding = self.start();
        let tz = binding.time_zone();
        write!(
            f,
            "{}[{}]",
            self.to_term(),
            tz.iana_name()
                .map(|s| s.to_string())
                .unwrap_or_else(|| format!("{:?}", tz))
        )
    }
}

impl serde::Serialize for TermTz {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        let s = self.to_string();
        serializer.serialize_str(&s)
    }
}


// Custom deserializer using FromStr so that Actix path path can parse different casing.
impl<'de> Deserialize<'de> for TermTz {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: Deserializer<'de>,
    {
        let s = String::deserialize(deserializer)?;
        TermTz::from_str(&s).map_err(serde::de::Error::custom)
    }
}


#[cfg(test)]
mod tests {

    use crate::{elec::iso::ISONE, interval::{date_tz::DateTz, term::*, term_tz::TermTz}};

    #[test]
    fn test_fmt() {
        let term = "F25".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        assert_eq!(term.to_string(), "Jan25[America/New_York]");
    }

    #[test]
    fn test_parse() -> Result<(), String> {
        let term = "3Jan25[America/New_York]".parse::<TermTz>()?;
        assert_eq!(term.start_date, "2025-01-03[America/New_York]".parse::<DateTz>()?);
        assert_eq!(term.end_date, "2025-01-03[America/New_York]".parse::<DateTz>()?);
        Ok(())
    }


    #[test]
    fn test_hours() {
        let term = "2025".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        assert_eq!(term.hours().len(), 8760);
    }
}
