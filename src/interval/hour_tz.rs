// use std::fmt::{Debug, Formatter};
// use std::{fmt, panic};

use std::fmt::{self, Formatter};

use jiff::{civil::DateTime, ToSpan, Unit, Zoned};

use crate::interval::interval::IntervalTzLike;

#[derive(Debug, Clone, Eq, PartialEq, Hash)]
pub struct Hour {
    start: DateTime,
}

#[derive(Debug, Clone, Eq, PartialEq, PartialOrd, Hash)]
pub struct HourTz {
    start: Zoned,
}

impl IntervalTzLike for HourTz {
    fn start(&self) -> Zoned {
        self.start.clone()
    }
    fn end(&self) -> Zoned {
        self.start.saturating_add(1.hours())
    }
}

impl HourTz {
    /// Return the hour that contains this datetime.
    pub fn containing(dt: Zoned) -> HourTz {
        let start = dt.with().minute(0).second(0).nanosecond(0).build().unwrap();
        HourTz { start }
    }

    pub fn next(&self) -> HourTz {
        HourTz { start: self.end() }
    }

    pub fn previous(&self) -> HourTz {
        HourTz { start: self.start.saturating_sub(1.hours()) }
    }
}

impl fmt::Display for HourTz {
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        let out = format!(
            "[{}, {})",
            self.start.strftime("%Y-%m-%dT%H:%M:%S%:z"),
            self.end().strftime("%Y-%m-%dT%H:%M:%S%:z")
        );
        f.write_str(&out)
    }
}

#[cfg(test)]
mod tests {

    use jiff::Zoned;

    use crate::interval::{
        hour_tz::{Hour, HourTz},
        interval::IntervalLike,
    };

    #[test]
    fn test_hourtz() {
        let dt = "2022-04-15T03:15:20[America/New_York]"
            .parse::<Zoned>()
            .unwrap();
        let hour = HourTz::containing(dt);
        assert_eq!(hour.start.hour(), 3);
        assert_eq!(hour.start.day(), 15);
        assert_eq!(hour.start.month(), 4);
        assert_eq!(hour.start.year(), 2022);
        assert_eq!(
            hour.next(),
            HourTz {
                start: "2022-04-15T04:00:00[America/New_York]"
                    .parse::<Zoned>()
                    .unwrap()
            }
        );
        assert_eq!(
            hour.to_string(),
            "[2022-04-15T03:00:00-04:00, 2022-04-15T04:00:00-04:00)"
        );
    }

    // #[test]
    // fn hour_ny() {
    //     let hour = Hour::new(2023, 3, 1, 0, New_York).unwrap();
    //     assert_eq!(format!("{}", hour), "2023-03-01 00 -05:00");
    //     let hour = hour.next();
    //     assert_eq!(format!("{}", hour), "2023-03-01 01 -05:00");
    //     assert!(hour.contains(New_York.with_ymd_and_hms(2023, 3, 1, 1, 17, 24).unwrap()));
    // }

    // #[test]
    // fn hour_dst() {
    //     let hour0 = Hour::new(2022, 11, 6, 0, New_York).unwrap();
    //     assert_eq!(format!("{}", hour0), "2022-11-06 00 -04:00");
    //     let hour1 = hour0.next();
    //     assert_eq!(format!("{}", hour1), "2022-11-06 01 -04:00");
    //     let hour2 = hour1.next();
    //     assert_eq!(format!("{}", hour2), "2022-11-06 01 -05:00");
    //     //
    //     let dt = New_York.timestamp_opt(1667710800, 0).unwrap(); // "2022-11-06T01:16:40-04:00"
    //     let hour = Hour::containing(dt);
    //     assert_eq!(format!("{}", hour), "2022-11-06 01 -04:00");
    //     let dt = dt + Duration::hours(1);
    //     let hour = Hour::containing(dt);
    //     assert_eq!(format!("{}", hour), "2022-11-06 01 -05:00");
    // }
}
