use std::path::Path;

use jiff::{
    civil::{Date, Time},
    Timestamp, ToSpan, Zoned,
};

pub struct MisReport {
    pub report_name: String,
    pub account_id: usize,
    pub report_date: Date,
    pub timestamp: Timestamp,
}

impl MisReport {
    /// Parse a tuple of (date, hour_ending) into an hour beginning.
    /// ISONE represents the hour as '01', '02', '02X', '03', ... '24'.
    ///
    /// Returned zoned is an **hour beginning** in America/New_York
    pub fn parse_hour_ending(date: Date, hour: &str) -> Zoned {
        let h: i8 = hour[0..2].parse().unwrap();
        let mut res = date.at(h-1, 0, 0, 0).intz("America/New_York").unwrap();

        if hour == "02X" {
            res = res.saturating_add(1.hour());
        }

        res
    }

    /// Parse a filename to return an MisReport.
    /// SD_RTLOAD_000000003_2017060100_20190205151707.CSV
    pub fn from_filename(filename: &str) -> MisReport {
        let path = Path::new(filename);
        let mut parts: Vec<&str> = path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .split("_")
            .collect();
        parts.reverse();

        // timestamp is from parts[0]
        let date = Date::strptime("%Y%m%d", parts[0].get(0..8).unwrap()).unwrap();
        let time = Time::strptime("%H%M%S", parts[0].get(8..).unwrap()).unwrap();
        let zdt = date
            .at(time.hour(), time.minute(), time.second(), 0)
            .intz("UTC")
            .unwrap();
        let timestamp = zdt.timestamp();

        let report_date = parts[1].to_string()[..8].parse::<Date>().unwrap();
        let account_id = parts[2].parse::<usize>().unwrap();
        let rn: Vec<&str> = parts[3..].iter().copied().rev().collect();
        let report_name = rn.join("_");

        MisReport {
            report_name,
            account_id,
            report_date,
            timestamp,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use jiff::{civil::Date, Timestamp, Zoned};

    use crate::db::isone::mis::lib_mis::MisReport;

    #[test]
    fn isone_hour_ending() -> Result<(), Box<dyn Error>> {
        let xs = [
            ("2015-11-01", "01"),
            ("2015-11-01", "02"),
            ("2015-11-01", "02X"),
            ("2015-11-01", "03"),
            ("2015-11-01", "04"),
        ];
        let he: Vec<Zoned> = xs
            .iter()
            .map(|e| -> Zoned {
                let date: Date = e.0.parse().unwrap();
                MisReport::parse_hour_ending(date, e.1)
            })
            .collect();

        assert_eq!(
            he[0],
            "2015-11-01T00:00:00-04:00[America/New_York]".parse()?
        );
        assert_eq!(
            he[1],
            "2015-11-01T01:00:00-04:00[America/New_York]".parse()?
        );
        assert_eq!(
            he[2],
            "2015-11-01T01:00:00-05:00[America/New_York]".parse()?
        );
        assert_eq!(
            he[3],
            "2015-11-01T02:00:00-05:00[America/New_York]".parse()?
        );
        assert_eq!(
            he[4],
            "2015-11-01T03:00:00-05:00[America/New_York]".parse()?
        );
        Ok(())
    }

    #[test]
    fn from_filename() -> Result<(), Box<dyn Error>> {
        let filename = "SD_RTLOAD_000000003_2017060100_20190205151707.CSV";
        let report = MisReport::from_filename(filename);
        assert_eq!(report.report_name, "SD_RTLOAD".to_string());
        assert_eq!(report.account_id, 3);
        assert_eq!(report.report_date, "2017-06-01".parse::<Date>()?);
        assert_eq!(
            report.timestamp,
            "2019-02-05T15:17:07Z".parse::<Timestamp>()?
        );
        Ok(())
    }
}
