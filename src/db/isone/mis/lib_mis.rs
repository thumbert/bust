use std::{fs::File, path::Path};

use jiff::{civil::{Date, Time}, Timestamp, Zoned};

pub struct MisReport {
    pub report_name: String,
    pub account_id: usize,
    pub report_date: Date,
    pub timestamp: Timestamp,
}

#[derive(Debug, Clone)]
struct MisReportError;

impl MisReport {
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

    use jiff::{civil::Date, Timestamp};

    use crate::db::isone::mis::lib_mis::MisReport;

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
