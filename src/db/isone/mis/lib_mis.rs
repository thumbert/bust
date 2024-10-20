use std::{fs::File, path::Path};

use jiff::{civil::Date, Timestamp, Zoned};

struct MisReport {
    report_name: String,
    account_id: usize,
    report_date: Date,
    timestamp: Timestamp,
}

impl MisReport {
    /// Parse a filename to return an MisReport.
    /// SD_RTLOAD_000000003_2017060100_20190205151707.CSV
    fn from_filename(filename: &str) -> MisReport {
        let path = Path::new(filename);
        let mut parts: Vec<&str> = path
            .file_stem()
            .unwrap()
            .to_str()
            .unwrap()
            .split("_")
            .collect();
        parts.reverse();
        println!("{:?}", parts);
        let zdt = Zoned::strptime("%Y%m%d%H%M%S%Z", format!("{}Z", parts[0]));
        println!("{:?}", zdt);
        let timestamp = parts[0].parse::<Timestamp>().unwrap();
        let report_date = parts[1].to_string()[..10].parse::<Date>().unwrap();
        let account_id = parts[2].parse::<usize>().unwrap();
        let report_name = parts[3..].join("");
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

    use jiff::civil::Date;

    use crate::isone::mis::lib_mis::*;

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
