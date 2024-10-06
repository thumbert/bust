use std::fs::File;

use jiff::{civil::Date, Timestamp};



struct MisReport {
    report_name: String,
    account_id: String,
    report_date: Date,
    timestamp: Timestamp,
}

impl MisReport {

    /// Parse a filename to return an MisReport.
    /// SD_RTLOAD_000000003_2017060100_20190205151707.CSV
    fn from_filename(filename: &str) -> MisReport {
        let name = "".to_string();
        // let parts = filename.split(pat)
        MisReport { report_name: name, account_id: (), report_date: (), timestamp: () }
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
        assert_eq!(report.account_id, "000000003");
        assert_eq!(report.report_date, "2017-06-01".parse::<Date>()?);
        assert_eq!(report.timestamp, "2019-02-05T15:17:07Z".parse::<Timestamp>()?);
        Ok(())
    }
}
