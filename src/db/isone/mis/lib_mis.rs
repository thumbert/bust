use std::{
    error::Error,
    fmt::Display,
    fs,
    hash::{Hash, Hasher},
    path::Path,
};

use jiff::{
    civil::{Date, Time},
    Timestamp, ToSpan, Zoned,
};

#[derive(Debug, Eq, PartialEq, Clone)]
pub struct MisReportInfo {
    pub report_name: String,
    pub account_id: usize,
    /// Date the report applies to.  If the report is a monthly report, it's usually
    /// the first of the month.
    pub report_date: Date,
    /// The timestamp the report was published by the ISO as the  
    /// number of seconds since Unix Epoch.  
    pub version: Timestamp,
}

impl Hash for MisReportInfo {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.report_name.hash(state);
        self.account_id.hash(state);
        self.report_date.hash(state);
        self.version.as_second().hash(state);
    }
}

impl MisReportInfo {
    /// Return the file name as produced by the ISO.
    pub fn filename_iso(&self) -> String {
        format!(
            "{}_{:0>9}_{}_{}.CSV",
            self.report_name,
            self.account_id,
            self.report_date.strftime("%Y%m%d00"),
            self.version.strftime("%Y%m%d%H%M%S")
        )
    }
}

impl Display for MisReportInfo {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "{{report_name: {}, account_id: {}, report_date: {}, version: {}}}",
            self.report_name, self.account_id, self.report_date, self.version
        )
    }
}

impl From<String> for MisReportInfo {
    /// # Arguments
    /// * filename - a fully qualified path, or a relative path
    ///
    fn from(filename: String) -> Self {
        let path = Path::new(&filename);
        let filename_iso = path.file_stem().unwrap().to_str().unwrap();
        let mut parts: Vec<&str> = filename_iso.split("_").collect();
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
        let report_name = rn.join("_").to_ascii_uppercase();

        MisReportInfo {
            report_name,
            account_id,
            report_date,
            version: timestamp,
        }
    }
}

pub trait MisReport {}

fn is_header_row(line: &str) -> bool {
    line.starts_with(r#""H""#) || line.starts_with("'H'") || line.starts_with("H")
}

fn is_data_row(line: &str) -> bool {
    line.starts_with(r#""D""#) || line.starts_with("'D'") || line.starts_with("D")
}

/// Get the header info and the data rows corresponding to the nth tab.
///
/// # Arguments
/// * `n` - the tab number
/// * `lines` - all the report content
///
pub fn extract_tab(n: isize, lines: &Vec<String>) -> Result<MisTab, Box<dyn Error>> {
    let mut n_headers: isize = -1;
    let mut tab_counter = 0;
    let mut header: Vec<String> = Vec::new();
    let mut header_info: Vec<String> = Vec::new();
    let mut tab_lines: Vec<String> = Vec::new();

    for line in lines {
        if is_header_row(line) {
            n_headers += 1;
            tab_counter = n_headers / 2;
        }
        if tab_counter != n {
            continue;
        }
        if is_data_row(line) {
            tab_lines.push(line.clone());
        }

        if is_header_row(line) {
            if n_headers % 2 == 0 {
                let mut rdr = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(line.as_bytes());
                for record in rdr.records() {
                    let mut vs: Vec<String> = record
                        .unwrap()
                        .iter()
                        .map(|e| e.to_string())
                        .filter(|e| !e.is_empty())
                        //.skip(1)
                        .collect();
                    header.append(&mut vs);
                }
            } else {
                let mut rdr = csv::ReaderBuilder::new()
                    .has_headers(false)
                    .from_reader(line.as_bytes());
                for record in rdr.records() {
                    let mut vs: Vec<String> = record
                        .unwrap()
                        .iter()
                        .map(|e| e.to_string())
                        .filter(|e| !e.is_empty())
                        //.skip(1)
                        .collect();
                    header_info.append(&mut vs);
                }
            }
        }
    }

    Ok(MisTab {
        header,
        header_info,
        lines: tab_lines,
    })
}

/// Parse a tuple of (date, hour_ending) into an hour beginning.
///
/// # Arguments
/// * `date` - the report date
/// * `hour` - ISONE represents the hour as '01', '02', '02X', '03', ... '24'.  But not
///   always.  Sometimes the output is '1', '2', '02X', etc.  
///
/// Returned zoned is an **hour beginning** in America/New_York
pub fn parse_hour_ending(date: &Date, hour: &str) -> Zoned {
    let h: i8 = if hour.len() == 1 {
        hour[0..1].parse().unwrap()
    } else {
        hour[0..2].parse().unwrap()
    };
    let mut res = date.at(h - 1, 0, 0, 0).intz("America/New_York").unwrap();

    if hour == "02X" {
        res = res.saturating_add(1.hour());
    }

    res
}

/// Read the report and return the lines as strings
pub fn read_report(filename: &str) -> Result<Vec<String>, Box<dyn Error>> {
    let mut lines = Vec::new();
    for line in fs::read_to_string(filename).unwrap().lines() {
        lines.push(line.to_string());
    }
    if lines.is_empty() {
        return Err(From::from("empty file, no content"));
    }
    Ok(lines)
}

#[derive(Debug, Clone)]
pub struct MisTab {
    /// Column names
    pub header: Vec<String>,
    /// Additional info for the column that the ISO provides, e.g. Number, String, MWh, $/MWh, %, Hour End, etc.
    pub header_info: Vec<String>,
    /// each element is an unprocessed (not split) data line
    pub lines: Vec<String>,
}

#[cfg(test)]
mod tests {
    use std::error::Error;

    use jiff::{civil::Date, Timestamp, Zoned};

    use crate::db::isone::mis::lib_mis::*;

    #[test]
    fn read_tab() -> Result<(), Box<dyn Error>> {
        let path = "../elec-server/test/_assets/sd_daasdt_000000002_2024111500_20241203135151.csv";
        let info = MisReportInfo::from(path.to_string());
        assert_eq!(info.account_id, 2);
        let lines = read_report(path).unwrap();
        assert_eq!(lines.len(), 198);

        // let tab0 = report.extract_tab(0, lines);
        // println!("{:?}", tab0.as_ref().unwrap().header);
        // assert_eq!(tab0.as_ref().unwrap().lines.len(), 57);

        Ok(())
    }

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
                parse_hour_ending(&date, e.1)
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
        let report = MisReportInfo::from(filename.to_string());
        assert_eq!(report.report_name, "SD_RTLOAD".to_string());
        assert_eq!(report.account_id, 3);
        assert_eq!(report.report_date, "2017-06-01".parse::<Date>()?);
        assert_eq!(report.version, "2019-02-05T15:17:07Z".parse::<Timestamp>()?);
        Ok(())
    }

    #[test]
    fn to_filename() -> Result<(), Box<dyn Error>> {
        let filename = "SD_RTLOAD_000000003_2017060100_20190205151707.CSV";
        let report = MisReportInfo::from(filename.to_string());
        assert_eq!(filename, report.filename_iso());
        Ok(())
    }
}
