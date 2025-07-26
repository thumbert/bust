use jiff::{civil::Date, Zoned};
use std::cmp::Ordering::*;
use std::error::Error;

pub fn is_dalmp_published(date: Date) -> Result<bool, Box<dyn Error>> {
    let tomorrow = Zoned::now().date().tomorrow()?;
    match date.cmp(&tomorrow) {
        std::cmp::Ordering::Less => Ok(true),
        std::cmp::Ordering::Greater => Ok(false),
        std::cmp::Ordering::Equal => {
            if Zoned::now().hour() < 12 {
                return Ok(false);
            }
            let url = "https://www.iso-ne.com/isoexpress/web/reports/pricing/-/tree/lmps-da-hourly";
            let content = reqwest::blocking::get(url)?.text()?;
            let tag = format!("WW_DALMP_ISO_{}.csv", date.strftime("%Y%m%d"));
            if content.contains(&tag) {
                return Ok(true);
            }
            Ok(false)
        }
    }
}

pub fn is_rtlmp_published(date: Date) -> Result<bool, Box<dyn Error>> {
    let today = Zoned::now().date();
    if (today - date).get_days() > 7 {
        return Ok(true);
    }
    match date.cmp(&today) {
        Greater | Equal => Ok(false),
        Less => {
            let url = "https://www.iso-ne.com/isoexpress/web/reports/pricing/-/tree/lmps-rt-hourly-final";
            let content = reqwest::blocking::get(url)?.text()?;
            let tag = format!("lmp_rt_final_{}.csv", date.strftime("%Y%m%d"));
            if content.contains(&tag) {
                return Ok(true);
            }
            Ok(false)
        }
    }
}

#[cfg(test)]
mod tests {

    use jiff::civil::date;

    use super::*;
    use std::error::Error;

    #[ignore]
    #[test]
    fn check_status_da() -> Result<(), Box<dyn Error>> {
        assert!(is_dalmp_published(date(2025, 3, 18))?);
        assert!(!is_dalmp_published(date(2042, 3, 18))?);
        Ok(())
    }

    #[ignore]
    #[test]
    fn check_status_rt() -> Result<(), Box<dyn Error>> {
        assert!(is_rtlmp_published(date(2025, 7, 17))?);
        assert!(!is_dalmp_published(date(2042, 3, 18))?);
        Ok(())
    }


}
