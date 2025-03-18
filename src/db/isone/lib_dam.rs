use jiff::{civil::Date, Zoned};
use std::error::Error;


pub fn is_dam_published(date: Date) -> Result<bool, Box<dyn Error>>  {
    let tomorrow = Zoned::now().date().tomorrow()?;
    match date.cmp(&tomorrow) {
        std::cmp::Ordering::Less => Ok(true),
        std::cmp::Ordering::Greater => Ok(false),
        std::cmp::Ordering::Equal => {
            let url = "https://www.iso-ne.com/isoexpress/web/reports/pricing/-/tree/lmps-da-hourly";
            let content = reqwest::blocking::get(url)?.text()?;   
            let tag = format!("WW_DALMP_ISO_{}.csv", date.strftime("%Y%m%d"));
            if content.contains(&tag) {
                return Ok(true);
            } 
            Ok(false)
        },
    }
}

#[cfg(test)]
mod tests {

    use jiff::civil::date;

    use super::*;
    use std::error::Error;

    #[ignore]
    #[test]
    fn check_status() -> Result<(), Box<dyn Error>> {
        assert!(is_dam_published(date(2025, 3, 18))?);
        assert!(!is_dam_published(date(2042, 3, 18))?);
        Ok(())
    }
}
