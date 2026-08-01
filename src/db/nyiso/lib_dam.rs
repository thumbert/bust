use jiff::{civil::Date, Zoned};
use std::error::Error;

/// Check if the Day-Ahead LMP file (DALMP) for the given date has been published
/// on the public NYISO website.
pub fn is_dalmp_published(date: Date) -> Result<bool, Box<dyn Error>> {
    let tomorrow = Zoned::now().date().tomorrow()?;
    match date.cmp(&tomorrow) {
        std::cmp::Ordering::Less => Ok(true),
        std::cmp::Ordering::Greater => Ok(false),
        std::cmp::Ordering::Equal => {
            if Zoned::now().hour() < 9 {
                return Ok(false);
            }
            let tag = format!("{}damlbmp_zone.csv", date.strftime("%Y%m%d"));
            let url = "https://mis.nyiso.com/public/P-2Alist.htm";
            let content = reqwest::blocking::get(url)?.text()?;
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
        assert!(is_dalmp_published(date(2026, 8, 2))?);
        assert!(!is_dalmp_published(date(2042, 3, 18))?);
        Ok(())
    }
}
