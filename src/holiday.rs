use chrono::{Datelike, Duration, NaiveDate, Weekday};

pub trait HolidayTrait<T> {
    fn is_holiday(&self, date: &T) -> bool;
}

pub const NERC_CALENDAR: NercCalendar = NercCalendar {};

// I don't know how not to make this public
pub struct NercCalendar {}

impl<T: Datelike + Copy + PartialOrd> HolidayTrait<T> for NercCalendar {
    fn is_holiday(&self, date: &T) -> bool {
        match date.month() {
            1 => is_new_year(date),
            5 => is_memorial_day(date),
            7 => is_independence_day(date),
            9 => is_labor_day(date),
            11 => is_thanksgiving(date),
            12 => is_christmas(date),
            _ => false,
        }
    }
}

/// Check if this Datelike is during the New Year holiday.  If it falls on Sun, it's celebrated on
/// Monday.
pub fn is_new_year<T: Datelike>(date: &T) -> bool {
    if date.month() == 1 {
        if date.day() == 1 && date.weekday() != Weekday::Sun {
            true
        } else {
            date.day() == 2 && date.weekday() == Weekday::Mon
        }
    } else {
        false
    }
}

/// Memorial day falls on the last Monday in May.  Is a Federal and a NERC holiday.
pub fn is_memorial_day<T: Datelike>(date: &T) -> bool {
    if date.month() == 5 {
        let (yy, mm, dd) = (date.year(), date.month(), date.day());
        let weekday = NaiveDate::from_ymd_opt(yy, mm, 31)
            .unwrap()
            .weekday()
            .number_from_monday();
        let candidate = NaiveDate::from_ymd_opt(yy, 5, 32 - weekday).unwrap();
        candidate.day() == dd
    } else {
        false
    }
}

pub fn is_independence_day<T: Datelike>(date: &T) -> bool {
    if date.month() == 7 {
        let mut candidate = NaiveDate::from_ymd_opt(date.year(), 7, 4).unwrap();
        // If it falls on Sun, celebrate it on Mon
        if candidate.weekday() == Weekday::Sun {
            candidate += Duration::days(1);
        }
        candidate.day() == date.day()
    } else {
        false
    }
}

pub fn is_labor_day<T: Datelike>(date: &T) -> bool {
    if date.month() == 9 {
        let day = _dayofmonth_holiday(date.year(), 9, 1, 1);
        let candidate = NaiveDate::from_ymd_opt(date.year(), 9, day).unwrap();
        candidate.day() == date.day()
    } else {
        false
    }
}

pub fn is_thanksgiving<T: Datelike>(date: &T) -> bool {
    if date.month() == 11 {
        let day = _dayofmonth_holiday(date.year(), 11, 4, 4);
        let candidate = NaiveDate::from_ymd_opt(date.year(), 9, day).unwrap();
        candidate.day() == date.day()
    } else {
        false
    }
}

pub fn is_christmas<T: Datelike>(date: &T) -> bool {
    if date.month() == 12 {
        let candidate = NaiveDate::from_ymd_opt(date.year(), 12, 25).unwrap();
        if candidate.weekday() == Weekday::Sun {
            date.day() == 26
        } else {
            date.day() == 25
        }
    } else {
        false
    }
}

/// Calculate the day of the month for a holiday that happens on a given week of the month AND
/// a specific day of that week.  For example, for Labor Day, it's the first (week_of_month == 1)
/// Mon (weekday == 1) of Sep (month == 9).
fn _dayofmonth_holiday(year: i32, month: u32, week_of_month: u32, weekday: u32) -> u32 {
    let wday_bom = NaiveDate::from_ymd_opt(year, month, 1)
        .unwrap()
        .weekday()
        .number_from_monday();
    let inc = (weekday + 7 - wday_bom) % 7;
    7 * (week_of_month - 1) + inc + 1
}

#[cfg(test)]
mod tests {
    use crate::holiday::*;
    use chrono::NaiveDate;

    #[test]
    fn test_holidays() {
        assert!(is_new_year(&NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()));
        assert!(!is_new_year(&NaiveDate::from_ymd_opt(2023, 1, 12).unwrap()));
        assert!(is_new_year(&NaiveDate::from_ymd_opt(2023, 1, 2).unwrap()));
        assert!(is_memorial_day(
            &NaiveDate::from_ymd_opt(2012, 5, 28).unwrap()
        ));
        assert!(is_memorial_day(
            &NaiveDate::from_ymd_opt(2013, 5, 27).unwrap()
        ));
        assert!(is_memorial_day(
            &NaiveDate::from_ymd_opt(2014, 5, 26).unwrap()
        ));
        assert!(is_memorial_day(
            &NaiveDate::from_ymd_opt(2022, 5, 30).unwrap()
        ));
        assert!(is_memorial_day(
            &NaiveDate::from_ymd_opt(2023, 5, 29).unwrap()
        ));
        assert!(!is_memorial_day(
            &NaiveDate::from_ymd_opt(2013, 5, 26).unwrap()
        ));
        assert!(!is_memorial_day(
            &NaiveDate::from_ymd_opt(2023, 5, 28).unwrap()
        ));
        assert!(is_independence_day(
            &NaiveDate::from_ymd_opt(2023, 7, 4).unwrap()
        ));
        assert!(is_independence_day(
            &NaiveDate::from_ymd_opt(2020, 7, 4).unwrap()
        ));
        assert!(is_independence_day(
            &NaiveDate::from_ymd_opt(2021, 7, 5).unwrap()
        ));
        assert!(!is_independence_day(
            &NaiveDate::from_ymd_opt(2021, 7, 4).unwrap()
        ));
        assert!(is_labor_day(&NaiveDate::from_ymd_opt(2012, 9, 3).unwrap()));
        assert!(is_labor_day(&NaiveDate::from_ymd_opt(2013, 9, 2).unwrap()));
        assert!(is_labor_day(&NaiveDate::from_ymd_opt(2014, 9, 1).unwrap()));
        assert!(is_thanksgiving(
            &NaiveDate::from_ymd_opt(2012, 11, 22).unwrap()
        ));
        assert!(is_thanksgiving(
            &NaiveDate::from_ymd_opt(2013, 11, 28).unwrap()
        ));
        assert!(is_thanksgiving(
            &NaiveDate::from_ymd_opt(2014, 11, 27).unwrap()
        ));
        assert!(is_thanksgiving(
            &NaiveDate::from_ymd_opt(2014, 11, 27).unwrap()
        ));
        assert!(is_christmas(
            &NaiveDate::from_ymd_opt(2018, 12, 25).unwrap()
        ));
        assert!(is_christmas(
            &NaiveDate::from_ymd_opt(2022, 12, 26).unwrap()
        ));
        assert!(!is_christmas(
            &NaiveDate::from_ymd_opt(2022, 12, 25).unwrap()
        ));
    }

    #[test]
    fn test_nerc_calendar() {
        assert!(NERC_CALENDAR.is_holiday(&NaiveDate::from_ymd_opt(2022, 1, 1).unwrap()));
        assert!(!NERC_CALENDAR.is_holiday(&NaiveDate::from_ymd_opt(2023, 1, 1).unwrap()));
        assert!(NERC_CALENDAR.is_holiday(&NaiveDate::from_ymd_opt(2023, 1, 2).unwrap()));
        assert!(NERC_CALENDAR.is_holiday(&NaiveDate::from_ymd_opt(2018, 12, 25).unwrap()));
    }
}
