use jiff::civil::*;

pub trait HolidayTrait {
    fn is_holiday(&self, date: &Date) -> bool;
}

pub const NERC_CALENDAR: NercCalendar = NercCalendar {};

pub struct NercCalendar {}

impl HolidayTrait for NercCalendar {
    fn is_holiday(&self, date: &Date) -> bool {
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
pub fn is_new_year(day: &Date) -> bool {
    if day.month() == 1 {
        if day.day() == 1 && day.weekday() != Weekday::Sunday {
            true
        } else {
            day.day() == 2 && day.weekday() == Weekday::Monday
        }
    } else {
        false
    }
}

/// Memorial day falls on the last Monday in May.  Is a Federal and a NERC holiday.
pub fn is_memorial_day(day: &Date) -> bool {
    if day.month() == 5 {
        let (yy, mm, dd) = (day.year(), day.month(), day.day());
        let weekday = date(yy, mm, 31).weekday().to_monday_one_offset();
        let candidate = date(yy, 5, 32 - weekday);
        candidate.day() == dd
    } else {
        false
    }
}

pub fn is_independence_day(day: &Date) -> bool {
    if day.month() == 7 {
        let mut candidate = date(day.year(), 7, 4);
        // If it falls on Sun, celebrate it on Mon
        if candidate.weekday() == Weekday::Sunday {
            candidate = candidate.tomorrow().unwrap();
        }
        candidate.day() == day.day()
    } else {
        false
    }
}

pub fn is_labor_day(day: &Date) -> bool {
    if day.month() == 9 {
        let dom = _dayofmonth_holiday(day.year(), 9, 1, 1);
        let candidate = date(day.year(), 9, dom as i8);
        candidate.day() == day.day()
    } else {
        false
    }
}

pub fn is_thanksgiving(day: &Date) -> bool {
    if day.month() == 11 {
        let dom = _dayofmonth_holiday(day.year(), 11, 4, 4);
        let candidate = date(day.year(), 9, dom as i8);
        candidate.day() == day.day()
    } else {
        false
    }
}

pub fn is_christmas(day: &Date) -> bool {
    if day.month() == 12 {
        let candidate = date(day.year(), 12, 25);
        if candidate.weekday() == Weekday::Sunday {
            day.day() == 26
        } else {
            day.day() == 25
        }
    } else {
        false
    }
}

/// Calculate the day of the month for a holiday that happens on a given week of the month AND
/// a specific day of that week.  For example, for Labor Day, it's the first (week_of_month == 1)
/// Mon (weekday == 1) of Sep (month == 9).
fn _dayofmonth_holiday(year: i16, month: i8, week_of_month: u8, weekday: u8) -> u8 {
    let weekday_bom = date(year, month, 1).weekday().to_monday_one_offset() as u8;
    let inc = (weekday + 7 - weekday_bom) % 7;
    7 * (week_of_month - 1) + inc + 1
}

#[cfg(test)]
mod tests {
    use crate::holiday::*;

    #[test]
    fn test_holidays() {
        assert!(is_new_year(&date(2022, 1, 1)));
        assert!(!is_new_year(&date(2023, 1, 12)));
        assert!(is_new_year(&date(2023, 1, 2)));
        assert!(is_memorial_day(&date(2012, 5, 28)));
        assert!(is_memorial_day(&date(2013, 5, 27)));
        assert!(is_memorial_day(&date(2014, 5, 26)));
        assert!(is_memorial_day(&date(2022, 5, 30)));
        assert!(is_memorial_day(&date(2023, 5, 29)));
        assert!(!is_memorial_day(&date(2013, 5, 26)));
        assert!(!is_memorial_day(&date(2023, 5, 28)));
        assert!(is_independence_day(&date(2023, 7, 4)));
        assert!(is_independence_day(&date(2020, 7, 4)));
        assert!(is_independence_day(&date(2021, 7, 5)));
        assert!(!is_independence_day(&date(2021, 7, 4)));
        assert!(is_labor_day(&date(2012, 9, 3)));
        assert!(is_labor_day(&date(2013, 9, 2)));
        assert!(is_labor_day(&date(2014, 9, 1)));
        assert!(is_thanksgiving(&date(2012, 11, 22)));
        assert!(is_thanksgiving(&date(2013, 11, 28)));
        assert!(is_thanksgiving(&date(2014, 11, 27)));
        assert!(is_thanksgiving(&date(2014, 11, 27)));
        assert!(is_christmas(&date(2018, 12, 25)));
        assert!(is_christmas(&date(2022, 12, 26)));
        assert!(!is_christmas(&date(2022, 12, 25)));
    }

    #[test]
    fn test_nerc_calendar() {
        assert!(NERC_CALENDAR.is_holiday(&date(2022, 1, 1)));
        assert!(!NERC_CALENDAR.is_holiday(&date(2023, 1, 1)));
        assert!(NERC_CALENDAR.is_holiday(&date(2023, 1, 2)));
        assert!(NERC_CALENDAR.is_holiday(&date(2018, 12, 25)));
    }
}
