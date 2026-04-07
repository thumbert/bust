use jiff::civil::*;

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

pub fn is_columbus_day(day: &Date) -> bool {
    if day.month() == 10 {
        let dom = _dayofmonth_holiday(day.year(), 10, 2, 1);
        let candidate = date(day.year(), 10, dom as i8);
        candidate.day() == day.day()
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

/// Juneteenth falls on June 19th.  
/// If it falls on a Sunday, it's celebrated on the following Monday.  
/// If it falls on a Saturday, it's celebrated on the preceding Friday.
/// Is a Federal holiday but not a NERC holiday.
/// 
/// Not celebrated before 2021, return false before 2021. 
/// 
pub fn is_juneteenth(day: &Date) -> bool {
    if day.year() < 2021 {
        return false;
    }
    if day.month() == 6 {
        let candidate = date(day.year(), 6, 19);
        if candidate.weekday() == Weekday::Sunday {
            return day.day() == 20;
        } else if candidate.weekday() == Weekday::Saturday {
            return day.day() == 18;
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

/// MLK birthday falls on the third Monday in Jan.  Is a Federal holiday but not a NERC holiday.
pub fn is_mlk_birthday(day: &Date) -> bool {
    if day.month() == 1 {
        let (yy, mm, dd) = (day.year(), day.month(), day.day());
        let weekday = date(yy, mm, 1).weekday().to_monday_one_offset(); // 1=Mon..7=Sun
        let candidate = date(yy, 1, 15 + (8 - weekday) % 7);
        candidate.day() == dd
    } else {
        false
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

pub fn is_thanksgiving(day: &Date) -> bool {
    if day.month() == 11 {
        let dom = _dayofmonth_holiday(day.year(), 11, 4, 4);
        let candidate = date(day.year(), 11, dom as i8);
        candidate.day() == day.day()
    } else {
        false
    }
}

pub fn is_veterans_day(day: &Date) -> bool {
    if day.month() == 11 {
        let candidate = date(day.year(), 11, 11);
        if candidate.weekday() == Weekday::Sunday {
            return day.day() == 12;
        } else if candidate.weekday() == Weekday::Saturday  {
            return day.day() == 10;
        }
        candidate.day() == day.day()
    } else {
        false
    }
}


/// Washington's birthday falls on the third Monday in Feb.  Is a Federal holiday but not a NERC holiday.
pub fn is_washington_birthday(day: &Date) -> bool {
    if day.month() == 2 {
        let (yy, mm, dd) = (day.year(), day.month(), day.day());
        let weekday = date(yy, mm, 1).weekday().to_monday_one_offset(); // 1=Mon..7=Sun
        let candidate = date(yy, 2, 15 + (8 - weekday) % 7);
        candidate.day() == dd
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
    use crate::time::calendar::{HolidayTrait, NERC_CALENDAR};

    use super::*;

    #[test]
    fn test_holidays() {
        assert!(is_christmas(&date(2018, 12, 25)));
        assert!(is_christmas(&date(2022, 12, 26)));
        assert!(!is_christmas(&date(2022, 12, 25)));
        assert!(is_columbus_day(&date(2017, 10, 9)));
        assert!(is_columbus_day(&date(2018, 10, 8)));
        assert!(is_columbus_day(&date(2019, 10, 14)));
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
        assert!(is_mlk_birthday(&date(2012, 1, 16)));
        assert!(is_mlk_birthday(&date(2013, 1, 21)));
        assert!(is_mlk_birthday(&date(2014, 1, 20)));
        assert!(is_independence_day(&date(2023, 7, 4)));
        assert!(is_independence_day(&date(2020, 7, 4)));
        assert!(is_independence_day(&date(2021, 7, 5)));
        assert!(!is_independence_day(&date(2021, 7, 4)));
        assert!(!is_juneteenth(&date(2020, 6, 19)));
        assert!(is_juneteenth(&date(2021, 6, 18)));
        assert!(is_juneteenth(&date(2022, 6, 20)));
        assert!(is_juneteenth(&date(2023, 6, 19)));
        assert!(is_labor_day(&date(2012, 9, 3)));
        assert!(is_labor_day(&date(2013, 9, 2)));
        assert!(is_labor_day(&date(2014, 9, 1)));
        assert!(is_thanksgiving(&date(2012, 11, 22)));
        assert!(is_thanksgiving(&date(2013, 11, 28)));
        assert!(is_thanksgiving(&date(2014, 11, 27)));
        assert!(is_thanksgiving(&date(2014, 11, 27)));
        assert!(is_veterans_day(&date(2012, 11, 12)));
        assert!(is_veterans_day(&date(2017, 11, 10)));
        assert!(is_veterans_day(&date(2019, 11, 11)));
        assert!(is_washington_birthday(&date(2017, 2, 20)));
        assert!(is_washington_birthday(&date(2018, 2, 19)));
        assert!(is_washington_birthday(&date(2021, 2, 15)));
    }

    #[test]
    fn test_nerc_calendar() {
        assert!(NERC_CALENDAR.is_holiday(&date(2022, 1, 1)));
        assert!(!NERC_CALENDAR.is_holiday(&date(2023, 1, 1)));
        assert!(NERC_CALENDAR.is_holiday(&date(2023, 1, 2)));
        assert!(NERC_CALENDAR.is_holiday(&date(2018, 12, 25)));
    }
}
