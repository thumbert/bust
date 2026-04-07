use jiff::civil::{Date, Weekday};

use crate::time::calendar::HolidayTrait;

#[allow(dead_code)]
fn last_business_day_before(date: Date, calendar: impl HolidayTrait) -> Date {
    let mut last_day = date.yesterday().unwrap();
    while !is_business_day(last_day, &calendar) {
        last_day = last_day.yesterday().unwrap();
    }
    last_day
}

#[allow(dead_code)]
fn is_business_day(date: Date, calendar: &impl HolidayTrait) -> bool {
    let weekday = date.weekday();
    if weekday == Weekday::Saturday || weekday == Weekday::Sunday {
        return false;
    }
    !calendar.is_holiday(&date)
}

#[cfg(test)]
mod tests {
    use jiff::civil::date;

    use crate::time::calendar::NERC_CALENDAR;

    use super::*;

    #[test]
    fn test_last_business_day_before() {
        assert_eq!(
            last_business_day_before(date(2024, 6, 17), NERC_CALENDAR),
            date(2024, 6, 14)
        ); // Monday
        assert_eq!(
            last_business_day_before(date(2024, 6, 18), NERC_CALENDAR),
            date(2024, 6, 17)
        ); // Tuesday
        assert_eq!(
            last_business_day_before(date(2024, 6, 19), NERC_CALENDAR),
            date(2024, 6, 18)
        ); // Wednesday
        assert_eq!(
            last_business_day_before(date(2024, 6, 20), NERC_CALENDAR),
            date(2024, 6, 19)
        ); // Thursday
        assert_eq!(
            last_business_day_before(date(2024, 6, 21), NERC_CALENDAR),
            date(2024, 6, 20)
        ); // Friday
        assert_eq!(
            last_business_day_before(date(2024, 6, 22), NERC_CALENDAR),
            date(2024, 6, 21)
        ); // Saturday
        assert_eq!(
            last_business_day_before(date(2024, 6, 23), NERC_CALENDAR),
            date(2024, 6, 21)
        ); // Sunday
    }
}
