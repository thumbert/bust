use jiff::civil::*;

use crate::time::holiday::*;

pub trait HolidayTrait {
    fn is_holiday(&self, date: &Date) -> bool;
}

pub const NERC_CALENDAR: NercCalendar = NercCalendar {};
pub const FEDERAL_HOLIDAY_CALENDAR: FederalHolidayCalendar = FederalHolidayCalendar {};

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


pub struct FederalHolidayCalendar {}

impl HolidayTrait for FederalHolidayCalendar {
    fn is_holiday(&self, date: &Date) -> bool {
        match date.month() {
            1 => is_new_year(date) || is_mlk_birthday(date),
            2 => is_washington_birthday(date),
            5 => is_memorial_day(date),
            6 => is_juneteenth(date),
            7 => is_independence_day(date),
            9 => is_labor_day(date),
            10 => is_columbus_day(date),
            11 => is_veterans_day(date) || is_thanksgiving(date),
            12 => is_christmas(date),
            _ => false,
        }
    }
}