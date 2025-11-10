
use jiff::{
    civil::date,
    ToSpan, Zoned,
};

use crate::interval::{
    date_tz::DateTz, hour_tz::HourTz, interval_base::{DateExt, IntervalTzLike}, month_tz::MonthTz, term::{Term, TermType}
};

// use pest::Parser;
// use pest_derive::Parser;


// #[derive(Parser)]
// #[grammar = "grammars/term.pest"]
// pub struct TermParser2;



#[derive(Clone,PartialEq)]
pub struct TermTz {
    pub start_date: DateTz,
    pub end_date: DateTz,
}

impl TermTz {
    pub fn new(start: DateTz, end: DateTz) -> Option<TermTz> {
        if start.start().time_zone() != end.end().time_zone() || end < start {
            return None;
        }
        Some(TermTz {
            start_date: start,
            end_date: end,
        })
    }

    /// Determining a TermType is a pretty expensive operation.
    pub fn term_type(&self) -> TermType {
        Term::new(self.start().date(), self.end().date())
            .unwrap()
            .term_type()
    }

    /// Return the hours in the term
    pub fn hours(&self) -> Vec<HourTz> {
        let mut hours = Vec::new();
        let mut current = self.start();
        let end_dt = self.end_date.end();
        while current < end_dt {
            hours.push(HourTz::containing(&current));
            current = current.saturating_add(1.hour());
        }
        hours
    }

    /// Return the days in the term
    pub fn days(&self) -> Vec<DateTz> {
        let mut days = Vec::new();
        let mut current = self.start_date.start().date();
        let end_date = self.end_date.end().date();
        while current <= end_date {
            days.push(current.with_tz(self.start().time_zone()));
            current = current.saturating_add(1.days());
        }
        days
    }

    /// Returns the months in this term.  If the term is not an exact month or 
    /// month range, return the minimal vector of months that cover the term.   
    pub fn months(&self) -> Vec<MonthTz> {
        let mut months = Vec::new();
        let mut current = MonthTz::containing(self.start());
        let end_month = MonthTz::containing(self.end());
        while current < end_month {
            months.push(MonthTz::containing(current.start()));
            let next = date(current.start().year(), current.start().month(), 1)
                .saturating_add(1.months())
                .with_tz(self.start().time_zone());
            current = MonthTz::containing(next.start());
        }
        months
    }
}

impl IntervalTzLike for TermTz {
    fn start(&self) -> Zoned {
        self.start_date.start()
    }
    fn end(&self) -> Zoned {
        self.end_date.end()
    }
}



#[cfg(test)]
mod tests {

    use crate::{elec::iso::ISONE, interval::term::*};


    #[test]
    fn test_hours() {
        let term = "2025".parse::<Term>().unwrap().with_tz(&ISONE.tz);
        assert_eq!(term.hours().len(), 8760);
    }

}

