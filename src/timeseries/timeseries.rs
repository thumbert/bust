// use std::cmp;
// use std::slice::Iter;

// use crate::interval::interval::IntervalLike;
// use std::vec::IntoIter;
// use serde::{Deserialize, Serialize};

// #[derive(Clone, Debug, Copy)]
// // #[derive(Clone, Deserialize, Serialize, Debug)]
// pub struct Obs<T: IntervalLike, K: Clone> {
//     pub interval: T,
//     pub value: K,
// }

// impl<T: IntervalLike, K: Clone> Obs<T, K> {
//     pub fn new(interval: T, value: K) -> Obs<T, K> {
//         Obs { interval, value }
//     }
// }

// impl<T, K> cmp::PartialEq for Obs<T, K>
// where
//     T: IntervalLike + PartialEq,
//     K: Eq + Clone,
// {
//     fn eq(&self, other: &Self) -> bool {
//         self.interval == other.interval && self.value == other.value
//     }
// }

// pub struct TimeSeries<T: IntervalLike, K: Clone>(Vec<Obs<T, K>>);

// #[derive(Display)]
// pub struct TimeSeries<T: IntervalLike, K: Clone>(Vec<(T, K)>);

// impl<T: IntervalLike, K: Clone> Default for TimeSeries<T, K> {
//     fn default() -> Self {
//         Self::new()
//     }
// }

// impl<T: IntervalLike, K: Clone> TimeSeries<T, K> {
//     pub fn new() -> TimeSeries<T, K> {
//         let v: Vec<(T, K)> = Vec::new();
//         TimeSeries(v)
//     }

//     pub fn filled(intervals: Vec<T>, value: K) -> TimeSeries<T, K> {
//         let mut v: Vec<(T, K)> = Vec::new();
//         for t in intervals.into_iter() {
//             let obs = (t, value.clone());
//             v.push(obs);
//         }
//         TimeSeries(v)
//     }

//     pub fn push(&mut self, value: (T, K)) {
//         // check that you only push at the end of the timeseries
//         if !self.is_empty() {
//             let obs = self.last().unwrap();
//             if value.0.start() < obs.0.start() {
//                 panic!("You can only push at the end of a timeseries!");
//             }
//         }
//         self.0.push(value);
//     }

//     pub fn first(&self) -> Option<&(T, K)> {
//         // self.observations.first()
//         self.0.first()
//     }

//     pub fn is_empty(&self) -> bool {
//         self.0.is_empty()
//     }

//     pub fn iter(&self) -> Iter<'_, (T, K)> {
//         self.0.iter()
//     }

//     pub fn last(&self) -> Option<&(T, K)> {
//         self.0.last()
//     }

//     pub fn len(&self) -> usize {
//         // self.observations.len()
//         self.0.len()
//     }
// }

// impl<T: IntervalLike, K: Clone> FromIterator<(T, K)> for TimeSeries<T, K> {
//     fn from_iter<I: IntoIterator<Item = (T, K)>>(iter: I) -> Self {
//         let mut c: TimeSeries<T, K> = TimeSeries::new();
//         for i in iter {
//             c.push(i);
//         }
//         c
//     }
// }

// impl<T: IntervalLike, K: Clone> IntoIterator for TimeSeries<T, K> {
//     type Item = (T, K);
//     type IntoIter = std::vec::IntoIter<Self::Item>;
//     fn into_iter(self) -> Self::IntoIter {
//         self.0.into_iter()
//     }
// }

// #[cfg(test)]
// mod tests {
//     use super::*;
//     use crate::interval::{hour::Hour, interval::Interval, month_tz::MonthTz};
//     use chrono::{Datelike, TimeZone};
//     use chrono_tz::{America::New_York, Tz};
//     use itertools::Itertools;

//     fn push_panic() -> TimeSeries<Hour, bool> {
//         let mut ts: TimeSeries<Hour, bool> = TimeSeries::new();
//         ts.push((
//             Hour::containing(Tz::UTC.with_ymd_and_hms(2022, 1, 1, 5, 0, 0).unwrap()),
//             true,
//         ));
//         ts.push((
//             Hour::containing(Tz::UTC.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()),
//             true,
//         ));
//         ts
//     }

//     #[test]
//     fn test_observation() {
//         let obs = (
//             Hour::containing(Tz::UTC.with_ymd_and_hms(2022, 1, 1, 5, 0, 0).unwrap()),
//             1.0,
//         );
//         println!("{:?}", obs);
//     }

//     #[test]
//     fn test_timeseries() {
//         let mut ts: TimeSeries<Hour, bool> = TimeSeries::new();
//         assert_eq!(ts.len(), 0);
//         ts.push((
//             Hour::containing(Tz::UTC.with_ymd_and_hms(2022, 1, 1, 5, 0, 0).unwrap()),
//             true,
//         ));
//         assert_eq!(ts.len(), 1);
//     }

//     #[test]
//     fn test_timeseries_iter() {
//         let mut ts: TimeSeries<Hour, bool> = TimeSeries::new();
//         ts.push((
//             Hour::containing(Tz::UTC.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap()),
//             true,
//         ));
//         ts.push((
//             Hour::containing(Tz::UTC.with_ymd_and_hms(2022, 1, 1, 1, 0, 0).unwrap()),
//             false,
//         ));
//         ts.push((
//             Hour::containing(Tz::UTC.with_ymd_and_hms(2022, 1, 1, 2, 0, 0).unwrap()),
//             true,
//         ));
//         let res: TimeSeries<Hour, bool> = ts.iter().filter(|e| e.1).cloned().collect();
//         for e in res.iter() {
//             println!("{:?}", e);
//         }
//         assert_eq!(res.len(), 2);
//     }

//     #[test]
//     fn test_group() {
//         let term = Interval::with_y(2022, 2022, New_York);
//         let hours = term.unwrap().hours();
//         let ts = TimeSeries::filled(hours, 1);

//         let groups = ts
//             .into_iter()
//             .map(|x| {
//                 let start = x.0.start();
//                 ((start.year(), start.month()), x.1)
//             })
//             .into_group_map();

//         let count = groups
//             .into_iter()
//             .map(|((year, month), value)| {
//                 (MonthTz::new(year, month, New_York).unwrap(), value.len())
//             })
//             .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
//             .collect::<TimeSeries<MonthTz, usize>>();

//         // TimeSeries<Month, usize>
//         // println!("{:}", count);
//         count
//             .into_iter()
//             .for_each(|(k, v)| println!("{:} -> {}", k, v));
//     }

//     #[test]
//     fn test_from_iter() {
//         let ts: TimeSeries<MonthTz, i32> = vec![
//             (MonthTz::new(2024, 1, New_York).unwrap(), 10),
//             (MonthTz::new(2024, 2, New_York).unwrap(), 11),
//             (MonthTz::new(2024, 3, New_York).unwrap(), 12),
//             (MonthTz::new(2024, 4, New_York).unwrap(), 14),
//         ]
//         .into_iter()
//         .collect();
//         assert_eq!(ts.len(), 4);

//         let x: Vec<i32> = ts.into_iter().map(|x| x.1).collect();
//         println!("{:?}", x);
//     }

//     #[test]
//     #[should_panic(expected = "You can only push at the end of a timeseries!")]
//     fn test_push_panic() {
//         push_panic();
//     }
// }
