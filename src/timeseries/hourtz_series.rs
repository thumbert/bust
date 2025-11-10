use crate::{
    interval::{hour_tz::HourTz, interval_base::IntervalTzLike},
    timeseries::series_tz::SeriesTz,
};

// pub struct HourTzSeries<V: Clone>(pub SeriesTz<HourTz, V>);

// impl<V: Clone> HourTzSeries<V> {
//     pub fn new() -> HourTzSeries<V> {
//         HourTzSeries(Vec::new())
//     }

//     pub fn filled(hours: Vec<HourTz>, value: V) -> HourTzSeries<V> {
//         let mut v: Vec<(HourTz, V)> = Vec::new();
//         for t in hours.into_iter() {
//             let obs = (t, value.clone());
//             v.push(obs);
//         }
//         HourTzSeries(v)
//     }

//     pub fn len(&self) -> usize {
//         self.0.0.len()
//     }

//     pub fn is_empty(&self) -> bool {
//         self.0.0.is_empty()
//     }

//     pub fn push(&mut self, value: (HourTz, V)) {
//         // check that you only push at the end of the timeseries
//         if !self.is_empty() && value.0.start() < self.0.0.last().unwrap().0.end() {
//             panic!("You can only push at the end of a timeseries!");
//         }
//         self.0.0.push(value);
//     }
// }

// impl<V: Clone> Default for HourTzSeries<V> {
//     fn default() -> Self {
//         Self::new()
//     }
// }
