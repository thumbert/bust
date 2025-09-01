use std::slice::Iter;
use std::{cmp, ops::Add};

use jiff::{civil::DateTime, Zoned};
use serde::{Deserialize, Serialize};
use std::vec::IntoIter;

use crate::interval::{
    date_tz::DateTz,
    hour_tz::HourTz,
    interval::{IntervalLike, IntervalTzLike},
    month_tz::MonthTz,
};

pub enum JoinType {
    Inner,
    Left,
    Outer,
    Right,
}

// #[derive(Clone, Debug, Copy)]
#[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Obs<T: IntervalLike, K: Clone> {
    pub interval: T,
    pub value: K,
}

impl<T: IntervalLike, K: Clone> Obs<T, K> {
    pub fn new(interval: T, value: K) -> Obs<T, K> {
        Obs { interval, value }
    }
}

impl<T, K> cmp::PartialEq for Obs<T, K>
where
    T: IntervalLike + PartialEq,
    K: Eq + Clone,
{
    fn eq(&self, other: &Self) -> bool {
        self.interval == other.interval && self.value == other.value
    }
}

pub struct Hourly;
#[derive(Clone, Debug, Copy)]
pub struct Daily;
pub struct Monthly;
pub struct Irregular;

pub enum Kind {
    Hourly,
    Daily,
    Monthly,
    Irregular,
}

// pub struct TimeSeries<T: IntervalLike, K: Clone>(Vec<Obs<T, K>>);
// pub struct Series<Type, V: Clone> {
//     data: Vec<(DateTime, V)>,
//     kind: std::marker::PhantomData<Type>,
// }

// pub struct Series<Type, V: Clone>(Vec<(DateTime, V)>);

// impl<V: Clone, Type> Series<Type, V> {
//     pub fn new() -> Series<Type, V> {
//         Series {
//             data: Vec::new(),
//             kind: std::marker::PhantomData,
//         }
//     }
// }

#[derive(Clone, Debug)]
pub struct SeriesTz<I: IntervalTzLike, V: Clone>(pub Vec<(I, V)>);

impl<I: IntervalTzLike, V: Clone> SeriesTz<I, V> {
    /// Creates a new, empty TimeSeries.
    pub fn new() -> SeriesTz<I, V> {
        SeriesTz(Vec::new())
    }

    pub fn fill(intervals: Vec<I>, value: V) -> SeriesTz<I, V> {
        let mut ts: SeriesTz<I, V> = SeriesTz::new();
        for ival in intervals {
            ts.push((ival, value.clone()));
        }
        ts
    }

    /// Returns the number of elements in the series.
    pub fn len(&self) -> usize {
        self.0.len()
    }

    /// Returns true if the series contains no elements.
    pub fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    /// Returns the capacity of the underlying vector.
    pub fn capacity(&self) -> usize {
        self.0.capacity()
    }

    /// Clears the series, removing all elements.
    pub fn clear(&mut self) {
        self.0.clear();
    }

    /// Reserves capacity for at least additional more elements.
    pub fn reserve(&mut self, additional: usize) {
        self.0.reserve(additional);
    }

    /// Shrinks the capacity of the vector as much as possible.
    pub fn shrink_to_fit(&mut self) {
        self.0.shrink_to_fit();
    }

    /// Pushes a (IntervalTzLike, V) tuple at the end of the series.
    pub fn push(&mut self, value: (I, V)) {
        if !self.is_empty() {
            // check that you only push at the end of the timeseries
            if value.0.start() < self.0.last().unwrap().0.end() {
                panic!("you can only push at the end of a timeseries!");
            }
            // check that timezones match
            if self.0.last().unwrap().0.end().time_zone() != value.0.start().time_zone() {
                panic!("The observation that you add should be in the same timezone as existing observations.")
            }
        }
        self.0.push(value);
    }

    /// Removes the last element from the series and returns it, or None if empty.
    pub fn pop(&mut self) -> Option<(I, V)> {
        self.0.pop()
    }

    /// Returns a reference to the element at index, or None.
    pub fn get(&self, index: usize) -> Option<&(I, V)> {
        self.0.get(index)
    }

    /// Returns a mutable reference to the element at index, or None.
    pub fn get_mut(&mut self, index: usize) -> Option<&mut (I, V)> {
        self.0.get_mut(index)
    }

    /// Provides a reference to the first element, or None.
    pub fn first(&self) -> Option<&(I, V)> {
        self.0.first()
    }

    /// Provides a reference to the last element, or None.
    pub fn last(&self) -> Option<&(I, V)> {
        self.0.last()
    }

    /// Removes and returns the element at position index.
    pub fn remove(&mut self, index: usize) -> (I, V) {
        self.0.remove(index)
    }

    /// Returns an iterator over references to the elements.
    pub fn iter(&self) -> std::slice::Iter<'_, (I, V)> {
        self.0.iter()
    }

    /// Returns an iterator over mutable references to the elements.
    pub fn iter_mut(&mut self) -> std::slice::IterMut<'_, (I, V)> {
        self.0.iter_mut()
    }

    /// Returns a slice of the underlying data.
    pub fn as_slice(&self) -> &[(I, V)] {
        self.0.as_slice()
    }

    /// Returns a mutable slice of the underlying data.
    pub fn as_mut_slice(&mut self) -> &mut [(I, V)] {
        self.0.as_mut_slice()
    }

    /// Extends the series with the contents of an iterator.
    pub fn extend<K: IntoIterator<Item = (I, V)>>(&mut self, iter: K) {
        self.0.extend(iter);
    }

    /// Retains only elements specified by the predicate.
    pub fn retain<F>(&mut self, f: F)
    where
        F: FnMut(&(I, V)) -> bool,
    {
        self.0.retain(f);
    }

    /// Splits off at the given index, returning a new TimeSeries.
    pub fn split_off(&mut self, at: usize) -> SeriesTz<I, V> {
        SeriesTz(self.0.split_off(at))
    }

    /// Truncates the series to the specified length.
    pub fn truncate(&mut self, len: usize) {
        self.0.truncate(len);
    }

    /// Drains a range from the series, returning an iterator.
    pub fn drain<R>(&mut self, range: R) -> std::vec::Drain<'_, (I, V)>
    where
        R: std::ops::RangeBounds<usize>,
    {
        self.0.drain(range)
    }

    /// Merge this timeseries with another one.
    pub fn merge<K: Clone>(&self, y: SeriesTz<I, K>, join_type: JoinType) -> SeriesTz<I, (V, K)> {
        let mut ts: SeriesTz<I, (V, K)> = SeriesTz::new();
        if self.is_empty() || y.is_empty() {
            return ts;
        }
        match join_type {
            JoinType::Inner => {
                let mut j: usize = 0;
                for e in self {
                    while y.get(j).unwrap().0.start() < e.0.start() && j < y.len() - 1 {
                        j += 1;
                    }
                    let yj = y.get(j).unwrap();
                    if e.0 == yj.0 {
                        ts.push((e.0.clone(), (e.1.clone(), yj.1.clone())));
                    }
                }
            }
            JoinType::Left => todo!(),
            JoinType::Outer => todo!(),
            JoinType::Right => todo!(),
        }

        ts
    }
}

/// Implement SeriesTz<(I,V)>  + V
impl<I: IntervalTzLike, V: Clone> Add<V> for SeriesTz<I, V>
where
    V: Clone + Add<Output = V>,
{
    type Output = SeriesTz<I, V>;
    fn add(self, rhs: V) -> Self::Output {
        SeriesTz(
            self.0
                .into_iter()
                .map(|(z, v)| (z, v + rhs.clone()))
                .collect(),
        )
    }
}

// Implement TimeSeries<(I,V)> + TimeSeries<(I,V)>
impl<I: IntervalTzLike, V: Clone> Add for SeriesTz<I, V>
where
    V: Clone + Add<Output = V>,
{
    type Output = SeriesTz<I, V>;
    fn add(self, rhs: SeriesTz<I, V>) -> Self::Output {
        let merged = self.merge(rhs, JoinType::Inner);
        SeriesTz(merged.into_iter().map(|z| (z.0, z.1 .0 + z.1 .1)).collect())
    }
}

// Implement FromIterator for SeriesTz
impl<I: IntervalTzLike, V: Clone> FromIterator<(I, V)> for SeriesTz<I, V> {
    fn from_iter<T: IntoIterator<Item = (I, V)>>(iter: T) -> Self {
        SeriesTz(iter.into_iter().collect())
    }
}

// Implement IntoIterator for TimeSeries so it can be iterated directly.
impl<I: IntervalTzLike, V: Clone> IntoIterator for SeriesTz<I, V> {
    type Item = (I, V);
    type IntoIter = std::vec::IntoIter<(I, V)>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.into_iter()
    }
}

// Implement iter and iter_mut for references to TimeSeries.
impl<'a, I: IntervalTzLike, V: Clone> IntoIterator for &'a SeriesTz<I, V> {
    type Item = &'a (I, V);
    type IntoIter = std::slice::Iter<'a, (I, V)>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter()
    }
}

impl<'a, I: IntervalTzLike, V: Clone> IntoIterator for &'a mut SeriesTz<I, V> {
    type Item = &'a mut (I, V);
    type IntoIter = std::slice::IterMut<'a, (I, V)>;
    fn into_iter(self) -> Self::IntoIter {
        self.0.iter_mut()
    }
}

// Implement Index and IndexMut for direct indexing.
use std::ops::{Index, IndexMut};
impl<I: IntervalTzLike, V: Clone> Index<usize> for SeriesTz<I, V> {
    type Output = (I, V);
    fn index(&self, index: usize) -> &Self::Output {
        &self.0[index]
    }
}
impl<I: IntervalTzLike, V: Clone> IndexMut<usize> for SeriesTz<I, V> {
    fn index_mut(&mut self, index: usize) -> &mut Self::Output {
        &mut self.0[index]
    }
}

impl<I: IntervalTzLike, V: Clone> Default for SeriesTz<I, V> {
    fn default() -> Self {
        Self::new()
    }
}


// /// Iterator for merging two SeriesTz instances
// pub struct MergeIterator<'a, T, V> {
//     iter1: std::slice::Iter<'a, (T, V)>,
//     iter2: std::slice::Iter<'a, (T, V)>,
// }

// impl<'a, T, V> MergeIterator<'a, T, V>
// where
//     T: Ord,
// {
//     pub fn new(series1: &'a [(T, V)], series2: &'a [(T, V)]) -> Self {
//         Self {
//             iter1: series1.iter(),
//             iter2: series2.iter(),
//         }
//     }
// }

// impl<'a, T, V> Iterator for MergeIterator<'a, T, V>
// where
//     T: Ord + Clone,
//     V: Clone,
// {
//     type Item = (T, V);

//     fn next(&mut self) -> Option<Self::Item> {
//         match (self.iter1.next(), self.iter2.next()) {
//             (Some((t1, v1)), Some((t2, v2))) => {
//                 if t1 <= t2 {
//                     Some((t1.clone(), v1.clone()))
//                 } else {
//                     Some((t2.clone(), v2.clone()))
//                 }
//             }
//             (Some((t1, v1)), None) => Some((t1.clone(), v1.clone())),
//             (None, Some((t2, v2))) => Some((t2.clone(), v2.clone())),
//             (None, None) => None,
//         }
//     }
// }

// impl<T, V> SeriesTz<T, V>
// where
//     T: IntervalTzLike + Ord + Clone,
//     V: Clone,
// {
//     pub fn merge<'a>(&'a self, other: &'a Self) -> MergeIterator<'a, T, V> {
//         MergeIterator::new(&self.0, &other.0)
//     }
// }


pub struct DateTzSeries<V: Clone>(pub SeriesTz<DateTz, V>);
pub struct MonthTzSeries<V: Clone>(Vec<(MonthTz, V)>);
// pub struct MonthTzSeries<V: Clone>(pub SeriesTz<MonthTz, V>);

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

#[cfg(test)]
mod tests {
    use crate::{
        elec::iso::ISONE,
        interval::{interval::DateExt, term::Term, term_tz::TermTz},
    };

    use super::*;
    // use crate::interval::{hour::Hour, interval::Interval, month_tz::MonthTz};
    use itertools::Itertools;
    use jiff::civil::date;

    #[test]
    #[should_panic]
    fn test_timeseries() {
        let mut ts: SeriesTz<HourTz, bool> = SeriesTz::new();
        assert_eq!(ts.len(), 0);
        ts.push((
            HourTz::containing(&"2022-01-01T00:00:00[America/New_York]".parse().unwrap()),
            true,
        ));
        assert_eq!(ts.len(), 1);
        // this now panics
        ts.push((
            HourTz::containing(&"2022-01-01T00:00:00[America/New_York]".parse().unwrap()),
            true,
        ));
    }

    #[test]
    fn test_different_intervals() {
        let mut ts: SeriesTz<_, f64> = SeriesTz::new();
        assert_eq!(ts.len(), 0);
        ts.push((date(2022, 1, 1).with_tz(&ISONE.tz), 1.0));
    }

    #[test]
    fn test_timeseries_add() {
        let hours = "Jan25".parse::<Term>().unwrap().with_tz(&ISONE.tz).hours();
        // add a scalar
        let ts1: SeriesTz<HourTz, f64> = SeriesTz::fill(hours.clone(), 1.0);
        let res = ts1.clone() + 2.0;
        assert_eq!(res.first().unwrap().1, 3.0);

        // add another timeseries, same domain
        let ts2: SeriesTz<HourTz, f64> = SeriesTz::fill(hours.clone(), 1.0);
        let res = ts1.clone() + ts2;
        assert_eq!(res.len(), hours.len());
        assert_eq!(res.first().unwrap().1, 2.0);

        // add another timeseries, different domain
        let ts2: SeriesTz<HourTz, f64> =
            SeriesTz::fill(hours.iter().take(32).cloned().collect(), 1.0);
        let res = ts1.clone() + ts2;
        assert_eq!(res.len(), 32);
        assert_eq!(res.first().unwrap().1, 2.0);
    }

    // #[test]
    // fn test_group() {
    //     let term = Interval::with_y(2022, 2022, New_York);
    //     let hours = term.unwrap().hours();
    //     let ts = TimeSeries::filled(hours, 1);

    //     let groups = ts
    //         .into_iter()
    //         .map(|x| {
    //             let start = x.0.start();
    //             ((start.year(), start.month()), x.1)
    //         })
    //         .into_group_map();

    //     let count = groups
    //         .into_iter()
    //         .map(|((year, month), value)| {
    //             (MonthTz::new(year, month, New_York).unwrap(), value.len())
    //         })
    //         .sorted_by(|a, b| Ord::cmp(&a.0, &b.0))
    //         .collect::<TimeSeries<MonthTz, usize>>();

    //     // TimeSeries<Month, usize>
    //     // println!("{:}", count);
    //     count
    //         .into_iter()
    //         .for_each(|(k, v)| println!("{:} -> {}", k, v));
    // }

    // #[test]
    // fn test_from_iter() {
    //     let ts: TimeSeries<MonthTz, i32> = vec![
    //         (MonthTz::new(2024, 1, New_York).unwrap(), 10),
    //         (MonthTz::new(2024, 2, New_York).unwrap(), 11),
    //         (MonthTz::new(2024, 3, New_York).unwrap(), 12),
    //         (MonthTz::new(2024, 4, New_York).unwrap(), 14),
    //     ]
    //     .into_iter()
    //     .collect();
    //     assert_eq!(ts.len(), 4);

    //     let x: Vec<i32> = ts.into_iter().map(|x| x.1).collect();
    //     println!("{:?}", x);
    // }

    // #[test]
    // #[should_panic(expected = "You can only push at the end of a timeseries!")]
    // fn test_push_panic() {
    //     push_panic();
    // }
}
