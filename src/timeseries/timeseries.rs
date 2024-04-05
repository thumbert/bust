use chrono::TimeZone;
use std::slice::Iter;
// use std::vec::IntoIter;
// use serde::{Deserialize, Serialize};
use crate::interval::IntervalLike;


#[derive(Clone, Debug)]
// #[derive(Clone, Deserialize, Serialize, Debug)]
pub struct Observation<T: IntervalLike, K: Clone> {
    pub interval: T,
    pub value: K,
}

pub struct TimeSeries<T: IntervalLike, K: Clone>(Vec<Observation<T, K>>); 

impl<T: IntervalLike, K: Clone> TimeSeries<T, K> {
    pub fn new() -> TimeSeries<T, K> {
        let v: Vec<Observation<T, K>> = Vec::new();
        TimeSeries(v)
    }

    pub fn push(&mut self, value: Observation<T, K>) {
        // check that you only push at the end of the timeseries
        if !self.is_empty() {
            let obs = &self.last().unwrap().interval;
            if value.interval.start() < obs.start() {
                panic!("You can only push at the end of a timeseries!");
            }
        }
        self.0.push(value);
    }

    pub fn first(&self) -> Option<&Observation<T, K>> {
        // self.observations.first()
        self.0.first()
    }

    pub fn is_empty(&self) -> bool {
        // self.observations.is_empty()
        self.0.is_empty()
    }

    // pub fn into_iter(&self) -> IntoIter<Observation<T, K>> {
    //     self.0.into_iter()
    // }

    pub fn iter(&self) -> Iter<'_, Observation<T, K>> {
        self.0.iter()
    }

    pub fn last(&self) -> Option<&Observation<T, K>> {
        // self.observations.last()
        self.0.last()
    }

    pub fn len(&self) -> usize {
        // self.observations.len()
        self.0.len()
    }
}

impl<T: IntervalLike, K: Clone> FromIterator<Observation<T,K>> for TimeSeries<T, K> {
    fn from_iter<I: IntoIterator<Item = Observation<T,K>>>(iter: I) -> Self {
        let mut c: TimeSeries<T,K> = TimeSeries::new();
        for i in iter {
            c.push(i);
        }
        c
    }
}


#[cfg(test)]
mod tests {
    use super::*;
    use crate::interval::hour::Hour;
    use chrono::Utc;
    use chrono_tz::Tz;

    fn push_panic() -> TimeSeries<Hour, bool> {
        let mut ts: TimeSeries<Hour, bool> = TimeSeries::new();
        ts.push(Observation {
            interval: Hour {
                start: Tz::UTC.with_ymd_and_hms(2022, 1, 1, 5, 0, 0).unwrap(),
            },
            value: true,
        });
        ts.push(Observation {
            interval: Hour {
                start: Tz::UTC.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
            },
            value: true,
        });
        ts
    }

    #[test]
    fn test_observation() {
        let obs = Observation {
            interval: Hour {
                start: Tz::UTC.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
            },
            value: 1.0,
        };
        println!("{:?}", obs);
    }

    #[test]
    fn test_timeseries() {
        let mut ts: TimeSeries<Hour, bool> = TimeSeries::new();
        assert_eq!(ts.len(), 0);
        // add a few observations
        ts.push(Observation {
            interval: Hour {
                start: Tz::UTC.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
            },
            value: true,
        });
        assert_eq!(ts.len(), 1);
    }

    #[test]
    fn test_timeseries_iter() {
        let mut ts: TimeSeries<Hour, bool> = TimeSeries::new();
        ts.push(Observation {
            interval: Hour {
                start: Tz::UTC.with_ymd_and_hms(2022, 1, 1, 0, 0, 0).unwrap(),
            },
            value: true,
        });
        ts.push(Observation {
            interval: Hour {
                start: Tz::UTC.with_ymd_and_hms(2022, 1, 1, 1, 0, 0).unwrap(),
            },
            value: false,
        });
        ts.push(Observation {
            interval: Hour {
                start: Tz::UTC.with_ymd_and_hms(2022, 1, 1, 2, 0, 0).unwrap(),
            },
            value: true,
        });
        let res: TimeSeries<Hour,bool>  = ts.iter()
            .filter(|e| e.value == true)
            .cloned()
            .collect();
        for e in res.iter() {
            println!("{:?}", e);
        }
        assert_eq!(res.len(), 2);
    }

    #[test]
    #[should_panic(expected = "You can only push at the end of a timeseries!")]
    fn test_push_panic() {
        push_panic();
    }
}








// // pub struct TimeSeriesIter<'a, T: TimeZone, K: Clone> {
// //     ts: &'a TimeSeries<T, K>,
// //     index: usize,
// // }

// // impl<'a, T: TimeZone, K: Clone> Iterator for TimeSeriesIter<'a, T, K> {
// //     type Item = Observation<T, K>;

// //     fn next(&mut self) -> Option<Self::Item> {
// //         if self.index < self.ts.len() {
// //             self.index += 1;
// //             Some(Observation {
// //                 interval: self.ts.observations[self.index - 1].interval.clone(),
// //                 value: self.ts.observations[self.index - 1].value.clone(),
// //             })
// //         } else {
// //             None
// //         }
// //     }
// // }
