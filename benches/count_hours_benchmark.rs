#[macro_use]
extern crate lazy_static;
use bust::{
    bucket::{Bucket, BucketLike, count_hours},
    interval::term_tz::TermTz,
};
use criterion::{criterion_group, criterion_main, Criterion};

lazy_static! {
    static ref PAIRS: Vec<(Bucket, TermTz)> = {
        let cal21 = "Cal 21[America/New_York]".parse::<TermTz>().unwrap();
        let cal22 = "Cal 22[America/New_York]".parse::<TermTz>().unwrap();
        let cal23 = "Cal 23[America/Los_Angeles]".parse::<TermTz>().unwrap();
        let cal24 = "Cal 24[America/New_York]".parse::<TermTz>().unwrap();
        let v = vec![
            (Bucket::Atc, cal21.clone()),
            (Bucket::B5x16, cal21.clone()),
            (Bucket::B2x16H, cal21.clone()),
            (Bucket::B7x8, cal21.clone()),
            (Bucket::Offpeak, cal21.clone()),
            (Bucket::B7x16, cal21.clone()),
            (Bucket::Atc, cal22.clone()),
            (Bucket::B5x16, cal22.clone()),
            (Bucket::B2x16H, cal22.clone()),
            (Bucket::B7x8, cal22.clone()),
            (Bucket::Offpeak, cal22.clone()),
            (Bucket::B7x16, cal22.clone()),
            (Bucket::Atc, cal24.clone()),
            (Bucket::B5x16, cal24.clone()),
            (Bucket::B2x16H, cal24.clone()),
            (Bucket::B7x8, cal24.clone()),
            (Bucket::Offpeak, cal24.clone()),
            (Bucket::B7x16, cal24.clone()),
            (Bucket::Atc, cal23.clone()),
            (Bucket::Caiso6x16, cal23.clone()),
            (Bucket::Caiso1x16H, cal23.clone()),
            (Bucket::Caiso7x8, cal23.clone()),
            (Bucket::CaisoOffpeak, cal23.clone()),
        ];
        v
    };
}

fn my_benchmark(c: &mut Criterion) {
    c.bench_function("count_hours rayon", |b| {
        b.iter(|| {
            let _ = count_hours(PAIRS.to_vec());
        });
    });
}

fn my_benchmark2(c: &mut Criterion) {
    c.bench_function("count_hours single thread", |b| {
        b.iter(|| {
            for (bucket, term) in PAIRS.iter() {
                let _ = bucket.count_hours(term);
            }
        });
    });
}


criterion_group!(benches, my_benchmark, my_benchmark2);
criterion_main!(benches);

// Not clear that rayon is using all cores, or this 