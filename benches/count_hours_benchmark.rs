#[macro_use]
extern crate lazy_static;
use bust::{
    bucket::{count_hours, Bucket},
    interval::term_tz::TermTz,
};
use criterion::{criterion_group, criterion_main, Criterion};

lazy_static! {
    static ref PAIRS: Vec<(Bucket, TermTz)> = {
        let cal22 = "Cal 22[America/New_York]".parse::<TermTz>().unwrap();
        let cal23 = "Cal 23[America/Los_Angeles]".parse::<TermTz>().unwrap();
        let cal24 = "Cal 24[America/New_York]".parse::<TermTz>().unwrap();
        let v = vec![
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
    c.bench_function("count_hours", |b| {
        b.iter(|| {
            let _ = count_hours(PAIRS.to_vec());
        });
    });
}

criterion_group!(benches, my_benchmark);
criterion_main!(benches);
