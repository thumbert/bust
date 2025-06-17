extern crate plotly;

use plotly::{Plot, Scatter};

fn main() {
    let mut plot = Plot::new();
    let trace = Scatter::new(vec!["2024-01-01 21:00:00-05", "2024-01-01 22:00:00-05", "2024-01-01 23:00:00-05"], vec![2, 1, 0]);
    plot.add_trace(trace);
    plot.show();

    // plot.write_html("out.html");
}
