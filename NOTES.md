

See the cargo book https://doc.rust-lang.org/cargo/guide/project-layout.html
for various topics

 - To run ALL tests do `cargo test`
 - To run the tests with output do `cargo test -- --nocapture`
 - To run the tests only from `interval.rs` with output do `cargo test -- --nocapture interval`
 - To run an example by hand do `cargo run -r --example calendar_bench`.  Note the 
   section `[[example]]` in the `Cargo.toml` file.  The flag `-r` runs in release 
   mode.  


