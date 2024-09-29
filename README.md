# bust
Beginning Rust

 To run a development server:
 * Run `cargo test`
 * Run `cargo build`
 * Launch as `./target/debug/server --port=8112`

To release a new version:
 * Run `cargo test`
 * Run `cargo build --release`  Took 13m 44s!
 * cp ./target/release/server ~/Software
 * Launch `~/Software/server` 


To check which process uses the 8111 port:
`lsof -i :8111`

