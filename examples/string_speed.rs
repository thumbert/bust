use std::{time::Instant, vec};

fn search_token(lines: &[String], token: &str) {
    let start = Instant::now();
    let mut res: Vec<String> = vec![];
    for line in lines {
        if line.contains(token) {
            res.push(line.to_string());
        }
    }
    println!(
        "Rust found {} lines containing token '{}'.  Elapsed time: {:.3?}",
        res.len(),
        token,
        start.elapsed()
    );
}

fn duckdb_search(tokens: Vec<&str>) {
    let mut start = Instant::now();
    let con = duckdb::Connection::open("/home/adrian/Downloads/Archive/PnodeTable/locations.duckdb").unwrap();

    for token in tokens {
        let mut stmt = con
            .prepare("SELECT name FROM locations WHERE name LIKE ?;")
            .unwrap();
        let token_for_query = format!("%{}%", token);
        let mut rows = stmt.query([token_for_query]).unwrap();
        let mut res: Vec<String> = vec![];
        while let Some(row) = rows.next().unwrap() {
            let line: String = row.get(0).unwrap();
            res.push(line);
        }
        println!(
            "DuckDB found {} lines containing token '{}'.  Elapsed time: {:.3?}",
            res.len(),
            token,
            start.elapsed()
        );
        start = Instant::now();
    }
}

fn main() {
    let csv_path = "/home/adrian/Downloads/Archive/PnodeTable/locations.csv";
    let contents = std::fs::read_to_string(csv_path).expect("Failed to read CSV file");
    let lines: Vec<String> = contents.lines().map(|line| line.to_string()).collect();

    println!("Total lines: {}", lines.len());
    search_token(&lines, "4004");
    search_token(&lines, "BGE");

    duckdb_search(vec!["4004", "BGE"]);
}
