use duckdb::{
    arrow::array::{Array, ArrayAccessor, AsArray, Datum, StringArray},
    params,
    types::{EnumType::UInt8, Value, ValueRef},
    Connection, Result, Row,
};

#[derive(Debug)]
enum State {
    CA,
    NY,
}

#[derive(Debug)]
struct Data {
    state: State,
    value: i32,
}


/// See https://github.com/duckdb/duckdb-rs/issues/365
/// Not a fan of what is needed to make this simple case work 
fn main() -> Result<()> {
    let conn = Connection::open_in_memory().unwrap();
    let _ = conn.execute(
        r#"
CREATE TABLE stats (
    name ENUM('CA', 'NY'),
    value INTEGER,
);
"#,
        [],
    );
    let _ = conn.execute("INSERT INTO stats VALUES (?, ?);", params!["CA", 10]);
    let _ = conn.execute("INSERT INTO stats VALUES (?, ?);", params!["CA", 20]);
    let _ = conn.execute("INSERT INTO stats VALUES (?, ?);", params!["NY", 4]);

    let mut idx = -1;
    let query = "SELECT * FROM stats;";
    let mut stmt = conn.prepare(query).unwrap();
    let res_iter = stmt.query_map([], |row| {
        idx += 1;
        let state = match row.get_ref_unwrap(0) {
            ValueRef::Enum(e, idx) => match e {
                UInt8(v) => v
                    .values()
                    .as_any()
                    .downcast_ref::<StringArray>()
                    .unwrap()
                    .value(v.key(idx).unwrap()),
                _ => panic!("Unknown state"),
            },
            _ => panic!("Oops, first column should be an enum"),
        };
        println!("state: {:?}", state);
        Ok(Data {
            state: match state {
                "CA" => State::CA,
                "NY" => State::NY,
                _ => panic!("Unknown state"),
            },
            value: row.get(1)?,
        })
    })?;
    let vs: Vec<Data> = res_iter.map(|e| e.unwrap()).collect();
    println!("\n\n\nDone");
    println!("{:?}", vs);

    Ok(())
}

// let mut idx = -1;

// lines below work, just clunky
//------------------------------------------------------
// let state = match row.get_ref_unwrap(0) {
//     ValueRef::Enum(e, idx) => match e {
//         UInt8(v) => v
//             .values()
//             .as_any()
//             .downcast_ref::<StringArray>()
//             .unwrap()
//             .value(v.key(idx).unwrap()),
//         _ => panic!("Unknown state"),
//     },
//     _ => panic!("Oops, first column should be an enum"),
// };
// println!("state: {:?}", state);
// Ok(Data {
//     state: match state {
//         "CA" => State::CA,
//         "NY" => State::NY,
//         _ => panic!("Unknown state"),
//     },
//     value: row.get(1)?,
// })
//------------------------------------------------------
