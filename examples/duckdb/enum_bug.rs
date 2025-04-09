use duckdb::{
    // arrow::array::StringArray, types::EnumType::UInt8, types::ValueRef, 
    Connection, Result,
};

#[derive(Debug)]
#[allow(dead_code)]
struct Person {
    id: i32,
    name: String,
}

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        r#"
CREATE TABLE enum_test (
    id INTEGER,
    name ENUM('AA', 'BB', 'CC')
);
INSERT INTO enum_test VALUES (0, 'BB');
INSERT INTO enum_test VALUES (1, 'AA');
INSERT INTO enum_test VALUES (2, 'CC');
INSERT INTO enum_test VALUES (3, 'BB');
    "#,
    )?;
    let mut stmt = conn.prepare("SELECT id, name FROM enum_test")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {
            id: row.get(0)?,
            name: match row.get_ref_unwrap(1).to_owned() {
                duckdb::types::Value::Enum(e) => e,
                _ => panic!("Oops"),
            },
        })
    })?;

    // ---->  THIS GOT FIXED IN 1.2.1  <----
    // // To get the correct enum value, you need to do this gymnastics
    // let person_iter = stmt.query_map([], |row| {
    //     let name = match row.get_ref_unwrap(1) {
    //         ValueRef::Enum(e, idx) => match e {
    //             UInt8(v) => v
    //                 .values()
    //                 .as_any()
    //                 .downcast_ref::<StringArray>()
    //                 .unwrap()
    //                 .value(v.key(idx).unwrap()),
    //             _ => panic!("Unknown name"),
    //         },
    //         _ => panic!("Oops, column should be an enum"),
    //     };
    //     Ok(Person {
    //         id: row.get(0)?,
    //         name: name.to_string(),
    //     })
    // })?;
    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }
    // Found person Person { id: 0, name: "BB" }
    // Found person Person { id: 1, name: "AA" }
    // Found person Person { id: 2, name: "CC" }
    // Found person Person { id: 3, name: "BB" }

    Ok(())
}
