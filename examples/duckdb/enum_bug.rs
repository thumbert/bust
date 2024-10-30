use duckdb::{Connection, Result};

#[derive(Debug)]
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

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }
    Ok(())
}
