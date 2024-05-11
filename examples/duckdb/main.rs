use duckdb::{Connection, Result};

#[derive(Debug)]
struct Person { id: i32, name: String, }

fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;

    conn.execute_batch(
        r"CREATE TABLE person (id BIGINT, name VARCHAR);
          INSERT INTO person VALUES (42, 'John');
        ")?;
    let mut stmt = conn.prepare("SELECT id, name FROM person")?;
    let person_iter = stmt.query_map([], |row| {
        Ok(Person {id: row.get(0)?, name: row.get(1)?})
    })?;

    for person in person_iter {
        println!("Found person {:?}", person.unwrap());
    }
    Ok(())
}