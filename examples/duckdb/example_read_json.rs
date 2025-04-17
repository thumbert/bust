use duckdb::{Connection, Result};
use jiff::Timestamp;


/// See another example in daas_strike_price.sql
fn main() -> Result<()> {
    let conn = Connection::open_in_memory()?;
    conn.execute_batch(
        r#"
CREATE TEMPORARY TABLE tmp
AS
    SELECT unnest(SingleSrcContingencyLimits.SingleSrcContingencyLimit, recursive := true)
    FROM read_json('~/Downloads/Archive/IsoExpress/SingleSourceContingency/Raw/2025/ssc_2025-01-10.json.gz')
;
SELECT * from tmp;
    "#,
    )?;
    let mut stmt =
        conn.prepare("SELECT BeginDate::TIMESTAMPTZ, RtFlowMw FROM tmp WHERE InterfaceName = 'Millstone 3';")?;
    let item_iter = stmt.query_map([], |row| {
        let ts = Timestamp::from_second(row.get::<usize,i64>(0).unwrap() / 1_000_000).unwrap();
        Ok((ts, row.get::<usize,f64>(1)?))
    })?;

    for item in item_iter {
        println!("{:?}", item.unwrap());
    }

    Ok(())
}
