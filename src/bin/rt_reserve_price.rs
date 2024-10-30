use std::error::Error;

use duckdb::Connection;

fn main() -> Result<(), Box<dyn Error>> {
    let conn = Connection::open(
        "/home/adrian/Downloads/Archive/IsoExpress/PricingReports/RtReservePrice/foo.duckdb",
    )?;

    conn.execute_batch(
        r"
CREATE TABLE IF NOT EXISTS foo (
    IntervalBeginning5Min TIMESTAMPTZ,
    Ros10MinSpinRequirement FLOAT,
    RosTotal10MinRequirement FLOAT,
    RosTotal30MinRequirement FLOAT,
    RosTmsrDesignatedMw FLOAT,
    RosTmnsrDesignatedMw FLOAT,
    RosTmorDesignatedMw FLOAT,
    RosTmsrClearingPrice FLOAT,
    RosTmnsrClearingPrice FLOAT,
    RosTmorClearingPrice FLOAT,
    SwctTotal30MinRequirement FLOAT,
    SwctTmsrDesignatedMw FLOAT,
    SwctTmnsrDesignatedMw FLOAT,
    SwctTmorDesignatedMw FLOAT,
    SwctTmsrClearingPrice FLOAT,
    SwctTmnsrClearingPrice FLOAT,
    SwctTmorClearingPrice FLOAT,
    CtTotal30MinRequirement FLOAT,
    CtTmsrDesignatedMw FLOAT,
    CtTmnsrDesignatedMw FLOAT,
    CtTmorDesignatedMw FLOAT,
    CtTmsrClearingPrice FLOAT,
    CtTmnsrClearingPrice FLOAT,
    CtTmorClearingPrice FLOAT,
    NemabstnTotal30MinRequirement FLOAT,
    NemabstnTmsrDesignatedMw FLOAT,
    NemabstnTmnsrDesignatedMw FLOAT,
    NemabstnTmorDesignatedMw FLOAT,
    NemabstnTmsrClearingPrice FLOAT,
    NemabstnTmnsrClearingPrice FLOAT,
    NemabstnTmorClearingPrice FLOAT
);
INSERT INTO foo
FROM read_csv(
    '/home/adrian/Downloads/Archive/IsoExpress/PricingReports/RtReservePrice/month/rt_reserve_price_2021-01.csv.gz', 
    header = true, 
    timestampformat = '%Y-%m-%dT%H:%M:%S.000%z');
        ")?;

    Ok(())
}
