use duckdb::Connection;
// use r2d2::PooledConnection;
// use r2d2_duckdb::DuckDBConnectionManager;


pub fn get_participant_ids(conn: Connection) -> Vec<i64> {
    let mut stmt = conn
        .prepare("SELECT DISTINCT maskedParticipantId from mra")
        .unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut ids: Vec<i64> = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        ids.push(row.get(0).unwrap());
    }
    ids
}

#[cfg(test)]
mod tests {
    use duckdb::Connection;

    use crate::api::isone::monthly_capacity_auction::get_participant_ids;

    #[test]
    fn test_participant_ids() {
        let conn = Connection::open("/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction/mra.duckdb").unwrap();
        let ids = get_participant_ids(conn);
        assert!(ids.len() >= 107);
    }
}
