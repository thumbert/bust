use actix_web::{get, web, HttpResponse, Responder};

use duckdb::{AccessMode, Config, Connection, Result};
use itertools::Itertools;
use jiff::{
    civil::Date,
    ToSpan,
};
use serde::{Deserialize, Serialize};


#[derive(Debug, Deserialize)]
struct OffersQuery {
    /// one or more masked asset ids, separated by commas
    masked_asset_ids: Option<String>,
}

#[get("/nyiso/energy_offers/dam/energy_offers/start/{start}/end/{end}")]
async fn api_da_offers(
    path: web::Path<(String, String)>,
    query: web::Query<OffersQuery>,
) -> impl Responder {
    let config = Config::default().access_mode(AccessMode::ReadOnly).unwrap();
    let conn = Connection::open_with_flags(get_path(), config).unwrap();

    let start_date: Date = path.0.to_string().parse().unwrap();
    let end_date: Date = path.1.to_string().parse().unwrap();
    let asset_ids: Option<Vec<i32>> = query
        .masked_asset_ids
        .as_ref()
        .map(|ids| ids.split(',').map(|e| e.parse::<i32>().unwrap()).collect());

    let offers = get_energy_offers(&conn, start_date, end_date, asset_ids).unwrap();
    HttpResponse::Ok().json(offers)
}

#[derive(Debug, PartialEq, Serialize)]
pub struct EnergyOffer {
    masked_asset_id: u32,
    timestamp_s: i64, // seconds since epoch of hour beginning
    segment: u8,
    price: f32,
    quantity: f32,
}

// Get the masked unit ids between a [start, end] date
pub fn get_unit_ids(conn: &Connection, start: Date, end: Date) -> Vec<u32> {
    let mut query = String::from("SELECT DISTINCT \"Masked Gen ID\" from da_offers ");
    query.push_str(&format!("WHERE \"Date Time\" >= '{}' ", start));
    query.push_str(&format!(
        "AND \"Date Time\" < '{}' ",
        end.checked_add(1.day()).ok().unwrap()
    ));
    query.push_str("ORDER BY \"Masked Gen ID\"");
    query.push(';');
    // println!("{}", query);

    let mut stmt = conn.prepare(&query).unwrap();
    let mut rows = stmt.query([]).unwrap();
    let mut ids: Vec<u32> = Vec::new();
    while let Some(row) = rows.next().unwrap() {
        ids.push(row.get(0).unwrap());
    }
    ids
}

// Get the energy offers between a [start, end] date for a list of units and participant ids
pub fn get_energy_offers(
    conn: &Connection,
    start: Date,
    end: Date,
    masked_unit_ids: Option<Vec<i32>>,
    // masked_participant_ids: Vec<i32>,
) -> Result<Vec<EnergyOffer>> {
    let query = format!(
        r#"
WITH unpivot_alias AS (
    UNPIVOT (
        SELECT "Masked Gen ID", "Date Time", 
            "Dispatch $/MW1",
            "Dispatch $/MW2",
            "Dispatch $/MW3",
            "Dispatch $/MW4",
            "Dispatch $/MW5",
            "Dispatch $/MW6",
            "Dispatch $/MW7",
            "Dispatch $/MW8",
            "Dispatch $/MW9",
            "Dispatch $/MW10",
            "Dispatch $/MW11",
            "Dispatch $/MW12",
            "Dispatch MW1" AS MW1, 
            ROUND("Dispatch MW2" - "Dispatch MW1", 1) AS MW2, 
            ROUND("Dispatch MW3" - "Dispatch MW2", 1) AS MW3, 
            ROUND("Dispatch MW4" - "Dispatch MW3", 1) AS MW4, 
            ROUND("Dispatch MW5" - "Dispatch MW4", 1) AS MW5, 
            ROUND("Dispatch MW6" - "Dispatch MW5", 1) AS MW6, 
            ROUND("Dispatch MW7" - "Dispatch MW6", 1) AS MW7, 
            ROUND("Dispatch MW8" - "Dispatch MW7", 1) AS MW8, 
            ROUND("Dispatch MW9" - "Dispatch MW8", 1) AS MW9, 
            ROUND("Dispatch MW10" - "Dispatch MW9", 1) AS MW10, 
            ROUND("Dispatch MW11" - "Dispatch MW10", 1) AS MW11, 
            ROUND("Dispatch MW12" - "Dispatch MW11", 1) AS MW12,  
        FROM da_offers
        WHERE "Date Time" >= '{}'
        AND "Date Time" < '{}'
        {}
        AND "Market" == 'DAM'
    )
    ON  ("MW1", "Dispatch $/MW1") AS "0", 
        ("MW2", "Dispatch $/MW2") AS "1", 
        ("MW3", "Dispatch $/MW3") AS "2",
        ("MW4", "Dispatch $/MW4") AS "3", 
        ("MW5", "Dispatch $/MW5") AS "4",
        ("MW6", "Dispatch $/MW6") AS "5", 
        ("MW7", "Dispatch $/MW7") AS "6",
        ("MW8", "Dispatch $/MW8") AS "7", 
        ("MW9", "Dispatch $/MW9") AS "8",
        ("MW10", "Dispatch $/MW10") AS "9", 
        ("MW11", "Dispatch $/MW11") AS "10",
        ("MW12", "Dispatch $/MW12") AS "11",
    INTO  
        NAME Segment
        VALUE MW, Price
)
SELECT "Masked Gen ID", 
    "Date Time", 
    CAST("Segment" as UTINYINT) AS Segment, 
    "MW", "Price", 
FROM unpivot_alias
ORDER BY "Masked Gen ID", "Date Time", "Price";    
    "#,
        start
            .intz("America/New_York")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        end.intz("America/New_York")
            .unwrap()
            .checked_add(1.day())
            .ok()
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        match masked_unit_ids {
            Some(ids) => format!("AND \"Masked Gen ID\" in ({}) ", ids.iter().join(", ")),
            None => "".to_string(),
        }
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let micro: i64 = row.get(1).unwrap();
        Ok(EnergyOffer {
            masked_asset_id: row.get(0).unwrap(),
            timestamp_s: micro / 1_000_000,
            segment: row.get(2)?,
            price: row.get(4)?,
            quantity: row.get(3)?,
        })
    })?;
    let offers: Vec<EnergyOffer> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}

/// Construct the stack
pub fn get_stack(
    conn: &Connection,
    start: Date,
    end: Date,
) -> Result<Vec<EnergyOffer>> {
    let query = format!(r#"
        WITH unpivot_alias AS (
            UNPIVOT (
                SELECT "Masked Gen ID", "Date Time", 
                    "Dispatch $/MW1",
                    "Dispatch $/MW2",
                    "Dispatch $/MW3",
                    "Dispatch $/MW4",
                    "Dispatch $/MW5",
                    "Dispatch $/MW6",
                    "Dispatch $/MW7",
                    "Dispatch $/MW8",
                    "Dispatch $/MW9",
                    "Dispatch $/MW10",
                    "Dispatch $/MW11",
                    "Dispatch $/MW12",
                    "Dispatch MW1" AS MW1, 
                    ROUND("Dispatch MW2" - "Dispatch MW1", 1) AS MW2, 
                    ROUND("Dispatch MW3" - "Dispatch MW2", 1) AS MW3, 
                    ROUND("Dispatch MW4" - "Dispatch MW3", 1) AS MW4, 
                    ROUND("Dispatch MW5" - "Dispatch MW4", 1) AS MW5, 
                    ROUND("Dispatch MW6" - "Dispatch MW5", 1) AS MW6, 
                    ROUND("Dispatch MW7" - "Dispatch MW6", 1) AS MW7, 
                    ROUND("Dispatch MW8" - "Dispatch MW7", 1) AS MW8, 
                    ROUND("Dispatch MW9" - "Dispatch MW8", 1) AS MW9, 
                    ROUND("Dispatch MW10" - "Dispatch MW9", 1) AS MW10, 
                    ROUND("Dispatch MW11" - "Dispatch MW10", 1) AS MW11, 
                    ROUND("Dispatch MW12" - "Dispatch MW11", 1) AS MW12,  
                FROM da_offers
                WHERE "Date Time" >= '{}'
                AND "Date Time" < '{}'

                WHERE "Date Time" >= '2024-03-01 00:00:00-05:00'
                AND "Date Time" < '2024-03-01 23:00:00-05:00'
                AND "Market" == 'DAM'
            )
            ON  ("MW1", "Dispatch $/MW1") AS "0", 
                ("MW2", "Dispatch $/MW2") AS "1", 
                ("MW3", "Dispatch $/MW3") AS "2",
                ("MW4", "Dispatch $/MW4") AS "3", 
                ("MW5", "Dispatch $/MW5") AS "4",
                ("MW6", "Dispatch $/MW6") AS "5", 
                ("MW7", "Dispatch $/MW7") AS "6",
                ("MW8", "Dispatch $/MW8") AS "7", 
                ("MW9", "Dispatch $/MW9") AS "8",
                ("MW10", "Dispatch $/MW10") AS "9", 
                ("MW11", "Dispatch $/MW11") AS "10",
                ("MW12", "Dispatch $/MW12") AS "11",
            INTO  
                NAME Segment
                VALUE MW, Price
        )
        SELECT *, 
            ROUND(SUM("MW") OVER (PARTITION BY "Date Time" ORDER BY "Idx"), 1) AS "cum_MW"   
        FROM (
            SELECT *,
                row_number() OVER (PARTITION BY "Date Time") AS "Idx",
            FROM (
                SELECT "Masked Gen ID", 
                    "Date Time", 
                    CAST("Segment" as UTINYINT) AS Segment, 
                    "MW", "Price", 
                FROM unpivot_alias
                ORDER BY "Date Time" ASC, Price ASC
            )
        )
        ORDER BY "Date Time", "Idx";
    "#,
        start
            .intz("America/New_York")
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
        end.intz("America/New_York")
            .unwrap()
            .checked_add(1.day())
            .ok()
            .unwrap()
            .strftime("%Y-%m-%d %H:%M:%S.000%:z"),
    );
    // println!("{}", query);
    let mut stmt = conn.prepare(&query).unwrap();
    let offers_iter = stmt.query_map([], |row| {
        let micro: i64 = row.get(1).unwrap();
        Ok(EnergyOffer {
            masked_asset_id: row.get(0).unwrap(),
            timestamp_s: micro / 1_000_000,
            segment: row.get(2)?,
            price: row.get(4)?,
            quantity: row.get(3)?,
        })
    })?;
    let offers: Vec<EnergyOffer> = offers_iter.map(|e| e.unwrap()).collect();

    Ok(offers)
}


fn get_path() -> String {
    "/home/adrian/Downloads/Archive/Nyiso/nyiso_energy_offers.duckdb".to_string()
}


#[cfg(test)]
mod tests {
    use duckdb::{AccessMode, Config, Connection, Result};
    use jiff::civil::{date, Date};

    use crate::api::nyiso::energy_offers::*;

    #[test]
    fn test_get_masked_unit_ids() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let start: Date = date(2023, 1, 1);
        let end: Date = date(2023, 1, 31);
        let ids = get_unit_ids(&conn, start, end);
        assert_eq!(ids.len(), 316);
        conn.close().unwrap();
        Ok(())
    }

    #[test]
    fn test_get_offers() -> Result<()> {
        let config = Config::default().access_mode(AccessMode::ReadOnly)?;
        let conn = Connection::open_with_flags(get_path(), config).unwrap();
        let xs = get_energy_offers(
            &conn,
            date(2024, 3, 1),
            date(2024, 3, 1),
            Some(vec![35537750, 55537750, 67537750, 75537750]),
        )
        .unwrap();
        conn.close().unwrap();
        let x0 = xs.first().unwrap();
        assert_eq!(
            *x0,
            EnergyOffer {
                masked_asset_id: 35537750,
                timestamp_s: 1709269200,
                segment: 0,
                price: 15.6,
                quantity: 150.0
            }
        );
        assert_eq!(xs.len(), 672);
        Ok(())
    }
}
