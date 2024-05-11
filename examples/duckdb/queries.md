

Import all the data from a list of CSV file.  I created the file with 
`src/bin/mra_bids_offers.rs`. 
```sql
CREATE TABLE mra 
  AS FROM read_csv(
    '/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction/tmp/*.csv');
```
What happens if you insert the same data multiple times?  Does it get repeated?
Yes it does.  


Insert one file at a time ...
```sql 
INSERT INTO mra
  AS FROM '/home/adrian/Documents/repos/git/thumbert/bust/mra_2023-12.csv';
```

```sql
EXPORT DATABASE '/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction/Duck' (
    FORMAT PARQUET,
    COMPRESSION ZSTD,
    ROW_GROUP_SIZE 100_000
);
```

```sql
IMPORT DATABASE '/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction/Duck';
```

```bash
./duckdb -csv -c \
  "IMPORT DATABASE '/home/adrian/Downloads/Archive/IsoExpress/Capacity/HistoricalBidsOffers/MonthlyAuction/Duck';
SELECT BidOffer, MaskedCapacityZoneId, SUM(Quantity) AS Quantity 
FROM mra
WHERE month = 202401
GROUP BY BidOffer, MaskedCapacityZoneId
ORDER BY BidOffer, MaskedCapacityZoneId;"
```


SELECT COUNT(*) FROM mra;

DESCRIBE mra;
.mode line
SELECT * FROM mra LIMIT 1;
.mode duckdb

SELECT BidOffer, SUM(Quantity) AS Quantity FROM mra
GROUP BY BidOffer;

**Note** The ISO messed up the columns in the CSV report for historical 
bids/offers.  The segment price/column are inverted!  Ouch.  


The total quantity for Bids and Offers matches the ISO report 
```sql
SELECT BidOffer, MaskedCapacityZoneId, ROUND(SUM(Quantity)) AS Quantity 
FROM mra
WHERE month = 202401
GROUP BY BidOffer, MaskedCapacityZoneId
ORDER BY BidOffer, MaskedCapacityZoneId;
```

Total quantity for exports almost matches the ISO report
```sql
SELECT BidOffer, MaskedCapacityZoneId, ROUND(SUM(Quantity)) AS Quantity 
FROM mra
WHERE month = 202401
AND MaskedExternalInterfaceId IS NOT NULL
GROUP BY BidOffer, MaskedCapacityZoneId
ORDER BY BidOffer, MaskedCapacityZoneId;
```

There is a discrepancy in the offers from ROP.  The detail file has 246 MW, 
the ISO summary report shows 256.1 MW.
```sql
SELECT * 
FROM mra
WHERE month = 202401
AND MaskedExternalInterfaceId IS NOT NULL
AND MaskedCapacityZoneId = 8500
AND BidOffer = 'offer';
```

Export query to CSV
```sql
COPY (
SELECT * 
FROM mra
WHERE month = 202401
AND MaskedExternalInterfaceId IS NOT NULL
AND MaskedCapacityZoneId = 8500
AND BidOffer = 'offer'
) 
TO 'output.csv' (HEADER, DELIMITER ',');

```



 
Clearing quantities match the ISO summary report.  The clearing price is 
3.938 for all capacity zones. 
```sql
SELECT BidOffer, MaskedCapacityZoneId, SUM(Price) AS Price FROM mra
WHERE BidOffer = 'Bid' 
AND Quantity >= 3.938
AND MaskedExternalInterfaceId IS NULL
GROUP BY BidOffer, MaskedCapacityZoneId
ORDER BY BidOffer, MaskedCapacityZoneId;
```

```sql
SELECT BidOffer, MaskedCapacityZoneId, SUM(Price) AS Price FROM mra
WHERE BidOffer = 'Offer' 
AND Quantity <= 3.938
AND MaskedExternalInterfaceId IS NULL
GROUP BY BidOffer, MaskedCapacityZoneId
ORDER BY BidOffer, MaskedCapacityZoneId;
```

