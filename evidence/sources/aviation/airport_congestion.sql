-- evidence/sources/aviation/airport_congestion.sql
SELECT * FROM read_parquet('C:/DE2/aviation_capstone/aviation_capstone/evidence/sources/aviation/main/mart_airport_congestion/*.parquet')