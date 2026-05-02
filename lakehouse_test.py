import duckdb
conn = duckdb.connect()
result = conn.execute("SELECT departure, arrival, airline, flight FROM read_parquet('lakehouse/raw/flights/date=2026-05-02/*.parquet') LIMIT 1").df()
print('departure:', result['departure'][0])
print('arrival:', result['arrival'][0])
print('airline:', result['airline'][0])
print('flight:', result['flight'][0])
conn.close()