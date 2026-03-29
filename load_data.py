import duckdb

con = duckdb.connect("data/nyc_taxi.duckdb")

con.execute("""
CREATE OR REPLACE TABLE raw_yellow_trips AS
SELECT * FROM read_parquet('data/yellow_tripdata_2023-*.parquet');
""")

print("Raw data loaded into DuckDB")
