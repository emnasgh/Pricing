import duckdb
conn = duckdb.connect()
df = conn.execute("SELECT * FROM read_parquet('/opt/airflow/data/prices.parquet') LIMIT 1").df()
print(list(df.columns))
conn.close()