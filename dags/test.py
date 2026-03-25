import pyarrow.parquet as pq
pf = pq.ParquetFile('/opt/airflow/data/prices.parquet')
batch = next(pf.iter_batches(batch_size=3))
df = batch.to_pandas()
print(df.columns.tolist())
print(df.dtypes)
print(df.head(3))