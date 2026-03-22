import duckdb
duck = duckdb.connect()
duck.execute("LOAD postgres;")
duck.execute("ATTACH 'host=postgres port=5432 dbname=retail_pricing user=postgres password=admin123' AS pg (TYPE postgres);")
result = duck.execute("SELECT COUNT(*) FROM pg.products_raw").fetchone()[0]
print(f"OK - {result} lignes")
duck.close()