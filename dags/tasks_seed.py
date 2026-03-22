import time
import io
import os
import json
import math
import gzip
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timezone
from common import get_db_connection, log_pipeline

PARQUET_PATH_food   = "/opt/airflow/data/food.parquet"
PARQUET_PATH_beauty = "/opt/airflow/data/beauty.parquet"
PARQUET_PATH_prices = "/opt/airflow/data/prices.parquet"
JSONL_PATH_other    = "/opt/airflow/data/openproductsfacts-products.jsonl"

CHUNK = 20000  # augmenté pour accélérer


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return None if np.isnan(obj) else float(obj)
        if isinstance(obj, float) and math.isnan(obj):
            return None
        if isinstance(obj, pd.Timestamp):
            return obj.isoformat()
        if hasattr(obj, 'item'):
            return obj.item()
        return super().default(obj)


def to_json_safe(item):
    for key in list(item.keys()):
        val = item[key]
        if isinstance(val, str) and (val.startswith('[{') or val.startswith('{"')):
            try:
                item[key] = json.loads(val)
            except Exception:
                item[key] = None
    return json.dumps(item, cls=NumpyEncoder).replace(': NaN', ': null').replace(':NaN', ':null')


def inserer_via_copy(rows, table, conn):
    """Insère via COPY + table temporaire SANS contraintes + DISTINCT ON CONFLICT."""
    if not rows:
        return 0
    cursor = conn.cursor()
    cursor.execute("""
        CREATE TEMP TABLE tmp_load (
            id          VARCHAR,
            raw_data    TEXT,
            api_updated TIMESTAMPTZ,
            inserted_at TIMESTAMPTZ
        ) ON COMMIT DROP;
    """)
    buf = io.StringIO()
    for row in rows:
        id_val, raw_data, api_updated = row
        ts  = api_updated.strftime("%Y-%m-%d %H:%M:%S%z") if api_updated else ""
        raw = str(raw_data).replace("\t", " ").replace("\n", " ")
        buf.write(f"{id_val}\t{raw}\t{ts}\t\\N\n")
    buf.seek(0)
    cursor.copy_expert("""
        COPY tmp_load (id, raw_data, api_updated, inserted_at)
        FROM STDIN
        WITH (FORMAT text, NULL '\\N', DELIMITER E'\t')
    """, buf)
    cursor.execute(f"""
        INSERT INTO {table} (id, raw_data, api_updated, inserted_at)
        SELECT DISTINCT ON (id, api_updated) id, raw_data, api_updated, NOW()
        FROM tmp_load
        ON CONFLICT (id, api_updated) DO NOTHING
    """)
    inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    return inserted


def charger_parquet_products(parquet_path, id_field, table, conn, source, skip_rows=0):
    """Lit parquet par chunks PyArrow avec reprise possible."""
    pf        = pq.ParquetFile(parquet_path)
    inserted  = 0
    chunk_num = 0
    skipped   = 0

    for batch in pf.iter_batches(batch_size=CHUNK):
        chunk_num += 1

        if skipped < skip_rows:
            skipped += CHUNK
            print(f"{source} chunk {chunk_num} → skip ({skipped}/{skip_rows})")
            continue

        df = batch.to_pandas()

        if id_field not in df.columns:
            continue

        # garder id comme VARCHAR — cohérent avec la colonne products_raw.id
        df = df[df[id_field].notna()].copy()
        df[id_field] = df[id_field].astype(str).str.strip()
        df = df[df[id_field] != ''].copy()
        df = df[df[id_field] != 'nan'].copy()

        if "last_modified_t" in df.columns:
            df["_api_updated"] = pd.to_datetime(
                df["last_modified_t"], unit="s", utc=True, errors="coerce"
            )
        else:
            df["_api_updated"] = None

        rows = []
        for _, row in df.iterrows():
            item = row.to_dict()
            item.pop("_api_updated", None)
            api_updated = row.get("_api_updated")
            if api_updated is not None and pd.isna(api_updated):
                api_updated = None
            try:
                rows.append((
                    str(row[id_field]),
                    to_json_safe(item),
                    api_updated.to_pydatetime() if hasattr(api_updated, 'to_pydatetime') else api_updated
                ))
            except Exception:
                continue

        n         = inserer_via_copy(rows, table, conn)
        inserted += n
        print(f"{source} chunk {chunk_num} ({len(rows)} lignes) | +{n} | total : {inserted}")

    return inserted


def charger_parquet_prices(parquet_path, table, conn, source):
    """Lit prices.parquet par chunks PyArrow."""
    pf        = pq.ParquetFile(parquet_path)
    inserted  = 0
    chunk_num = 0

    for batch in pf.iter_batches(batch_size=CHUNK):
        df = batch.to_pandas()

        if "id" not in df.columns:
            continue

        df = df[df["id"].notna()].copy()

        if "updated" in df.columns:
            df["_api_updated"] = pd.to_datetime(
                df["updated"], unit="s", utc=True, errors="coerce"
            )
        else:
            df["_api_updated"] = None

        rows = []
        for _, row in df.iterrows():
            item = row.to_dict()
            item.pop("_api_updated", None)
            api_updated = row.get("_api_updated")
            if api_updated is not None and pd.isna(api_updated):
                api_updated = None
            try:
                rows.append((
                    str(int(row["id"])),
                    to_json_safe(item),
                    api_updated.to_pydatetime() if hasattr(api_updated, 'to_pydatetime') else api_updated
                ))
            except Exception:
                continue

        n          = inserer_via_copy(rows, table, conn)
        inserted  += n
        chunk_num += 1
        print(f"{source} chunk {chunk_num} ({len(rows)} lignes) | +{n} | total : {inserted}")

    return inserted


def charger_jsonl(jsonl_path, table, conn, source):
    """Lit le JSONL (gz ou non) ligne par ligne — zéro RAM."""
    inserted  = 0
    chunk_num = 0
    batch     = []

    try:
        conn.rollback()
    except Exception:
        pass

    if os.path.exists(jsonl_path):
        jsonl_path_real = jsonl_path
        open_func       = open
    elif os.path.exists(jsonl_path + '.gz'):
        jsonl_path_real = jsonl_path + '.gz'
        open_func       = gzip.open
    else:
        raise FileNotFoundError(f"Fichier introuvable : {jsonl_path} ou {jsonl_path}.gz")

    print(f"Lecture {jsonl_path_real}...")

    with open_func(jsonl_path_real, "rt", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                item = json.loads(line)
                code = str(item.get("code", "")).strip()[:50]
                if not code:
                    continue

                ts          = item.get("last_modified_t")
                api_updated = datetime.fromtimestamp(ts, tz=timezone.utc) if ts else None
                batch.append((code, to_json_safe(item), api_updated))

                if len(batch) >= CHUNK:
                    try:
                        n          = inserer_via_copy(batch, table, conn)
                        inserted  += n
                        chunk_num += 1
                        print(f"{source} chunk {chunk_num} | +{n} | total : {inserted}")
                    except Exception as e:
                        conn.rollback()
                        print(f"Erreur chunk {chunk_num} ignoré : {e}")
                    batch = []

            except Exception:
                continue

    if batch:
        try:
            n         = inserer_via_copy(batch, table, conn)
            inserted += n
            print(f"{source} dernier chunk | +{n} | total : {inserted}")
        except Exception as e:
            conn.rollback()
            print(f"Erreur dernier chunk ignoré : {e}")

    return inserted


def seed_prices(**context):
    start_time    = time.time()
    rows_inserted = 0
    rows_ignored  = 0
    rows_errors   = 0
    error_message = None
    status        = "failed"

    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM prices_raw")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        if count > 0:
            print(f"Déjà seedé ({count} lignes) → skip")
            status = "success"
            return

        print("Seed prices : PyArrow streaming → PostgreSQL COPY...")
        conn          = get_db_connection()
        rows_inserted = charger_parquet_prices(
            PARQUET_PATH_prices, "prices_raw", conn, "Prices"
        )
        conn.close()

        status = "success"
        print(f"Seed prices terminé : {rows_inserted} lignes insérées")

    except Exception as e:
        status        = "failed"
        error_message = str(e)
        print(f"Erreur : {e}")
        raise
    finally:
        duration = time.time() - start_time
        log_pipeline("pipeline_seed", "seed_prices", rows_inserted, rows_ignored, rows_errors, status, duration, error_message)


def seed_products(**context):
    start_time    = time.time()
    rows_inserted = 0
    rows_ignored  = 0
    rows_errors   = 0
    error_message = None
    status        = "failed"

    try:
        conn   = get_db_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*) FROM products_raw")
        count = cursor.fetchone()[0]
        cursor.close()
        conn.close()

        total_food    = pq.ParquetFile(PARQUET_PATH_food).metadata.num_rows
        total_beauty  = pq.ParquetFile(PARQUET_PATH_beauty).metadata.num_rows
        total_parquet = total_food + total_beauty

        print(f"Food : {total_food} | Beauty : {total_beauty} | Déjà en base : {count}")

        conn = get_db_connection()

        if count < total_parquet:
            skip_food = min(count, total_food)
            print(f"Chargement food (reprise à partir de {skip_food} lignes)...")
            n              = charger_parquet_products(
                PARQUET_PATH_food, "code", "products_raw", conn, "Food",
                skip_rows=skip_food
            )
            rows_inserted += n
            print(f"Food terminé : {n} lignes insérées cette session")

            print("Chargement beauty...")
            n              = charger_parquet_products(
                PARQUET_PATH_beauty, "code", "products_raw", conn, "Beauty",
                skip_rows=0
            )
            rows_inserted += n
            print(f"Beauty terminé : {n} lignes insérées")
        else:
            print("Food + Beauty déjà seedés → skip")

        print("Chargement JSONL other...")
        n              = charger_jsonl(
            JSONL_PATH_other, "products_raw", conn, "Other"
        )
        rows_inserted += n
        print(f"Other terminé : {n} lignes insérées")

        conn.close()
        status = "success"
        print(f"Seed products terminé : {rows_inserted} lignes total insérées")

    except Exception as e:
        status        = "failed"
        error_message = str(e)
        print(f"Erreur : {e}")
        raise
    finally:
        duration = time.time() - start_time
        log_pipeline("pipeline_seed", "seed_products", rows_inserted, rows_ignored, rows_errors, status, duration, error_message)