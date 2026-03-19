import time
import json
import math
import duckdb
import os
from common import get_db_connection, unix_to_datetime, log_pipeline

PARQUET_PATH_prices = "/opt/airflow/data/prices.parquet"
PARQUET_PATH_food   = "/opt/airflow/data/food.parquet"
PARQUET_PATH_beauty = "/opt/airflow/data/beauty.parquet"
JSONL_PATH_other    = "/opt/airflow/data/openproductsfacts-products.jsonl"

BATCH_SIZE = 5000


def clean_nan(obj):
    if isinstance(obj, float) and math.isnan(obj):
        return None
    elif isinstance(obj, dict):
        return {k: clean_nan(v) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [clean_nan(v) for v in obj]
    return obj


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

        print("Seed initial : chargement parquet prices")
        duck = duckdb.connect()
        df   = duck.execute(f"SELECT * FROM read_parquet('{PARQUET_PATH_prices}')").df()
        duck.close()
        print(f"Parquet chargé : {len(df)} lignes")

        conn   = get_db_connection()
        cursor = conn.cursor()

        for item in df.to_dict(orient="records"):
            try:
                api_updated = unix_to_datetime(item.get("updated"))
                cursor.execute("""
                    INSERT INTO prices_raw (id, raw_data, api_updated, inserted_at)
                    VALUES (%s, %s, %s, NOW())
                    ON CONFLICT (id, api_updated) DO NOTHING
                """, (item["id"], json.dumps(clean_nan(item)), api_updated))

                if cursor.rowcount == 1:
                    rows_inserted += 1
                else:
                    rows_ignored += 1

            except Exception as e:
                rows_errors += 1
                conn.rollback()
                print(f"Erreur id={item.get('id')}: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        status = "success"
        print(f"Seed terminé : Inséré: {rows_inserted} | Ignoré: {rows_ignored} | Erreurs: {rows_errors}")

    except Exception as e:
        status        = "failed"
        error_message = str(e)
        raise
    finally:
        duration = time.time() - start_time
        log_pipeline("pipeline_seed", "seed_prices", rows_inserted, rows_ignored, rows_errors, status, duration, error_message)


def inserer_batch(items, rows_inserted, rows_ignored, rows_errors):
    conn   = get_db_connection()
    cursor = conn.cursor()

    # étape 1 — préparer toutes les valeurs du batch
    valeurs = []
    for item in items:
        try:
            api_updated = unix_to_datetime(item.get("last_modified_t"))
            id_val = str(item.get("code", "")).strip()
            if not id_val:
                rows_errors += 1
                continue
            valeurs.append((id_val, json.dumps(clean_nan(item)), api_updated))
        except Exception as e:
            rows_errors += 1
            print(f"Erreur préparation code={item.get('code')}: {e}")

    if not valeurs:
        conn.close()
        return rows_inserted, rows_ignored, rows_errors

    # étape 2 — insérer tout le batch en une seule requête
    try:
        cursor.executemany("""
            INSERT INTO products_raw (id, raw_data, api_updated, inserted_at)
            VALUES (%s, %s, %s, NOW())
            ON CONFLICT (id, api_updated) DO NOTHING
        """, valeurs)

        rows_inserted += cursor.rowcount
        rows_ignored  += len(valeurs) - cursor.rowcount
        conn.commit()

    except Exception as e:
        rows_errors += len(valeurs)
        conn.rollback()
        print(f"Erreur batch complet : {e}")

    cursor.close()
    conn.close()
    return rows_inserted, rows_ignored, rows_errors


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

        duck = duckdb.connect()

        total_food = duck.execute(
            f"SELECT COUNT(*) FROM read_parquet('{PARQUET_PATH_food}')"
        ).fetchone()[0]
        total_beauty = duck.execute(
            f"SELECT COUNT(*) FROM read_parquet('{PARQUET_PATH_beauty}')"
        ).fetchone()[0]
        total_parquet = total_food + total_beauty

        print(f"Food total : {total_food} | Beauty total : {total_beauty}")
        print(f"Lignes déjà en base : {count}")

        if count >= total_parquet:
            print(f"Déjà seedé ({count} lignes) → skip")
            status = "success"
            duck.close()
            return

        rows_already = count
        offset_food  = min((rows_already // BATCH_SIZE) * BATCH_SIZE, total_food)
        print(f"Reprise depuis {rows_already} lignes | Food offset : {offset_food}")

        # ── FOOD par batch ──
        print("Chargement food.parquet par batch...")
        offset = offset_food
        while offset < total_food:
            df_batch = duck.execute(f"""
                SELECT * FROM read_parquet('{PARQUET_PATH_food}')
                LIMIT {BATCH_SIZE} OFFSET {offset}
            """).df()
            if len(df_batch) == 0:
                break
            items = df_batch.to_dict(orient="records")
            del df_batch
            rows_inserted, rows_ignored, rows_errors = inserer_batch(
                items, rows_inserted, rows_ignored, rows_errors
            )
            print(f"Food batch {offset}→{offset+len(items)} | Inséré: {rows_inserted}")
            offset += BATCH_SIZE

        # ── BEAUTY par batch ──
        rows_after_food = rows_already - total_food
        offset_beauty   = max(0, (rows_after_food // BATCH_SIZE) * BATCH_SIZE) if rows_already > total_food else 0
        print(f"Chargement beauty.parquet par batch... | Beauty offset : {offset_beauty}")

        offset = offset_beauty
        while offset < total_beauty:
            df_batch = duck.execute(f"""
                SELECT * FROM read_parquet('{PARQUET_PATH_beauty}')
                LIMIT {BATCH_SIZE} OFFSET {offset}
            """).df()
            if len(df_batch) == 0:
                break
            items = df_batch.to_dict(orient="records")
            del df_batch
            rows_inserted, rows_ignored, rows_errors = inserer_batch(
                items, rows_inserted, rows_ignored, rows_errors
            )
            print(f"Beauty batch {offset}→{offset+len(items)} | Inséré: {rows_inserted}")
            offset += BATCH_SIZE

        duck.close()

        # ── OTHER (JSONL) par batch ──
        print("Chargement JSONL other par batch...")
        batch = []
        with open(JSONL_PATH_other, "r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line:
                    continue
                try:
                    batch.append(json.loads(line))
                except Exception:
                    continue

                if len(batch) >= BATCH_SIZE:
                    rows_inserted, rows_ignored, rows_errors = inserer_batch(
                        batch, rows_inserted, rows_ignored, rows_errors
                    )
                    print(f"Other batch inséré | Total inséré: {rows_inserted}")
                    batch = []

            if batch:
                rows_inserted, rows_ignored, rows_errors = inserer_batch(
                    batch, rows_inserted, rows_ignored, rows_errors
                )

        status = "success"
        print(f"Seed terminé : Inséré: {rows_inserted} | Ignoré: {rows_ignored} | Erreurs: {rows_errors}")

    except Exception as e:
        status        = "failed"
        error_message = str(e)
        raise
    finally:
        duration = time.time() - start_time
        log_pipeline("pipeline_seed", "seed_products", rows_inserted, rows_ignored, rows_errors, status, duration, error_message)
