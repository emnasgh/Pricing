import time
import io
import os
import json
import math
import gzip
import requests
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from datetime import datetime, timezone
from common import get_db_connection, log_pipeline

#chemins locaux
DATA_PATH           = "/opt/airflow/data"
PARQUET_PATH_prices = f"{DATA_PATH}/prices.parquet"
PARQUET_PATH_food   = f"{DATA_PATH}/food.parquet"
PARQUET_PATH_beauty = f"{DATA_PATH}/beauty.parquet"
JSONL_PATH_other    = f"{DATA_PATH}/openproductsfacts-products.jsonl"

# urls de téléchargement (data.gouv.fr)
URL_PRICES = "https://www.data.gouv.fr/api/1/datasets/r/49716ed5-aacf-4692-8b2d-3cc6d15bf1d1"
URL_FOOD   = "https://www.data.gouv.fr/api/1/datasets/r/05b27bd4-7294-4753-9228-fe035a35b110"
URL_BEAUTY = "https://www.data.gouv.fr/api/1/datasets/r/5ee7a3da-4ac9-45a5-9175-7d7f71349275"
URL_OTHER  = "https://www.data.gouv.fr/api/1/datasets/r/53104bea-7cd5-4dbe-b421-54b7a56c8dba"

CHUNK = 20000


# ─── Encodeur JSON ────────────────────────────────────────────────────────────
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
            return None if pd.isna(obj) else obj.isoformat()
        if isinstance(obj, pd.NaT.__class__):
            return None
        if hasattr(obj, 'isoformat'):          # date, datetime
            return obj.isoformat()
        if hasattr(obj, 'item'):               # numpy scalar générique
            return obj.item()
        try:
            import decimal
            if isinstance(obj, decimal.Decimal):
                return float(obj)
        except ImportError:
            pass
        if isinstance(obj, bytes):
            return obj.decode("utf-8", errors="replace")
        if isinstance(obj, (list, tuple)):
            return list(obj)
        # Dernier recours : convertir en string plutôt que planter
        try:
            return str(obj)
        except Exception:
            return None


def to_json_safe(item):
    for key in list(item.keys()):
        val = item[key]
        if isinstance(val, str) and (val.startswith('[{') or val.startswith('{"')):
            try:
                item[key] = json.loads(val)
            except Exception:
                item[key] = None
    return json.dumps(item, cls=NumpyEncoder).replace(': NaN', ': null').replace(':NaN', ':null')


# ─── Téléchargement ───────────────────────────────────────────────────────────
def telecharger_fichier(url, dest_path, source):
    # Supprimer si existe déjà pour avoir données fraîches
    if os.path.exists(dest_path):
        os.remove(dest_path)
        print(f"{source} supprimé → retéléchargement")

    print(f"Téléchargement {source} depuis {url}...")
    os.makedirs(os.path.dirname(dest_path), exist_ok=True)

    headers = {"User-Agent": "Mozilla/5.0 (compatible; DataPipeline/1.0)"}
    response = requests.get(url, stream=True, timeout=600, headers=headers)
    response.raise_for_status()

    total = 0
    with open(dest_path, "wb") as f:
        for chunk in response.iter_content(chunk_size=1024 * 1024):
            f.write(chunk)
            total += len(chunk)
            print(f"  {source} : {round(total / 1024 / 1024, 1)} MB téléchargés", end="\r")

    print(f"\n{source} téléchargé → {dest_path} ({round(total / 1024 / 1024, 1)} MB)")

    # Décompresser si gz
    if dest_path.endswith(".gz"):
        dest_unzip = dest_path[:-3]
        print(f"Décompression {source}...")
        with gzip.open(dest_path, "rb") as f_in:
            with open(dest_unzip, "wb") as f_out:
                f_out.write(f_in.read())
        os.remove(dest_path)
        print(f"{source} décompressé → {dest_unzip}")


def telecharger_fichiers(**context):
    telecharger_fichier(URL_PRICES, PARQUET_PATH_prices,       "prices.parquet")
    telecharger_fichier(URL_FOOD,   PARQUET_PATH_food,         "food.parquet")
    telecharger_fichier(URL_BEAUTY, PARQUET_PATH_beauty,       "beauty.tsv")
    telecharger_fichier(URL_OTHER,  JSONL_PATH_other + ".gz",  "other.jsonl")
    print("Tous les fichiers sont téléchargés !")


# ─── Insertion via COPY ───────────────────────────────────────────────────────
def get_table_info(table, conn):
    """Retourne (colonnes, raw_data_is_jsonb)."""
    cursor = conn.cursor()
    cursor.execute("""
        SELECT column_name, data_type FROM information_schema.columns
        WHERE table_name = %s AND table_schema = 'public'
        ORDER BY ordinal_position
    """, (table,))
    rows = cursor.fetchall()
    cursor.close()
    cols = [r[0] for r in rows]
    raw_data_type = next((r[1] for r in rows if r[0] == "raw_data"), "text")
    return cols, raw_data_type.lower() == "jsonb"


def inserer_via_copy(rows, table, conn):
    if not rows:
        return 0

    # Détecte les colonnes réelles + type de raw_data
    real_cols, raw_is_jsonb = get_table_info(table, conn)
    has_api_updated = "api_updated" in real_cols

    cursor = conn.cursor()

    # Table temporaire (id en VARCHAR pour COPY)
    tmp_cols = "id VARCHAR, raw_data TEXT"
    if has_api_updated:
        tmp_cols += ", api_updated TIMESTAMPTZ"
    tmp_cols += ", inserted_at TIMESTAMPTZ"

    cursor.execute(f"""
        CREATE TEMP TABLE tmp_load ({tmp_cols}) ON COMMIT DROP;
    """)

    # Préparation buffer COPY
    buf = io.StringIO()
    for row in rows:
        id_val, raw_data, api_updated = row

        raw = str(raw_data)\
            .replace("\\", "\\\\")\
            .replace("\t", " ")\
            .replace("\n", " ")\
            .replace("\r", " ")

        if has_api_updated:
            ts = api_updated.strftime("%Y-%m-%d %H:%M:%S%z") if api_updated else ""
            buf.write(f"{id_val}\t{raw}\t{ts}\t\\N\n")
        else:
            buf.write(f"{id_val}\t{raw}\t\\N\n")

    buf.seek(0)

    # Colonnes COPY
    if has_api_updated:
        copy_cols = "id, raw_data, api_updated, inserted_at"
    else:
        copy_cols = "id, raw_data, inserted_at"

    cursor.copy_expert(f"""
        COPY tmp_load ({copy_cols})
        FROM STDIN
        WITH (FORMAT text, NULL '\\N', DELIMITER E'\t')
    """, buf)

    # Cast JSON si besoin
    raw_cast = "raw_data::jsonb" if raw_is_jsonb else "raw_data"

    
    if has_api_updated:
        insert_cols = "id, raw_data, api_updated, inserted_at"
        select_clause = f"""
            SELECT DISTINCT ON (id::varchar, COALESCE(api_updated, '1970-01-01'::TIMESTAMPTZ))
                id::varchar,
                {raw_cast},
                api_updated,
                NOW()
        """
        conflict = "ON CONFLICT (id, api_updated) DO NOTHING"
    else:
        insert_cols = "id, raw_data, inserted_at"
        select_clause = f"""
            SELECT DISTINCT ON (id::varchar)
                id::varchar,
                {raw_cast},
                NOW()
        """
        conflict = "ON CONFLICT (id) DO NOTHING"

    # Insertion finale
    cursor.execute(f"""
        INSERT INTO {table} ({insert_cols})
        {select_clause}
        FROM tmp_load
        {conflict}
    """)

    inserted = cursor.rowcount
    conn.commit()
    cursor.close()
    return inserted

# ─── Chargement parquet prices ────────────────────────────────────────────────
def charger_parquet_prices(parquet_path, table, conn, source):
    import pyarrow as pa
    import pyarrow.parquet as pq2

    pf        = pq2.ParquetFile(parquet_path)
    inserted  = 0
    chunk_num = 0

    for batch in pf.iter_batches(batch_size=CHUNK):
        chunk_num += 1

        if chunk_num == 1:
            print(f"{source} colonnes disponibles : {batch.schema.names}")
            id_type = batch.schema.field("id").type if "id" in batch.schema.names else "absent"
            print(f"{source} dtypes id : {id_type}")

        if "id" not in batch.schema.names:
            print(f"{source} chunk {chunk_num} : colonne 'id' absente")
            continue

        # ── Extraire id et api_updated directement depuis pyarrow (pas de pandas) ──
        id_col = batch.column("id")

        # Trouver colonne date
        date_col_name = next((c for c in ["updated", "date", "created"] if c in batch.schema.names), None)
        if chunk_num == 1:
            print(f"{source} colonne date utilisee : '{date_col_name}'")

        # Convertir le batch entier en JSON avec pyarrow (vectorisé natif)
        # On passe par pandas uniquement pour to_json qui est implémenté en C
        df = batch.to_pandas()

        # Extraire api_updated avant sérialisation
        if date_col_name:
            api_updated_series = pd.to_datetime(df[date_col_name], utc=True, errors="coerce")
        else:
            api_updated_series = pd.Series([pd.NaT] * len(df), index=df.index)

        # Extraire ids
        ids = df["id"].tolist()

        # Sérialisation JSON vectorisée : to_json orient=records en C, puis split par ligne
        # Exclure la colonne date pour éviter doublon dans raw_data (optionnel, on garde tout)
        try:
            json_lines = df.to_json(orient="records", lines=True, date_format="iso", default_handler=str)
            json_list  = json_lines.strip().split("\n")
        except Exception as e:
            print(f"{source} chunk {chunk_num} ERREUR to_json : {e}")
            continue

        if len(json_list) != len(ids):
            print(f"{source} chunk {chunk_num} longueur mismatch {len(json_list)} vs {len(ids)}")
            continue

        rows    = []
        skipped = 0
        for id_val, json_str, ts in zip(ids, json_list, api_updated_series):
            if pd.isna(id_val) if isinstance(id_val, float) else (id_val is None):
                skipped += 1
                continue
            id_str = str(int(id_val)) if isinstance(id_val, (int, float)) else str(id_val).split(".")[0]
            if not id_str or id_str in ("nan", "None", ""):
                skipped += 1
                continue
            try:
                api_updated = None if pd.isna(ts) else ts.to_pydatetime()
            except (TypeError, ValueError):
                api_updated = None
            rows.append((id_str, json_str, api_updated))

        if skipped:
            print(f"{source} chunk {chunk_num} : {skipped} lignes ignorées (id invalide)")

        n         = inserer_via_copy(rows, table, conn)
        inserted += n
    return inserted


# ─── Chargement parquet products ─────────────────────────────────────────────
def charger_parquet_products(parquet_path, id_field, table, conn, source):
    pf        = pq.ParquetFile(parquet_path)
    inserted  = 0
    chunk_num = 0

    for batch in pf.iter_batches(batch_size=CHUNK):
        chunk_num += 1
        df = batch.to_pandas()

        if id_field not in df.columns:
            continue

        df[id_field] = df[id_field].astype(str).str.strip()
        df = df[df[id_field].notna() & (df[id_field] != '') & (df[id_field] != 'nan')].copy()

        if df.empty:
            continue

        # api_updated depuis last_modified_t (unix timestamp)
        if "last_modified_t" in df.columns:
            api_updated_series = pd.to_datetime(df["last_modified_t"], unit="s", utc=True, errors="coerce")
        else:
            api_updated_series = pd.Series([pd.NaT] * len(df), index=df.index)

        # Sérialisation JSON vectorisée en C via to_json
        try:
            json_lines = df.to_json(orient="records", lines=True, date_format="iso", default_handler=str)
            json_list  = json_lines.strip().split("\n")
        except Exception as e:
            print(f"{source} chunk {chunk_num} ERREUR to_json : {e}")
            continue

        ids = df[id_field].tolist()
        if len(json_list) != len(ids):
            print(f"{source} chunk {chunk_num} longueur mismatch {len(json_list)} vs {len(ids)}")
            continue

        rows = []
        for id_str, json_str, ts in zip(ids, json_list, api_updated_series):
            try:
                api_updated = None if pd.isna(ts) else ts.to_pydatetime()
            except (TypeError, ValueError):
                api_updated = None
            rows.append((id_str, json_str, api_updated))

        n         = inserer_via_copy(rows, table, conn)
        inserted += n
        print(f"{source} chunk {chunk_num} ({len(rows)} lignes) | +{n} | total : {inserted}")

    return inserted


# ─── Chargement JSONL ─────────────────────────────────────────────────────────
def charger_jsonl(jsonl_path, table, conn, source):
    inserted  = 0
    chunk_num = 0
    batch     = []

    try:
        conn.rollback()
    except Exception:
        pass

    if not os.path.exists(jsonl_path):
        raise FileNotFoundError(f"Fichier introuvable : {jsonl_path}")

    print(f"Lecture {jsonl_path}...")

    with open(jsonl_path, "rt", encoding="utf-8") as f:
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


# ─── SEED PRICES ──────────────────────────────────────────────────────────────
def seed_prices(**context):
    start_time    = time.time()
    rows_inserted = 0
    rows_ignored  = 0
    rows_errors   = 0
    error_message = None
    status        = "failed"

    try:
        print("Seed prices depuis parquet...")
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


# ─── SEED PRODUCTS ────────────────────────────────────────────────────────────
def seed_products(**context):
    start_time    = time.time()
    rows_inserted = 0
    rows_ignored  = 0
    rows_errors   = 0
    error_message = None
    status        = "failed"

    try:
        conn = get_db_connection()

        print("Chargement food...")
        n              = charger_parquet_products(
            PARQUET_PATH_food, "code", "products_raw", conn, "Food"
        )
        rows_inserted += n
        print(f"Food terminé : {n} lignes insérées")

        print("Chargement beauty...")
        n              = charger_parquet_products(
            PARQUET_PATH_beauty, "code", "products_raw", conn, "Beauty"
        )
        rows_inserted += n
        print(f"Beauty terminé : {n} lignes insérées")

        print("Chargement JSONL other...")
        n              = charger_jsonl(JSONL_PATH_other, "products_raw", conn, "Other")
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