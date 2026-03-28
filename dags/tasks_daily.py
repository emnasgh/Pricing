from datetime import timedelta, datetime
import requests
import time
import json
import os
import tempfile
from airflow.models import Variable
from common import get_db_connection, unix_to_datetime, log_pipeline

INSERT_EVERY = 20   # INSERT tous les 20 pages = 2 000 lignes max en RAM



def get_last_updated(table_name: str):
    """
    Watermark de reprise = MAX(api_updated) des lignes déjà insérées.
    Exclut la sentinelle epoch (1970-01-01).
    """
    conn   = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(f"""
        SELECT MAX(api_updated)
        FROM {table_name}
        WHERE api_updated > '1970-01-01'
    """)
    last_date = cursor.fetchone()[0]
    cursor.close()
    conn.close()
    return last_date

# Insert batch via COPY + table temporaire

def copy_batch_to_db(cursor, conn, batch):
    """
    Insert un batch via COPY + table temporaire ( low RAM).
    - ON COMMIT DELETE ROWS → table survit entre les batchs
    - Fichier tmp supprimé après chaque batch
    - rows_ignored ne peut pas être négatif
    """
    if not batch:
        return 0, 0, 0

    rows_inserted = 0
    rows_rejected = 0
    tmp_path = None

    try:
        with tempfile.NamedTemporaryFile(mode="w", suffix=".tsv", delete=False) as tmp:
            tmp_path = tmp.name
            for item in batch:
                if not item.get("id"):
                    rows_rejected += 1
                    continue
                try:
                    api_updated = unix_to_datetime(item.get("updated"))
                    if api_updated is None:
                        api_updated = datetime(1970, 1, 1)

                    raw_json = (
                        json.dumps(item, ensure_ascii=False)
                            .replace("\\", "\\\\")
                            .replace("\n", "\\n")
                            .replace("\r", "\\r")
                            .replace("\t", "\\t")
                    )
                    tmp.write(f"{item['id']}\t{raw_json}\t{api_updated}\n")

                except Exception as e:
                    rows_rejected += 1
                    print(f"  Sérialisation échouée id={item.get('id')}: {e}")

        cursor.execute("""
            CREATE TEMP TABLE IF NOT EXISTS tmp_prices (
                id          TEXT,
                raw_data    TEXT,
                api_updated TIMESTAMP
            ) ON COMMIT DELETE ROWS
        """)
        cursor.execute("TRUNCATE tmp_prices")

        with open(tmp_path, "r") as f:
            cursor.copy_from(f, "tmp_prices", sep="\t",
                             columns=("id", "raw_data", "api_updated"))

        cursor.execute("""
            INSERT INTO prices_raw (id, raw_data, api_updated, inserted_at)
            SELECT id, raw_data, api_updated, NOW()
            FROM tmp_prices
            ON CONFLICT (id, api_updated) DO NOTHING
        """)

        rows_inserted = cursor.rowcount
        conn.commit()

    finally:
        if tmp_path and os.path.exists(tmp_path):
            os.unlink(tmp_path)

    rows_ignored = max(len(batch) - rows_inserted - rows_rejected, 0)
    return rows_inserted, rows_ignored, rows_rejected


# Fetch une page avec retry exponentiel

def fetch_page(params: dict, max_retries: int = 3):
    for attempt in range(max_retries):
        try:
            response = requests.get(
                "https://prices.openfoodfacts.org/api/v1/prices",
                params=params,
                timeout=30
            )
            response.raise_for_status()
            return response.json()
        except Exception as e:
            wait = 5 * (attempt + 1)
            print(f"  Retry {attempt + 1}/{max_retries} (attente {wait}s): {e}")
            time.sleep(wait)
    return None


def fetch_and_insert_prices(**context):
    start_time    = time.time()
    rows_inserted = 0       
    rows_ignored  = 0
    rows_rejected = 0
    status        = "failed"
    error_message = None
    conn          = None

    try:
        last_date = get_last_updated("prices_raw")
        if last_date is None:
            raise Exception("prices_raw est vide — lancer seed !")

        last_date_delta = last_date - timedelta(minutes=5)
        print(f"Fetch depuis {last_date_delta}")

        # Reprendre depuis la dernière page sauvegardée 
        resume_page = int(Variable.get("prices_resume_page", default_var=1))
        print(f"Reprise depuis la page {resume_page}")

        params = {
            "updated__gte": str(last_date_delta),
            "size"        : 100,
            "page"        : resume_page,
        }

        total_pages        = None
        batch              = []
        pages_since_insert = 0

        conn   = get_db_connection()
        cursor = conn.cursor()

        while True:
            data = fetch_page(params)

            if not data or "items" not in data:
                raise Exception(f"Réponse API invalide: {data}")

            if total_pages is None:
                total_pages = data["pages"]
                print(f"Total pages : {total_pages} | Reprise page {resume_page}")

            batch.extend(data["items"])
            pages_since_insert += 1

            current_page = params["page"]
            is_last_page = current_page >= total_pages

            print(f"page {current_page}/{total_pages} | batch={len(batch)}")

            if pages_since_insert >= INSERT_EVERY or is_last_page:
                ins, ign, rej  = copy_batch_to_db(cursor, conn, batch)
                rows_inserted += ins
                rows_ignored  += ign
                rows_rejected += rej
                print(f"  ✓ commit | +{ins} insérés | {ign} ignorés | {rej} rejetés")

                batch              = []
                pages_since_insert = 0

                # Sauvegarder la progression
                Variable.set("prices_resume_page", current_page + 1)

            if is_last_page:
                # Reset pour le prochain run quotidien
                Variable.set("prices_resume_page", 1)
                break

            params["page"] += 1

        cursor.close()
        conn.close()

        print(
            f"FINAL → Inséré: {rows_inserted} | "
            f"Ignoré: {rows_ignored} | Rejeté: {rows_rejected}"
        )
        status = "success"

    except Exception as e:
        error_message = str(e)
        if conn:
            try:
                conn.rollback()
            except Exception:
                pass
        raise

    finally:
        duration = time.time() - start_time
        log_pipeline(
            "pipeline_daily", "fetch_and_insert_prices",
            rows_inserted, rows_ignored, rows_rejected,
            status, duration, error_message
        )

# Coverage stat

import time
import json
from common import get_db_connection, log_pipeline

def safe_extract_code(text):
    """
    Extraction sécurisée du champ 'code' depuis raw_data.
    Retourne None si le JSON est invalide ou la clé n'existe pas.
    """
    try:
        data = json.loads(text)
        return data.get("code")
    except (json.JSONDecodeError, TypeError, AttributeError):
        return None

def safe_extract_product_code(text):
    """
    Extraction sécurisée du champ 'product_code' depuis raw_data.
    Retourne None si le JSON est invalide ou la clé n'existe pas.
    """
    try:
        data = json.loads(text)
        return data.get("product_code")
    except (json.JSONDecodeError, TypeError, AttributeError):
        return None

def compute_coverage_stats(**context):
    """
    Calcul des statistiques de couverture entre prices_raw et products_raw.
    - total_prices    : nombre total de prix
    - with_product    : prix liés à un produit connu
    - without_product : prix sans produit connu
    - coverage_pct    : pourcentage de couverture
    """

    start_time = time.time()
    status = "failed"
    error_message = None
    conn = None
    cursor = None

    try:
        conn = get_db_connection()
        cursor = conn.cursor()

        #  Créer table de stats si elle n'existe pas
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS pipeline_coverage_stats (
                id              SERIAL PRIMARY KEY,
                run_date        DATE         NOT NULL,
                total_prices    BIGINT       NOT NULL,
                with_product    BIGINT       NOT NULL,
                without_product BIGINT       NOT NULL,
                coverage_pct    NUMERIC(5,2) NOT NULL,
                computed_at     TIMESTAMP    NOT NULL DEFAULT NOW()
            )
        """)

        # Récupérer raw_data de products_raw et prices_raw
        cursor.execute("SELECT raw_data FROM products_raw")
        products_rows = cursor.fetchall()
        cursor.execute("SELECT raw_data FROM prices_raw")
        prices_rows = cursor.fetchall()

        #  Construire set de codes connus
        known_codes = set()
        for row in products_rows:
            code = safe_extract_code(row[0])
            if code:
                known_codes.add(code)

        # Calculer stats côté Python
        total_prices = 0
        with_product = 0
        without_product = 0

        for row in prices_rows:
            total_prices += 1
            product_code = safe_extract_product_code(row[0])
            if product_code and product_code in known_codes:
                with_product += 1
            else:
                without_product += 1

        coverage_pct = round((with_product / total_prices * 100), 2) if total_prices > 0 else 0.0

        print(
            f"Coverage stats → total={total_prices} | "
            f"avec produit={with_product} | "
            f"sans produit={without_product} | "
            f"couverture={coverage_pct}%"
        )

        # Insérer stats dans la table
        cursor.execute("""
            INSERT INTO pipeline_coverage_stats
                (run_date, total_prices, with_product, without_product, coverage_pct, computed_at)
            VALUES (CURRENT_DATE, %s, %s, %s, %s, NOW())
        """, (total_prices, with_product, without_product, coverage_pct))

        conn.commit()
        status = "success"

    except Exception as e:
        error_message = str(e)
        raise

    finally:
        # Logging Airflow
        duration = time.time() - start_time
        log_pipeline(
            "pipeline_daily",
            "compute_coverage_stats",
            total_prices if 'total_prices' in locals() else 0,
            with_product if 'with_product' in locals() else 0,
            without_product if 'without_product' in locals() else 0,
            status,
            duration,
            error_message
        )

        # Fermeture connexion
        if cursor and not cursor.closed:
            cursor.close()
        if conn and not conn.closed:
            conn.close()