from datetime import datetime, timedelta, timezone
import psycopg2
import requests
import time
import json
import duckdb
import os

DB_CONFIG = {
    "host"    : "host.docker.internal",
    "port"    : 5432,
    "database": "retail_pricing",
    "user"    : "postgres",
}

API_URL      = "https://prices.openfoodfacts.org/api/v1/prices"
target_store = "leclerc"
PARQUET_PATH = "/opt/airflow/data/prices.parquet"
TMP_PATH     = "/tmp/airflow_data"

def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

def get_watermark(dataset_name):
    conn   = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "SELECT last_processed_date FROM watermark WHERE dataset_name=%s",
        (dataset_name,)
    )
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0]


def update_watermark(dataset_name, new_date):
    conn   = get_db_connection()
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE watermark SET last_processed_date=%s WHERE dataset_name=%s",
        (new_date, dataset_name)
    )
    conn.commit()
    cursor.close()
    conn.close()
    print(f"Watermark mis à jour : {dataset_name} → {new_date}")


def unix_to_datetime(ts):
    """Convertit un timestamp Unix (secondes ou millisecondes) en datetime UTC."""
    if ts is None:
        return None
    try:
        ts = int(ts)
        # si > 1e10 → millisecondes, sinon secondes
        if ts > 10_000_000_000:
            ts = ts / 1000
        return datetime.fromtimestamp(ts, tz=timezone.utc)
    except Exception:
        return None


def telecharger_prices(**context):
    os.makedirs(TMP_PATH, exist_ok=True)
    last_date = get_watermark('prices')

    # ── 1ère exécution → lire parquet local ──
    if str(last_date)[:10] == "2010-01-01":
        print("1ère exécution → chargement depuis parquet local")
        conn = duckdb.connect()
        df   = conn.execute(f"SELECT * FROM read_parquet('{PARQUET_PATH}')").df()
        conn.close()
        print(f"Parquet chargé : {len(df)} lignes")

        tmp_file = f"{TMP_PATH}/all_prices.json"
        df.to_json(tmp_file, orient="records")
        print(f"Sauvegardé dans {tmp_file}")

        context["ti"].xcom_push(key="source", value="parquet")
        context["ti"].xcom_push(key="tmp_file", value=tmp_file)

    # ── Exécutions suivantes → API delta ──
    else:
        last_date_delta = last_date - timedelta(minutes=5)
        print(f"Delta depuis {last_date_delta} → API")

        params    = {
            "updated__gte": str(last_date_delta),
            "size"        : 100,
            "page"        : 1
        }
        all_items = []

        while True:
            data = None
            for tentative in range(3):
                try:
                    response = requests.get(API_URL, params=params, timeout=30)
                    data     = response.json()
                    break
                except Exception as e:
                    print(f"Erreur page {params['page']} tentative {tentative+1}: {e}")
                    time.sleep(5)

            if data is None:
                raise Exception(f"Page {params['page']} inaccessible après 3 tentatives")

            if "items" not in data:
                raise Exception(f"Réponse inattendue page {params['page']} : {data}")

            all_items.extend(data["items"])
            print(f"page {params['page']}/{data['pages']} - {len(all_items)} lignes")

            if params["page"] >= data["pages"]:
                break
            params["page"] += 1

        if len(all_items) == 0:
            print("Aucune nouvelle donnée")
            context["ti"].xcom_push(key="source", value="empty")
            return

        print(f"Téléchargé : {len(all_items)} lignes")

        if len(all_items) <= 5000:
            context["ti"].xcom_push(key="source", value="api_xcom")
            context["ti"].xcom_push(key="all_items", value=all_items)
            print("Données passées via XCom")
        else:
            tmp_file = f"{TMP_PATH}/all_prices.json"
            with open(tmp_file, "w") as f:
                json.dump(all_items, f)
            context["ti"].xcom_push(key="source", value="api_file")
            context["ti"].xcom_push(key="tmp_file", value=tmp_file)
            print(f"Données sauvegardées dans {tmp_file}")


def inserer_prices_raw(**context):
    ti     = context["ti"]
    source = ti.xcom_pull(task_ids="telecharger_prices", key="source")

    if source == "empty":
        print("Aucune donnée à insérer")
        return

    if source == "api_xcom":
        all_items = ti.xcom_pull(task_ids="telecharger_prices", key="all_items")
    elif source in ("parquet", "api_file"):
        tmp_file  = ti.xcom_pull(task_ids="telecharger_prices", key="tmp_file")
        with open(tmp_file, "r") as f:
            all_items = json.load(f)

    print(f"Chargé : {len(all_items)} lignes depuis {source}")

    conn   = get_db_connection()
    cursor = conn.cursor()

    rows_inserted = 0
    rows_ignored  = 0
    rows_rejected = 0

    for item in all_items:
        try:
            api_updated = unix_to_datetime(item.get("updated"))

            cursor.execute("""
                INSERT INTO prices_raw (id, raw_data, api_updated, inserted_at)
                VALUES (%s, %s, %s, NOW())
                ON CONFLICT (id, api_updated) DO NOTHING
            """, (item["id"], json.dumps(item), api_updated))

            if cursor.rowcount == 1:
                rows_inserted += 1
            else:
                rows_ignored += 1

        except Exception as e:
            rows_rejected += 1
            conn.rollback()  # reset la transaction pour ne pas bloquer les lignes suivantes
            print(f"Erreur insertion id={item.get('id')}: {e}")

    conn.commit()

    # watermark → max updated converti en datetime
    valid_dates = [unix_to_datetime(item.get("updated")) for item in all_items]
    valid_dates = [d for d in valid_dates if d is not None]
    if valid_dates:
        max_date = max(valid_dates)
        update_watermark("prices", max_date)

    print(f"Inséré : {rows_inserted} | Ignoré : {rows_ignored} | Rejeté : {rows_rejected}")

    cursor.close()
    conn.close()