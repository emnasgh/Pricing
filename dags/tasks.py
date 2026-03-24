from datetime import datetime, timedelta, timezone
import requests
import time
import json
import os
from common import get_db_connection, unix_to_datetime, log_pipeline

API_URL      = "https://prices.openfoodfacts.org/api/v1/prices"
TMP_PATH     = "/tmp/airflow_data"

def get_last_updated():
    """Retourne MAX(api_updated) de prices_raw, ou None si vide."""
    conn   = get_db_connection()
    cursor = conn.cursor()
    cursor.execute("SELECT MAX(api_updated) FROM prices_raw")
    result = cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0] if result else None


def telecharger_prices(**context):
    start_time    = time.time()
    error_message = None
    status        = "failed"
    try:
        os.makedirs(TMP_PATH, exist_ok=True)
        last_date = get_last_updated()
        if last_date is None:
            raise Exception("prices_raw est vide — lance d'abord le DAG pipeline_seed !")

        last_date_delta = last_date - timedelta(minutes=5)
        print(f"Delta depuis {last_date_delta} → API")

        params    = {"updated__gte": str(last_date_delta), "size": 100, "page": 1}
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
                raise Exception(f"Réponse inattendue : {data}")

            all_items.extend(data["items"])
            print(f"page {params['page']}/{data['pages']} - {len(all_items)} lignes")

            if params["page"] >= data["pages"]:
                break
            params["page"] += 1

        if len(all_items) == 0:
            print("Aucune nouvelle donnée")
            context["ti"].xcom_push(key="source", value="empty")
            status = "success"
            return

        if len(all_items) <= 5000:
            context["ti"].xcom_push(key="source", value="api_xcom")
            context["ti"].xcom_push(key="all_items", value=all_items)
        else:
            tmp_file = f"{TMP_PATH}/all_prices.json"
            with open(tmp_file, "w") as f:
                json.dump(all_items, f)
            context["ti"].xcom_push(key="source", value="api_file")
            context["ti"].xcom_push(key="tmp_file", value=tmp_file)

        print(f"Téléchargé : {len(all_items)} lignes")
        status = "success"

    except Exception as e:
        status        = "failed"
        error_message = str(e)
        raise
    finally:
        duration = time.time() - start_time
        log_pipeline("pipeline_prices", "telecharger_prices", 0, 0, 0, status, duration, error_message)


def inserer_prices_raw(**context):
    start_time    = time.time()
    rows_inserted = 0
    rows_ignored  = 0
    rows_rejected = 0
    error_message = None
    status        = "failed"

    try:
        ti     = context["ti"]
        source = ti.xcom_pull(task_ids="telecharger_prices", key="source")

        if source == "empty":
            print("Aucune donnée à insérer")
            status = "success"
            return

        if source == "api_xcom":
            all_items = ti.xcom_pull(task_ids="telecharger_prices", key="all_items")
        elif source == "api_file":
            tmp_file  = ti.xcom_pull(task_ids="telecharger_prices", key="tmp_file")
            with open(tmp_file, "r") as f:
                all_items = json.load(f)

        print(f"Chargé : {len(all_items)} lignes depuis {source}")

        conn   = get_db_connection()
        cursor = conn.cursor()

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
                conn.rollback()
                print(f"Erreur insertion id={item.get('id')}: {e}")

        conn.commit()
        cursor.close()
        conn.close()
        status = "success"
        print(f"Inséré : {rows_inserted} | Ignoré : {rows_ignored} | Rejeté : {rows_rejected}")

    except Exception as e:
        status        = "failed"
        error_message = str(e)
        raise
    finally:
        duration = time.time() - start_time
        log_pipeline("pipeline_prices", "inserer_prices_raw", rows_inserted, rows_ignored, rows_rejected, status, duration, error_message)
    
