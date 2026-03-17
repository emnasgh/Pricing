from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests
import time
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

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


def get_spark():
    return SparkSession.builder \
        .appName("dag_prices_daily") \
        .config("spark.driver.memory", "2g") \
        .getOrCreate()


def get_watermark(dataset_name):
    conn   = psycopg2.connect(**DB_CONFIG)
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
    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE watermark SET last_processed_date=%s WHERE dataset_name=%s",
        (new_date, dataset_name)
    )
    conn.commit()
    cursor.close()
    conn.close()


def aplatir_item(item):
    loc  = item.get("location") or {}
    prod = item.get("product")  or {}
    return {
        "id"                          : str(item.get("id") or ""),
        "product_code"                : str(item.get("product_code") or prod.get("code") or ""),
        "product_name"                : str(item.get("product_name") or prod.get("product_name") or ""),
        "category_tag"                : str(item.get("category_tag") or ""),
        "price"                       : item.get("price"),
        "currency"                    : str(item.get("currency") or ""),
        "date"                        : str(item.get("date") or ""),
        "created"                     : str(item.get("created") or ""),
        "price_is_discounted"         : item.get("price_is_discounted"),
        "price_without_discount"      : item.get("price_without_discount"),
        "type"                        : str(item.get("type") or ""),
        "location_osm_id"             : str(item.get("location_osm_id") or loc.get("osm_id") or ""),
        "location_osm_display_name"   : str(loc.get("osm_name") or loc.get("osm_display_name") or ""),
        "location_osm_address_city"   : str(loc.get("osm_address_city") or ""),
        "location_osm_address_country": str(loc.get("osm_address_country") or ""),
        "location_osm_lat"            : loc.get("osm_lat"),
        "location_osm_lon"            : loc.get("osm_lon"),
    }


def telecharger_prices():
    last_date = min(get_watermark('prices'), get_watermark('competitors'))

    # 1ère exécution → lire parquet local avec Spark
    if str(last_date)[:10] == "2010-01-01":
        print("1ère exécution → chargement depuis parquet local")
        spark = get_spark()
        df = spark.read.parquet(PARQUET_PATH)
        print(f"Parquet chargé : {df.count()} lignes")
        df.write.mode("overwrite").parquet(f"{TMP_PATH}/all_prices.parquet")
        print(f"Sauvegardé : {df.count()} lignes")
        spark.stop()

    # Exécutions suivantes → API delta
    else:
        print(f"Delta depuis {last_date} → API")
        params = {
            "page"        : 1,
            "created__gte": str(last_date),
            "size"        : 100
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
                raise Exception(f"Page {params['page']} inaccessible après 3 tentatives → tâche échouée")

            if "items" not in data:
                raise Exception(f"Réponse inattendue page {params['page']} : {data}")

            all_items.extend(data["items"])
            print(f"page {params['page']}/{data['pages']} - {len(all_items)} lignes")

            if params["page"] >= data["pages"]:
                break

            params["page"] += 1

        if len(all_items) == 0:
            print("Aucune nouvelle donnée : watermark non mis à jour")
            return

        # aplatir + convertir en Spark DataFrame
        flat_items = [aplatir_item(item) for item in all_items]
        spark = get_spark()
        df = spark.createDataFrame(flat_items)
        df.write.mode("overwrite").parquet(f"{TMP_PATH}/all_prices.parquet")
        print(f"Sauvegardé : {df.count()} lignes")
        spark.stop()


def separer_prices():
    spark = get_spark()
    df = spark.read.parquet(f"{TMP_PATH}/all_prices.parquet")
    print(f"Chargé : {df.count()} lignes")

    df = df.withColumn(
        "location_osm_display_name",
        F.lower(F.col("location_osm_display_name"))
    )

    target      = df.filter(F.col("location_osm_display_name").contains(target_store))
    concurrents = df.filter(~F.col("location_osm_display_name").contains(target_store))

    target.write.mode("overwrite").parquet(f"{TMP_PATH}/target.parquet")
    concurrents.write.mode("overwrite").parquet(f"{TMP_PATH}/concurrents.parquet")

    print(f"Target      : {target.count()} lignes")
    print(f"Concurrents : {concurrents.count()} lignes")

    if target.count() > 0:
        max_date_tar = target.agg(F.max("created")).collect()[0][0]
        update_watermark('prices', max_date_tar)
        print(f"Watermark prices mis à jour : {max_date_tar}")

    if concurrents.count() > 0:
        max_date_con = concurrents.agg(F.max("created")).collect()[0][0]
        update_watermark('competitors', max_date_con)
        print(f"Watermark competitors mis à jour : {max_date_con}")

    spark.stop()


def valider_donnees():
    spark = get_spark()
    df_tar = spark.read.parquet(f"{TMP_PATH}/target.parquet")
    df_con = spark.read.parquet(f"{TMP_PATH}/concurrents.parquet")

    print(f"Target      : {df_tar.count()} lignes")
    print(f"Concurrents : {df_con.count()} lignes")

    COLONNES_GARDER = [
        "id", "product_code", "product_name",
        "price", "currency", "date", "created",
        "price_is_discounted", "price_without_discount",
        "type", "location_osm_id", "location_osm_display_name",
        "location_osm_address_city", "location_osm_address_country",
        "location_osm_lat", "location_osm_lon"
    ]

    cols_tar = [c for c in COLONNES_GARDER if c in df_tar.columns]
    cols_con = [c for c in COLONNES_GARDER if c in df_con.columns]

    df_tar = df_tar.select(cols_tar)
    df_con = df_con.select(cols_con)

    df_tar.write.mode("overwrite").parquet(f"{TMP_PATH}/target_validated.parquet")
    df_con.write.mode("overwrite").parquet(f"{TMP_PATH}/concurrents_validated.parquet")

    spark.stop()


with DAG(
    dag_id="dag_prices_daily",
    start_date=datetime.now(),
    schedule="@daily",
    catchup=False
) as dag:

    t1 = PythonOperator(
        task_id="telecharger_prices",
        python_callable=telecharger_prices,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    t2 = PythonOperator(
        task_id="separer_prices",
        python_callable=separer_prices
    )

    t3 = PythonOperator(
        task_id="valider_donnees",
        python_callable=valider_donnees
    )

    t1 >> t2 >> t3