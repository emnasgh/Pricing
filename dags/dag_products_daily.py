"""from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import psycopg2
import requests
import polars as pl
import time
import json

DB_CONFIG={
    "host": "host.docker.internal",
    "port": 5432,
    "database": "retail_pricing",
    "user": "postgres",   
}

API_URL = "https://prices.openfoodfacts.org/api/v1/products"

def get_watermark(name):
    conn=psycopg2.connect(**DB_CONFIG)
    cursor=conn.cursor()
    cursor.execute("SELECT last_processed_date FROM watermark WHERE dataset_name=?", (name,))
    result= cursor.fetchone()
    cursor.close()
    conn.close()
    return result[0]

def update_watermark(new_date):
    conn   = psycopg2.connect(**DB_CONFIG)
    cursor = conn.cursor()
    cursor.execute(
        "UPDATE watermark SET last_processed_date=%s WHERE dataset_name='prices'",
        (new_date,)
    )
    conn.commit()
    cursor.close()
    conn.close()

def telecharger_prices():
    last_date_food = get_watermark("food")
    last_date_beauty = get_watermark("beauty")
    last_date_other = get_watermark("other")
    
    """