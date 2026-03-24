from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from tasks_seed import telecharger_fichiers, seed_prices, seed_products

with DAG(
    dag_id="pipeline_seed",
    start_date=datetime(2024, 1, 1),
    schedule=None,
    catchup=False,
    tags=["seed", "init"]
) as dag:

    t1 = PythonOperator(
        task_id="telecharger_fichiers",
        python_callable=telecharger_fichiers
    )

    t2 = PythonOperator(
        task_id="seed_prices",
        python_callable=seed_prices
    )

    t3 = PythonOperator(
        task_id="seed_products",
        python_callable=seed_products
    )

    t1 >> t2 >> t3