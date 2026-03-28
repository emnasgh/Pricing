from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from tasks_seed import seed_prices, seed_products, telecharger_fichiers

with DAG(
    dag_id="pipeline_seed",
    start_date=datetime(2026, 3, 30),  #  jour du lancement
    schedule_interval="@once",        
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

    t4 = TriggerDagRunOperator(
        task_id="trigger_daily",
        trigger_dag_id="dag_daily",
        wait_for_completion=False
    )

    # Dépendances
    t1 >> t2 >> t3 >> t4 