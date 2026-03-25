from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from datetime import datetime
from tasks_seed import seed_prices, seed_products ,telecharger_fichiers

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

    """t4 = BashOperator(
        task_id="sqlmesh_plan",
        bash_command="cd /opt/airflow/sqlmesh && sqlmesh plan --auto-apply"
    )"""

    t1 >> t2 >>t3