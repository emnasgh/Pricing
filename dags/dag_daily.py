from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from tasks_daily import fetch_and_insert_prices, compute_coverage_stats

default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    dag_id="dag_daily",
    start_date=datetime(2026, 3, 27),
    schedule_interval=None,           
    catchup=False,
    default_args=default_args,
    max_active_runs=1,
) as dag:

    start = EmptyOperator(task_id="start")

    fetch_and_insert = PythonOperator(
        task_id="fetch_and_insert_prices",
        python_callable=fetch_and_insert_prices,
        execution_timeout=timedelta(hours=6),   
    )

    compute_coverage = PythonOperator(
        task_id="compute_coverage_stats",
        python_callable=compute_coverage_stats,
    )

    """run_sqlmesh_transformation = BashOperator(
        task_id="run_sqlmesh_transformation",
        bash_command="cd /opt/airflow/sqlmesh && sqlmesh plan --auto-apply",
    )"""

    start >> fetch_and_insert >> compute_coverage 