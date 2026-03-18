from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta
from tasks import telecharger_prices, inserer_prices_raw

with DAG(
    dag_id="pipeline_prices",
    start_date=datetime(2024, 1, 1),
    schedule="@daily",
    catchup=False
) as dag:

    #  T1 : Extract 
    t1 = PythonOperator(
        task_id="telecharger_prices",
        python_callable=telecharger_prices,
        retries=3,
        retry_delay=timedelta(minutes=5)
    )

    #  T2 : Load : prices_raw 
    t2 = PythonOperator(
        task_id="inserer_prices_raw",
        python_callable=inserer_prices_raw,
        retries=2,
        retry_delay=timedelta(minutes=2)
    )
    
    t3 = BashOperator(
        task_id="sqlmesh_plan",
        bash_command="cd /opt/airflow/sqlmesh && sqlmesh plan --auto-apply"
    )

    t1 >> t2 >> t3
  

