from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator



DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 0,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "pmu_revenue_evaluation_rdv",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["dbt"],
    max_active_runs=1,
    description="DAG запуска дбт моделей, тест",
) as dag:
    
    start = BashOperator(task_id='start',
                                   bash_command = """cd / && ls opt/airflow/dbt/main_dbt_project/"""
                                   ) 
    dbt_run_rdv = BashOperator(task_id='dbt_run_rdv',
                                   bash_command = """cd / && cd /opt/airflow/dbt/main_dbt_project/models && 
                                   dbt run --profiles-dir /opt/airflow/dbt/main_dbt_project/models/rzdm_rdv --select hub_rdv__revenue --target rzdm_rdv &&
                                   dbt run --profiles-dir /opt/airflow/dbt/main_dbt_project/models/rzdm_rdv --select sat_rdv__revenue --target rzdm_rdv"""
                                   )                                  


    # Зависимости
    start >> dbt_run_rdv 
    
