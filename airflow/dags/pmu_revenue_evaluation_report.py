from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

from starrocks_operators.starrocks_to_postgres_operator import StarRocksToPostgresOperator

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
    "pmu_revenue_evaluation_report",
    default_args=DEFAULT_ARGS,
    schedule=None,
    catchup=False,
    tags=["dbt"],
    max_active_runs=1,
    description="DAG запуска дбт моделей, mart и report",
) as dag:
    
    start = BashOperator(task_id='start',
                                   bash_command = """cd / && ls opt/airflow/dbt/main_dbt_project/"""
                                   )                                 
    dbt_run_mart = BashOperator(task_id='dbt_run_mart',
                                   bash_command = """cd / && cd /opt/airflow/dbt/main_dbt_project/models && 
                                   dbt run --profiles-dir /opt/airflow/dbt/main_dbt_project/models/rzdm_rdv --select rzdm_mart+ --target rzdm_mart"""
                                   ) 
                                
    dbt_run_report = BashOperator(task_id='dbt_run_report',
                                       bash_command = """cd / && cd /opt/airflow/dbt/main_dbt_project/models && 
                                   dbt run --profiles-dir /opt/airflow/dbt/main_dbt_project/models/rzdm_rdv --select rzdm_report+ --target rzdm_report"""
                                   )                                   

    sync_mart = StarRocksToPostgresOperator(
        task_id="sync_mart",
        starrocks_database="main_mart",
        postgres_database="starrocks_db",
        postgres_schema="main_mart",
        starrocks_conn_id="starrocks_default",
        postgres_conn_id="airflow_db",
        size_batch=50000,
        exclude_views=False,
        dag=dag,
    )

    sync_report = StarRocksToPostgresOperator(
        task_id="sync_report",
        starrocks_database="main_report",
        postgres_database="starrocks_db",
        postgres_schema="main_report",
        starrocks_conn_id="starrocks_default",
        postgres_conn_id="airflow_db",
        size_batch=50000,
        exclude_views=False,
        dag=dag,
    )

    # Зависимости
    start >> dbt_run_mart >> dbt_run_report >> sync_mart >> sync_report
    
