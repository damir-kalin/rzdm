from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.sdk import task_group
from airflow.models import Variable

from cosmos import (
    DbtTaskGroup,
    ProfileConfig,
    ProjectConfig,
    ExecutionConfig,
    RenderConfig,
)
from cosmos.constants import ExecutionMode

from starrocks_operators.starrocks_to_postgres_operator import StarRocksToPostgresOperator

# Конфигурация проекта dbt
# Используем кеширование для более быстрого парсинга при импорте DAG
project_config = ProjectConfig(
    dbt_project_path="/opt/airflow/dbt/main_dbt_project",
    models_relative_path="models",
)


def get_profile_config(target_name: str):
    """Создает ProfileConfig для указанного target"""
    return ProfileConfig(
        profile_name="main_dbt_project",
        target_name=target_name,
        profiles_yml_filepath="/opt/airflow/dbt/main_dbt_project/profiles.yml",
    )


# Конфигурация выполнения - LOCAL режим для выполнения внутри Airflow
execution_config = ExecutionConfig(
    execution_mode=ExecutionMode.LOCAL,
)

# Конфигурация рендеринга - будет переопределяться для каждой группы


def get_render_config(select_path: str):
    """Создает RenderConfig с фильтрацией по пути.

    Args:
        select_path: Путь для фильтрации моделей
            (например, "path:models/rzdm_rdv/hubs")

    Returns:
        RenderConfig с настроенной фильтрацией
    """
    return RenderConfig(
        emit_datasets=True,
        select=[select_path],  # select должен быть списком
        # Тесты включены, но настроены с severity: warn,
        # поэтому они не будут ломать пайплайн
    )


# Аргументы по умолчанию для DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 5,
    "retry_delay": timedelta(seconds=10),
}

with DAG(
    dag_id="main_dbt",
    default_args=default_args,
    schedule="@daily",
    catchup=False,
    tags=["dbt", "cosmos", "data_pipeline"],
    description=(
        "DAG для пошаговой трансформации данных через dbt с использованием "
        "Cosmos. Использует DbtTaskGroup для автоматического управления "
        "зависимостями моделей."
    ),
    max_active_runs=1,
    # Ограничение параллельных задач для предотвращения перегрузки
    max_active_tasks=2,
) as dag:

    # Начальная задача
    start = EmptyOperator(
        task_id="start",
        doc_md="Начало выполнения DAG трансформации данных",
    )

    @task_group(group_id="rdv_group")
    def rdv_group():
        # Группа задач 1: RDV Hubs - создание хабов Raw Data Vault
        # Используем path селектор для фильтрации моделей на этапе создания задач
        rdv_hubs_group = DbtTaskGroup(
            group_id="rdv_hubs_group",
            project_config=project_config,
            profile_config=get_profile_config("rzdm_rdv"),
            execution_config=execution_config,
            render_config=get_render_config("path:models/rzdm_rdv/hubs"),
            operator_args={
                "select": "rzdm_rdv.hubs+",
                "full_refresh": False,
                # Исключаем relationships тесты, так как они могут
                # ссылаться на модели из других групп (links), которые
                # еще не созданы. Используем паттерн для исключения всех
                # тестов, содержащих "relationships"
                "exclude": "test_name:*relationships*",
            },
            dag=dag,
        )

        # Группа задач 2: RDV Satellites - создание сателлитов Raw Data Vault
        # Используем full_refresh=True для incremental моделей,
        # так как pre-hook удаляет таблицы перед созданием
        rdv_satellites_group = DbtTaskGroup(
            group_id="rdv_satellites_group",
            project_config=project_config,
            profile_config=get_profile_config("rzdm_rdv"),
            execution_config=execution_config,
            render_config=get_render_config("path:models/rzdm_rdv/satellites"),
            operator_args={
                "select": "rzdm_rdv.satellites+",
                "full_refresh": True,  # Необходимо для incremental моделей
                # Исключаем relationships тесты, так как они могут
                # ссылаться на модели из других групп (links), которые
                # еще не созданы. Используем паттерн для исключения всех
                # тестов, содержащих "relationships"
                "exclude": "test_name:*relationships*",
            },
            dag=dag,
        )

        # Группа задач 3: RDV Links - создание связей Raw Data Vault
        rdv_links_group = DbtTaskGroup(
            group_id="rdv_links_group",
            project_config=project_config,
            profile_config=get_profile_config("rzdm_rdv"),
            execution_config=execution_config,
            render_config=get_render_config("path:models/rzdm_rdv/links"),
            operator_args={
                "select": "rzdm_rdv.links+",
                "full_refresh": False,
            },
            dag=dag,
        )

        return rdv_hubs_group >> rdv_satellites_group >> rdv_links_group

    @task_group(group_id="bdv_group")
    def bdv_group():
        # Группа задач 4: BDV - бизнес-витрина данных (Business Data Vault)
        bdv_hubs_group = DbtTaskGroup(
            group_id="bdv_hubs_group",
            project_config=project_config,
            profile_config=get_profile_config("rzdm_bdv"),
            execution_config=execution_config,
            render_config=get_render_config("path:models/rzdm_bdv/hubs"),
            operator_args={
                "select": "rzdm_bdv.hubs+",
                "full_refresh": False,
                # Исключаем relationships тесты, так как они могут
                # ссылаться на модели из других групп (links), которые
                # еще не созданы. Используем паттерн для исключения всех
                # тестов, содержащих "relationships"
                "exclude": "test_name:*relationships*",
            },  
            dag=dag,
        )

        bdv_satellites_group = DbtTaskGroup(
            group_id="bdv_satellites_group",
            project_config=project_config,
            profile_config=get_profile_config("rzdm_bdv"),
            execution_config=execution_config,
            render_config=get_render_config("path:models/rzdm_bdv/satellites"),
            operator_args={
                "select": "rzdm_bdv.satellites+",
                "full_refresh": False,
            },
            dag=dag,
        )

        bdv_links_group = DbtTaskGroup(
            group_id="bdv_links_group",
            project_config=project_config,
            profile_config=get_profile_config("rzdm_bdv"),
            execution_config=execution_config,
            render_config=get_render_config("path:models/rzdm_bdv/links"),
            operator_args={
                "select": "rzdm_bdv.links+",
                "full_refresh": False,
            },
            dag=dag,
        )

        return bdv_hubs_group >> bdv_satellites_group >> bdv_links_group

    @task_group(group_id="mart_group")
    def mart_group():
        # Группа задач 5: MART - витрина данных
        mart_dim_group = DbtTaskGroup(
            group_id="mart_dim_group",
            project_config=project_config,
            profile_config=get_profile_config("rzdm_mart"),
            execution_config=execution_config,
            render_config=get_render_config("path:models/rzdm_mart/dims"),
            operator_args={
                "select": "path:models/rzdm_mart/dims+",
                "full_refresh": False,
            },
            dag=dag,
        )

        mart_fact_group = DbtTaskGroup(
            group_id="mart_fact_group",
            project_config=project_config,
            profile_config=get_profile_config("rzdm_mart"),
            execution_config=execution_config,
            render_config=get_render_config("path:models/rzdm_mart/facts"),
            operator_args={
                "select": "path:models/rzdm_mart/facts+",
                "full_refresh": False,
            },
            dag=dag,
        )

        return mart_dim_group >> mart_fact_group

    @task_group(group_id="report_group")
    def report_group():
        # Группа задач 6: REPORT - отчеты
        report_group = DbtTaskGroup(
            group_id="report_group",
            project_config=project_config,
            profile_config=get_profile_config("rzdm_report"),
            execution_config=execution_config,
            render_config=get_render_config("path:models/rzdm_report"),
            operator_args={
                "select": "rzdm_report+",
                "full_refresh": False,
            },
            dag=dag,
        )
        return report_group

    # Синхронизация StarRocks -> Postgres по БД/схемам
    # Имя БД Postgres берём из Variable, схема по слоям
    # @task_group(group_id="sync_group")
    # def sync_group():
    sync_rdv = StarRocksToPostgresOperator(
        task_id="sync_rdv",
        starrocks_database="main_rdv",
        postgres_database="starrocks_db",
        postgres_schema="main_rdv",
        starrocks_conn_id="starrocks_default",
        postgres_conn_id="airflow_db",
        size_batch=50000,
        exclude_views=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )

    sync_bdv = StarRocksToPostgresOperator(
        task_id="sync_bdv",
        starrocks_database="main_bdv",
        postgres_database="starrocks_db",
        postgres_schema="main_bdv",
        starrocks_conn_id="starrocks_default",
        postgres_conn_id="airflow_db",
        size_batch=50000,
        exclude_views=False,
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
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
        trigger_rule=TriggerRule.ALL_SUCCESS,
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
        trigger_rule=TriggerRule.ALL_SUCCESS,
        dag=dag,
    )
        # return sync_rdv >> sync_bdv >> sync_mart >> sync_report

    # Конечная задача
    end = EmptyOperator(
        task_id="end",
        trigger_rule=TriggerRule.ALL_SUCCESS,
        doc_md="Завершение выполнения DAG трансформации данных",
    )

    # Определение зависимостей между группами задач
    # Строгая последовательность с барьерами между группами
    rdv = rdv_group()
    rdv_done = EmptyOperator(task_id="rdv_done", trigger_rule=TriggerRule.ALL_SUCCESS)

    bdv = bdv_group()
    bdv_done = EmptyOperator(task_id="bdv_done", trigger_rule=TriggerRule.ALL_SUCCESS)

    mart = mart_group()
    mart_done = EmptyOperator(task_id="mart_done", trigger_rule=TriggerRule.ALL_SUCCESS)

    report = report_group()
    report_done = EmptyOperator(task_id="report_done", trigger_rule=TriggerRule.ALL_SUCCESS)

    start >> rdv >> rdv_done >> sync_rdv >> \
    bdv >> bdv_done >> sync_bdv >> \
    mart >> mart_done >> sync_mart >> \
    report >> report_done >> sync_report >> end
