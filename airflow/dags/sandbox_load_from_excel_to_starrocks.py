from datetime import datetime, timedelta

from airflow import DAG
from airflow.utils.trigger_rule import TriggerRule
from airflow.operators.python import PythonOperator
from airflow.decorators import task, task_group
from airflow.models import Variable

from base_operators import (
    CheckMergedFieldsOperator,
    CheckMultipleSheetsOperator,
    DeleteInfoMetadataOperator,
)
from dbt_operators import DbtTypingOperator
from starrocks_operators import (
    StarRocksStreamLoadOperator,
    StarRocksToPostgresOperator,
    StarRocksDropTableOperator,
    StarRocksDropViewOperator,
)
from minio_operators import MinioFileSensor, MinioDeleteFileOperator


def get_changed_files(**context):
    """Получить список файлов для динамической обработки"""
    changed_files = context['ti'].xcom_pull(
        task_ids='minio_file_sensor', key='changed_files'
    )

    if not changed_files:
        return []

    return changed_files


def get_missing_files(**context):
    """Получить список файлов для динамической обработки"""
    missing_files = context['ti'].xcom_pull(
        task_ids='minio_file_sensor', key='missing_files'
    )

    if not missing_files:
        return []
    return missing_files


@task
def process_changed_file(file_data):
    """
    Обработать один файл с полной изоляцией ошибок.
    Возвращает детальную статистику обработки.
    """

    start_time = datetime.now()
    file_name = file_data['file_name']
    bucket_name = file_data['bucket']
    key = file_data['key']
    field = file_data['guid']
    table_name = file_data['table_name']
    starrocks_table = f"{table_name}_{bucket_name}"
    destination_schema = f"sandbox_db_{bucket_name.replace('bucket', '')}"
    status = file_data.get('status', 'missing')
    file_size = file_data.get('size', 0)

    # Статистика по шагам обработки
    steps_completed = []

    try:
        # 1. Проверка объединенных ячеек
        merged_operator = CheckMergedFieldsOperator(
            task_id=f"check_merged_fields_{field}",
            bucket_name=bucket_name,
            key=key,
            field=field,
            s3_conn_id="minio_default",
            postgres_conn_id="airflow_db",
        )
        merged_operator.execute({})
        steps_completed.append('check_merged_cells')

        # 2. Проверка множественных листов
        sheets_operator = CheckMultipleSheetsOperator(
            task_id=f"check_multiple_sheets_{field}",
            bucket_name=bucket_name,
            key=key,
            field=field,
            s3_conn_id="minio_default",
            postgres_conn_id="airflow_db",
        )
        sheets_operator.execute({})
        steps_completed.append('check_multiple_sheets')

        # 3. Очистка таблицы в StarRocks
        # (выполняется только если файл изменен)
        dropped = False
        if status == "changed":
            print(
                f"Файл {file_name} был изменен, выполняется DROP таблицы "
                f"{destination_schema}.{table_name}"
            )
            drop_operator = StarRocksDropTableOperator(
                task_id=f"starrocks_truncate_{field}",
                starrocks_database=destination_schema,
                starrocks_table=table_name,
                starrocks_conn_id="starrocks_default",
                dag=None,
            )
            drop_result = drop_operator.execute({})
            if drop_result:
                steps_completed.append('drop_table')
                dropped = True
        else:
            print(
                f"Файл {file_name} со статусом '{status}', "
                f"Удаление таблицы не требуется"
            )

        # 4. Загрузка в StarRocks
        starrocks_operator = StarRocksStreamLoadOperator(
            task_id=f"starrocks_stream_load_{field}",
            s3_bucket=bucket_name,
            s3_key=key,
            starrocks_table=starrocks_table,
            file_id=field,
            postgres_conn_id="airflow_db",
        )
        starrocks_result = starrocks_operator.execute({})

        # Получаем количество загруженных строк из результата
        if isinstance(starrocks_result, dict):
            rows_loaded = starrocks_result.get('rows_loaded', 0)
            starrocks_success = starrocks_result.get('success', False)
        else:
            # Обратная совместимость со старым форматом (True/False)
            rows_loaded = 0
            starrocks_success = bool(starrocks_result)

        steps_completed.append('load_to_starrocks')

        # 5. dbt типизация
        dbt_operator = DbtTypingOperator(
            task_id=f"dbt_typing_{field}",
            source_schema="stage",
            source_table=starrocks_table,
            destination_schema=destination_schema,
            destination_table=table_name,
            field=field,
            pg_conn_id="airflow_db",
            dbt_project_path="/opt/airflow/dbt/sandbox_dbt_project",
        )
        dbt_operator.execute({})
        steps_completed.append('dbt_typing')

        dropped_stage = False
        if 'dbt_typing' in steps_completed:
            print(
                f"Очистка таблицы таблицы stage.{table_name+'_'+bucket_name}"
            )
            drop_stage_operator = StarRocksDropTableOperator(
                task_id=f"starrocks_truncate_stage_{field}",
                starrocks_database='stage',
                starrocks_table=table_name+'_'+bucket_name,
                starrocks_conn_id="starrocks_default",
                dag=None,
            )
            drop_stage_operator = drop_stage_operator.execute({})
            if drop_stage_operator:
                steps_completed.append('drop_stage_table')
                dropped_stage = True

        # 6. Удаление старого файла из MinIO
        # Только если файл изменен и загрузка успешна
        deleted = False
        if status == "changed" and starrocks_success:
            delete_operator = MinioDeleteFileOperator(
                task_id=f"minio_delete_file_{field}",
                s3_bucket=bucket_name,
                file_name=file_name,
                status=status,
                s3_conn_id="minio_default",
                postgres_conn_id="airflow_db",
            )
            delete_result = delete_operator.execute({})
            if delete_result:
                steps_completed.append('delete_old_file')
                deleted = True

            # 7. Удаление информации о файле из базы данных
            delete_operator = DeleteInfoMetadataOperator(
                task_id=f"delete_info_metadata_{field}",
                file_name=file_name,
                bucket=bucket_name,
                rn=2,
                postgres_conn_id="airflow_db",
            )
            delete_result = delete_operator.execute({})

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        return {
            'file_name': file_name,
            'status': 'success',
            'bucket': bucket_name,
            'destination_schema': destination_schema,
            'destination_table': table_name,
            'starrocks_table': starrocks_table,
            'file_status': status,
            'file_size_bytes': file_size,
            'rows_loaded': rows_loaded,
            'table_dropped': dropped,
            'dropped_stage': dropped_stage,
            'old_file_deleted': deleted,
            'steps_completed': steps_completed,
            'processing_time_seconds': round(processing_time, 2),
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        }

    except Exception as e:
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        return {
            'file_name': file_name,
            'status': 'error',
            'error': str(e),
            'bucket': bucket_name,
            'file_status': status,
            'file_size_bytes': file_size,
            'steps_completed': steps_completed,
            'processing_time_seconds': round(processing_time, 2),
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        }


@task
def process_missing_file(file_data):
    """
    Обработать один файл с полной изоляцией ошибок.
    Возвращает детальную статистику обработки.
    """

    start_time = datetime.now()
    file_name = file_data['file_name']
    bucket_name = file_data['bucket']
    key = file_data['key']
    field = file_data['guid']
    table_name = file_data['table_name']
    starrocks_table = f"{table_name}_{bucket_name}"
    destination_schema = f"sandbox_db_{bucket_name.replace('bucket', '')}"
    status = file_data['status']
    file_size = file_data.get('size', 0)

    # Статистика по шагам обработки
    steps_completed = []
    errors = []

    try:
        # 1. Удаление информации о файле из базы данных
        print("Шаг 1: удаление записи из Postgres по key")
        try:
            delete_operator = DeleteInfoMetadataOperator(
                task_id=f"delete_info_metadata_{field}",
                file_name=file_name,
                bucket=bucket_name,
                postgres_conn_id="airflow_db",
            )
            delete_result = delete_operator.execute({})
            if delete_result:
                steps_completed.append('delete_info_metadata')
            else:
                steps_completed.append('delete_info_metadata_failed')
        except Exception as e:
            steps_completed.append('delete_info_metadata_error')
            errors.append(f"delete_info_metadata: {str(e)}")

        # 2. Удаление таблицы в StarRocks
        print("Шаг 2: удаление таблицы в StarRocks")
        try:
            drop_operator = StarRocksDropTableOperator(
                task_id=f"starrocks_truncate_{field}",
                starrocks_database=destination_schema,
                starrocks_table=table_name,
                starrocks_conn_id="starrocks_default",
                dag=None,
            )
            drop_result = drop_operator.execute({})
            if drop_result:
                steps_completed.append('drop_table')
            else:
                steps_completed.append('drop_table_failed')
        except Exception as e:
            steps_completed.append('drop_table_error')
            errors.append(f"drop_table: {str(e)}")

        # 3. Удаление представления в StarRocks
        print("Шаг 3: удаление представления в StarRocks")
        try:
            drop_view_operator = StarRocksDropViewOperator(
                task_id=f"starrocks_drop_view_{field}",
                starrocks_database=destination_schema,
                starrocks_view="v_"+table_name,
                starrocks_conn_id="starrocks_default",
                dag=None,
            )
            drop_view_result = drop_view_operator.execute({})
            if drop_view_result:
                steps_completed.append('drop_view')
            else:
                steps_completed.append('drop_view_failed')
        except Exception as e:
            steps_completed.append('drop_view_error')
            errors.append(f"drop_view: {str(e)}")

        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        return {
            'file_name': file_name,
            'status': 'success',
            'bucket': bucket_name,
            'destination_schema': destination_schema,
            'destination_table': table_name,
            'starrocks_table': starrocks_table,
            'file_status': status,
            'file_size_bytes': file_size,
            'steps_completed': steps_completed,
            'errors': errors,
            'processing_time_seconds': round(processing_time, 2),
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        }

    except Exception as e:
        end_time = datetime.now()
        processing_time = (end_time - start_time).total_seconds()

        return {
            'file_name': file_name,
            'status': 'error',
            'error': str(e),
            'bucket': bucket_name,
            'file_status': status,
            'file_size_bytes': file_size,
            'steps_completed': steps_completed,
            'errors': errors,
            'processing_time_seconds': round(processing_time, 2),
            'start_time': start_time.strftime('%Y-%m-%d %H:%M:%S'),
            'end_time': end_time.strftime('%Y-%m-%d %H:%M:%S'),
        }


@task
def summarize_results(changed_results=None, missing_results=None):
    """
    Подвести итоги обработки всех файлов.
    Выводит детальную информацию о каждом обработанном файле.
    """
    # Нормализуем входные результаты из двух веток
    parts = []
    for part in [changed_results, missing_results]:
        if part is None:
            continue
        try:
            seq = list(part)
        except Exception:
            seq = [part]
        for item in seq:
            if isinstance(item, (list, tuple)):
                parts.extend([x for x in item if isinstance(x, dict)])
            elif isinstance(item, dict):
                parts.append(item)

    results = parts

    if not results:
        print(
            "Нет файлов для обработки. Обе ветки пустые "
            "(new/changed/missing = 0)"
        )
        return {
            'total': 0,
            'successful': 0,
            'failed': 0,
            'total_rows_loaded': 0,
            'total_processing_time_seconds': 0,
            'total_size_bytes': 0,
        }

    # results уже нормализован

    successful = [r for r in results if r.get('status') == 'success']
    failed = [r for r in results if r.get('status') == 'error']
    deleted_files = [r for r in results if r.get('status') == 'deleted']

    # Разбивка по типу файла (new/changed/missing)
    by_kind = {
        'new': [r for r in successful if r.get('file_status') == 'new'],
        'changed': [r for r in successful if r.get('file_status') == 'changed'],
        'missing': [r for r in results if r.get('file_status') == 'missing'],
    }

    total_rows = sum(r.get('rows_loaded', 0) for r in successful)
    total_time = sum(r.get('processing_time_seconds', 0) for r in results)
    total_size = sum(r.get('file_size_bytes', 0) for r in results)

    print("=" * 80)
    print("ИТОГОВАЯ СТАТИСТИКА ОБРАБОТКИ ФАЙЛОВ")
    print("=" * 80)
    print(f"Всего файлов обработано: {len(results)}")
    print(f"Успешно: {len(successful)}")
    print(f"С ошибками: {len(failed)}")
    print(f"Общее время обработки: {total_time:.2f} сек")
    print(f"Общий размер файлов: {total_size:,} байт "
          f"({total_size / 1024 / 1024:.2f} MB)")

    # Краткая сводка по типам
    print("КАТЕГОРИИ:")
    print(f"  new: {len(by_kind['new'])}")
    print(f"  changed: {len(by_kind['changed'])}")
    print(f"  missing: {len(by_kind['missing'])}")

    if successful:
        print("\n" + "=" * 80)
        print(f"УСПЕШНО ОБРАБОТАННЫЕ ФАЙЛЫ ({len(successful)})")
        print("=" * 80)

        for idx, result in enumerate(successful, 1):
            print(f"\n{idx}. Файл: {result['file_name']}")
            print(f"   Бакет: {result['bucket']}")
            print(f"   Статус файла: {result.get('file_status', '-')}")
            print(f"   Схема назначения: {result['destination_schema']}")
            print(f"   Таблица: {result['destination_table']}")
            print(f"   Размер файла: {result['file_size_bytes']:,} байт")
            print(
                f"   Загружено строк: {result.get('rows_loaded', 0):,}"
            )
            print(f"   Таблица в destination_schema удалена: "
                  f"{'Да' if result.get('table_dropped') else 'Нет'}")
            print(f"   Старый файл удален: "
                  f"{'Да' if result.get('old_file_deleted') else 'Нет'}")
            print(f"   Осуществлено удаление таблицы в stage:"
                  f"{'Да' if result.get('dropped_stage') else 'Нет'}")
            print(f"   Время обработки: "
                  f"{result['processing_time_seconds']:.2f} сек")
            print(f"   Начало: {result['start_time']}")
            print(f"   Конец: {result['end_time']}")

            steps = result.get('steps_completed', [])
            if steps:
                print(f"   Выполненные шаги ({len(steps)}): "
                      f"{', '.join(steps)}")

    if failed:
        print("\n" + "=" * 80)
        print(f"ФАЙЛЫ С ОШИБКАМИ ({len(failed)})")
        print("=" * 80)

        for idx, result in enumerate(failed, 1):
            print(f"\n{idx}. Файл: {result['file_name']}")
            print(f"   Бакет: {result['bucket']}")
            print(f"   Статус файла: {result.get('file_status', '-')}")
            print(f"   Размер файла: {result['file_size_bytes']:,} байт")
            print(f"   Время до ошибки: "
                  f"{result['processing_time_seconds']:.2f} сек")
            print(f"   Начало: {result['start_time']}")
            print(f"   Ошибка: {result.get('error', 'Unknown error')}")

            steps = result.get('steps_completed', [])
            if steps:
                print(f"   Выполнено шагов до ошибки ({len(steps)}): "
                      f"{', '.join(steps)}")

        print("\n" + "=" * 80)

    if deleted_files:
        print("\n" + "=" * 80)
        print(f"ФАЙЛЫ УДАЛЕНЫ ({len(deleted_files)})")
        print("=" * 80)
        for idx, result in enumerate(deleted_files, 1):
            print(f"\n{idx}. Файл: {result['file_name']}")
            print(f"   Бакет: {result['bucket']}")
            print(f"   Статус файла: {result['file_status']}")
            print(f"   Размер файла: {result['file_size_bytes']:,} байт")
            print(f"   Время обработки: "
                  f"{result['processing_time_seconds']:.2f} сек")
            print(f"   Начало: {result['start_time']}")
            print(f"   Конец: {result['end_time']}")
            print(
                "   Выполненные шаги ("
                f"{len(result.get('steps_completed', []))}"
                "): "
                f"{', '.join(result.get('steps_completed', []))}"
            )
            print(f"   Ошибка: {result.get('error', 'Unknown error')}")
            print(
                "   Выполненные шаги ("
                f"{len(result.get('steps_completed', []))}"
                "): "
                f"{', '.join(result.get('steps_completed', []))}"
            )

        print("\n" + "=" * 80)

    return {
        'total': len(results),
        'successful': len(successful),
        'failed': len(failed),
        'total_rows_loaded': total_rows,
        'total_processing_time_seconds': round(total_time, 2),
        'total_size_bytes': total_size,
    }


# =============================================================================
# DAG ОПРЕДЕЛЕНИЕ
# =============================================================================

DEFAULT_ARGS = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 1, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(seconds=30),
}

with DAG(
    "sandbox_load_from_excel_to_starrocks",
    default_args=DEFAULT_ARGS,
    schedule=timedelta(seconds=30),
    catchup=False,
    tags=["minio_file_sensor", "production", "parallel"],
    max_active_runs=1,
    description="DAG для загрузки данных из Excel в StarRocks",
) as dag:

    # Сенсор для мониторинга файлов в MinIO
    minio_file_sensor = MinioFileSensor(
        task_id="minio_file_sensor",
        s3_buckets=[
            "hrbucket",
            "commercialbucket",
            "medicalbucket",
            "financialbucket",
            "accountingbucket"
        ],
        metadata_table="public.file_metadata",
        s3_conn_id="minio_default",
        postgres_conn_id="airflow_db",
        poke_interval=15,
        timeout=3600,
        mode="reschedule",
    )

    # Получение списка файлов для обработки
    get_changed_files_task = PythonOperator(
        task_id="get_changed_files",
        python_callable=get_changed_files,
    )

    get_missing_files_task = PythonOperator(
        task_id="get_missing_files",
        python_callable=get_missing_files,
    )

    # Параллельная обработка файлов (dynamic task mapping)
    process_changed_files = process_changed_file.expand(
        file_data=get_changed_files_task.output
    )

    process_missing_files = process_missing_file.expand(
        file_data=get_missing_files_task.output
    )

    # Подведение итогов (передаём список XComArg, без конкатенации операторов)
    summarize_task = summarize_results.override(
        task_id="summarize_results",
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    )(
        changed_results=process_changed_files,
        missing_results=process_missing_files,
    )


    @task_group(group_id="sync_sandbox_group")
    def sync_sandbox_group():
        dbs = [
            "sandbox_db_commercial",
            "sandbox_db_medical",
            "sandbox_db_hr",
            "sandbox_db_accounting",
            "sandbox_db_financial",
        ]

        prev = None
        for db_name in dbs:
            task = StarRocksToPostgresOperator(
                task_id=f"sync_{db_name}",
                starrocks_database=db_name,
                postgres_database=db_name,
                postgres_schema="public",
                starrocks_conn_id="starrocks_default",
                postgres_conn_id="airflow_db",
                size_batch=50000,
                exclude_views=False,
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
                dag=dag,
            )
            if prev is not None:
                prev >> task
            prev = task
        return prev

    # Определение зависимостей
    # Зависимости без вложенных списков
    minio_file_sensor >> get_changed_files_task
    minio_file_sensor >> get_missing_files_task
    get_changed_files_task >> process_changed_files
    get_missing_files_task >> process_missing_files
    [process_changed_files, process_missing_files] >> summarize_task
    summarize_task >> sync_sandbox_group()
