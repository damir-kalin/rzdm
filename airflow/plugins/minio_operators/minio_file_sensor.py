"""
Сенсор для мониторинга изменений файлов в MinIO
"""
import pandas as pd
from datetime import datetime
from airflow.sensors.base import BaseSensorOperator

from base_operators.s3_file_mixin import S3FileMixin
from base_operators.file_status_mixin import FileStatusMixin
from base_operators.file_validation_mixin import FileValidationMixin


class MinioFileSensor(
    BaseSensorOperator,
    S3FileMixin,
    FileStatusMixin,
    FileValidationMixin
):
    """
    Сенсор для мониторинга изменений в файлах в бакетах MinIO.
    Отслеживает изменения и обновляет информацию в базе данных.
    Использует миксины для переиспользования кода.
    """

    def __init__(
        self,
        s3_conn_id: str = "minio_default",
        s3_buckets: list = None,
        postgres_conn_id: str = "airflow_db",
        metadata_table: str = "public.file_metadata",
        *args,
        **kwargs,
    ):
        """
        Args:
            s3_conn_id: ID соединения S3/MinIO
            s3_buckets: Список бакетов для мониторинга
            postgres_conn_id: ID соединения PostgreSQL
            metadata_table: Таблица метаданных
            **kwargs: Дополнительные параметры для BaseSensorOperator
        """
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_buckets = s3_buckets or [
            "hrbucket",
            "commercialbucket",
            "medicalbucket",
            "financialbucket",
            "accountingbucket"
        ]
        self.postgres_conn_id = postgres_conn_id
        self.metadata_table = metadata_table

    def get_postgres_hook(self, database: str = "service_db"):
        """Override для совместимости с миксинами"""
        from airflow.providers.postgres.hooks.postgres import PostgresHook
        return PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            database=database
        )

    def get_s3_hook(self):
        """Override для совместимости с миксинами"""
        from airflow.providers.amazon.aws.hooks.s3 import S3Hook
        return S3Hook(aws_conn_id=self.s3_conn_id)

    def _get_start_date(self, context):
        """Получить start_date как datetime объект"""
        start_date = context.get('start_date')
        if isinstance(start_date, str):
            # Парсим строку в datetime
            try:
                return datetime.fromisoformat(
                    start_date.replace('Z', '+00:00')
                )
            except ValueError:
                # Если не удается распарсить, используем текущее время
                return datetime.utcnow()
        return start_date

    def execute(self, context):
        """
        Переопределяем execute полностью для обхода проблемы с run_duration
        """
        # Исправляем start_date в контексте
        if 'start_date' in context and isinstance(context['start_date'], str):
            context['start_date'] = self._get_start_date(context)

        # Вызываем poke напрямую, обходя базовый execute
        return self.poke(context)

    def check_file_change(
        self,
        file_name: str,
        user_name: str,
        guid: str,
        bucket: str,
        last_modified,
        size: int,
        key: str
    ) -> int:
        """
        Проверить изменение файла в базе данных

        Returns:
            1 - новый файл
            2 - измененный файл
            3 - ошибка расширения
            4 - без изменений
        """
        hook = self.get_postgres_hook(database="service_db")
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            sql = ("SELECT check_update_file_s3(%s::VARCHAR, %s::VARCHAR, "
                   "%s::VARCHAR, %s::VARCHAR, %s::TIMESTAMP, %s::BIGINT, "
                   "%s::VARCHAR);")
            cur.execute(
                sql,
                (file_name, user_name, guid, bucket, last_modified, size, key)
            )
            result = cur.fetchone()
            conn.commit()
            return result[0]
        except Exception as e:
            conn.rollback()
            self.log.error(f"Ошибка при проверке файла {file_name}: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    def _get_file_info(self):
        """Получить информацию о текущих файлах из S3"""
        hook = self.get_s3_hook()
        objects = []

        for bucket in self.s3_buckets:
            keys = hook.list_keys(bucket_name=bucket)
            if keys:
                for key in keys:
                    # Парсим имя файла
                    file_name, user_name, guid = self.parse_file_name(key)

                    # Получаем метаданные объекта
                    obj = hook.get_key(key, bucket_name=bucket)
                    objects.append({
                        "key": key,
                        "file_name": file_name,
                        "user_name": user_name,
                        "guid": guid,
                        "bucket": bucket,
                        "table_name": file_name.replace(" ", "_")
                                               .replace(".", "_"),
                        "last_modified": pd.to_datetime(obj.last_modified),
                        "size": int(obj.content_length)
                    })

        return pd.DataFrame(
            objects,
            columns=[
                "key", "file_name", "user_name", "guid", "bucket",
                "table_name", "last_modified", "size"
            ]
        )
    
    def _get_missing_files(self, current_df: pd.DataFrame) -> list:
        """Получить отсутствующие в S3 файлы из метаданных Postgres"""
        missing_files: list = []
        try:
            hook = self.get_postgres_hook(database="service_db")
            conn = hook.get_conn()
            cur = conn.cursor()
            keys = current_df["key"].astype(str).tolist()
            self.log.info(f"Получены следующие ключи: {keys}")
            cur.execute(
                (
                    "SELECT key, file_name, user_name, "
                    "guid, bucket, table_name "
                    "FROM public.get_missing_s3_files(%s)"
                ),
                (keys,)
            )
            rows = cur.fetchall()
            missing_files = [
                {
                    "key": r[0],
                    "file_name": r[1],
                    "user_name": r[2],
                    "guid": r[3],
                    "bucket": r[4],
                    "table_name": r[5],
                    "status": "missing",
                }
                for r in rows
            ]
            conn.commit()
            self.log.info(
                "Получены следующие файлы, отсутствующие в S3: %s",
                missing_files
            )
        except Exception as e:
            if 'conn' in locals():
                conn.rollback()
            self.log.error(
                f"Ошибка получения списка файлов, отсутствующих в S3: {e}"
            )
            missing_files = []
        finally:
            if 'cur' in locals():
                cur.close()
            if 'conn' in locals():
                conn.close()
        
        return missing_files
        
    def poke(self, context):
        """Проверить наличие изменений в файлах"""
        current_df = self._get_file_info()
        missing_files = self._get_missing_files(current_df)
        processed_files = []  # Инициализируем заранее

        if not current_df.empty or missing_files:
            if missing_files:
                self.log.info("Файлы, которые были удалены из бакетов: %s", missing_files)
                context["ti"].xcom_push(
                    key="missing_files",
                    value=missing_files
                )

            if not current_df.empty:
                self.log.info(
                    "Обнаружены следующие файлы: %s",
                    current_df["file_name"].tolist()
                )

                new_files = []
                changed_files = []
                no_changed_files = []
                error_files = []

                # Проверяем каждый файл
                for _, row in current_df.iterrows():
                    result = self.check_file_change(
                        row["file_name"],
                        row["user_name"],
                        row["guid"],
                        row["bucket"],
                        row["last_modified"],
                        row["size"],
                        row["key"]
                    )

                    file_info = {
                        "key": row["key"],
                        "file_name": row["file_name"],
                        "user_name": row["user_name"],
                        "guid": row["guid"],
                        "bucket": row["bucket"],
                        "table_name": row["table_name"],
                        "last_modified": (row["last_modified"]
                                        .strftime("%Y-%m-%d %H:%M:%S")
                                        if pd.notna(row["last_modified"])
                                        else None),
                        "size": int(row["size"]),
                        "status": "unknown"
                    }

                    # Классифицируем результат
                    if result == 1:
                        file_info["status"] = "new"
                        new_files.append(file_info)
                    elif result == 2:
                        file_info["status"] = "changed"
                        changed_files.append(file_info)
                    elif result == 3:
                        file_info["status"] = "error"
                        error_files.append(file_info)
                        self.log.error(
                            "В бакете %s при обработке файла %s "
                            "возникла ошибка расширения файла.",
                            row["bucket"],
                            row["file_name"]
                        )
                    elif result == 4:
                        file_info["status"] = "no_changed"
                        no_changed_files.append(file_info)

                # Логируем результаты
                self._log_results(
                    new_files,
                    changed_files,
                    no_changed_files,
                    error_files
                )

                # Пушим обработанные файлы в XCom
                processed_files = new_files + changed_files

                if processed_files:
                    context["ti"].xcom_push(
                        key="changed_files",
                        value=processed_files
                    )
            
            if processed_files or missing_files:
                return True # Если есть изменения или отсутствующие файлы, то возвращаем True
            else: 
                return False # Если нет изменений и нет отсутствующих файлов, то возвращаем False
        else:
            self.log.info("Файлы не обнаружены в бакетах и нет файлов, которые были удалены.")
            return False # Если бакет пуст и нет отсутствующих файлов, то возвращаем False


    def _log_results(
        self,
        new_files,
        changed_files,
        no_changed_files,
        error_files
    ):
        """Логировать результаты обработки"""
        self.log.info("Обработка файлов завершена. Результаты:")

        if new_files:
            self.log.info("Обнаружено %s новых файлов.", len(new_files))
            self.log.info("Новые файлы: %s", new_files)

        if changed_files:
            self.log.info(
                "Обнаружено %s измененных файлов.",
                len(changed_files)
            )
            self.log.info("Измененные файлы: %s", changed_files)

        if no_changed_files:
            self.log.info(
                "Обнаружено %s файлов без изменений.",
                len(no_changed_files)
            )
            self.log.info("Файлы без изменений: %s", no_changed_files)

        if error_files:
            self.log.info(
                "Обнаружено %s файлов с ошибками расширения файла.",
                len(error_files)
            )
            self.log.info("Файлы с ошибками: %s", error_files)
