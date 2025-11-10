"""
Оператор для загрузки данных в StarRocks через Stream Load API.
"""
from typing import Optional
from airflow.providers.amazon.aws.hooks.s3 import S3Hook

from base_operators.base_hooks_operator import BaseHooksOperator
from starrocks_operators.starrocks_connection_mixin import StarRocksConnectionMixin
from starrocks_operators.starrocks_table_manager import StarRocksTableManager
from starrocks_operators.starrocks_stream_loader import StarRocksStreamLoader
from starrocks_operators.starrocks_data_converter import StarRocksDataConverter
from base_operators.file_status_mixin import FileStatusMixin


class StarRocksStreamLoadOperator(
    BaseHooksOperator,
    StarRocksConnectionMixin,
    FileStatusMixin
):
    """
    Оператор для загрузки данных из Excel файлов S3 в StarRocks
    используя Stream Load API.
    """

    def __init__(
        self,
        s3_bucket: str,
        s3_key: str,
        starrocks_table: str,
        file_id: str,
        s3_conn_id: str = "minio_default",
        postgres_conn_id: str = "airflow_db",
        starrocks_conn_id: str = "starrocks_default",
        starrocks_database: str = "stage",
        column_separator: str = "|",
        timeout: int = 600,
        max_retries: int = 3,
        chunk_size: Optional[int] = None,
        *args,
        **kwargs,
    ):
        """
        Args:
            s3_bucket: Имя S3 бакета
            s3_key: Ключ файла в S3
            starrocks_table: Имя таблицы в StarRocks
            file_id: ID файла для отслеживания статуса
            s3_conn_id: ID подключения S3 в Airflow
            postgres_conn_id: ID подключения PostgreSQL в Airflow
            starrocks_conn_id: ID подключения StarRocks в Airflow
            starrocks_database: Имя базы данных в StarRocks
            column_separator: Разделитель колонок для CSV
            timeout: Таймаут операций в секундах
            max_retries: Количество повторных попыток
            chunk_size: Размер чанка для разбиения больших файлов
        """
        super().__init__(
            s3_conn_id=s3_conn_id,
            postgres_conn_id=postgres_conn_id,
            starrocks_conn_id=starrocks_conn_id,
            *args,
            **kwargs
        )

        # Параметры файла и таблицы
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.starrocks_database = starrocks_database
        self.starrocks_table = starrocks_table
        self.file_id = file_id

        # Параметры загрузки
        self.column_separator = column_separator
        self.timeout = timeout
        self.max_retries = max_retries
        self.chunk_size = chunk_size

        # Инициализация подключения к StarRocks
        self._initialize_starrocks_connection(
            conn_id=starrocks_conn_id,
            max_retries=max_retries
        )

        # Инициализация компонентов
        self.table_manager = StarRocksTableManager(
            mysql_conn_id=starrocks_conn_id,
            database=starrocks_database,
            logger=self.log
        )

        self.stream_loader = StarRocksStreamLoader(
            session=self.starrocks_session,
            host=self.starrocks_host,
            port=self.starrocks_port,
            user=self.starrocks_credentials[0],
            password=self.starrocks_credentials[1],
            timeout=timeout,
            logger=self.log
        )

        self.data_converter = StarRocksDataConverter(logger=self.log)

    def execute(self, context):
        """
        Выполняет загрузку данных из Excel файла в StarRocks.

        Returns:
            dict: Результат загрузки с метриками
        """
        try:
            self.log.info(
                f"Начало загрузки: {self.s3_key} из бакета {self.s3_bucket} "
                f"в таблицу {self.starrocks_database}.{self.starrocks_table}"
            )

            # 1. Обновляем статус: начало загрузки
            self.update_file_status(
                self.file_id,
                "file_load_to_store"
            )

            # 2. Получаем данные из S3
            file_content = self._get_file_from_s3()

            # 3. Читаем Excel файл
            self.update_file_status(
                self.file_id,
                "reading_file"
            )
            df = self.data_converter.read_excel_from_bytes(file_content)

            # 4. Получаем колонки и создаем таблицу
            columns = self.table_manager.get_columns_from_dataframe(df)
            self.table_manager.create_table_if_not_exists(
                table_name=self.starrocks_table,
                columns=columns
            )

            # 5. Конвертируем в CSV
            csv_data = self.data_converter.dataframe_to_csv(
                df,
                self.column_separator
            )

            # 6. Загружаем в StarRocks
            if self.chunk_size and len(df) > self.chunk_size:
                # Чанковая загрузка
                result = self.stream_loader.execute_chunked_load(
                    database=self.starrocks_database,
                    table=self.starrocks_table,
                    df=df,
                    chunk_size=self.chunk_size,
                    column_separator=self.column_separator
                )
            else:
                # Обычная загрузка
                result = self.stream_loader.execute_stream_load(
                    database=self.starrocks_database,
                    table=self.starrocks_table,
                    payload=csv_data,
                    columns=columns,
                    column_separator=self.column_separator
                )

            # 7. Обновляем статус: успешно завершено
            self.update_file_status(
                self.file_id,
                "stage_db_load_completed"
            )

            rows_loaded = result.get('NumberLoadedRows', 0)
            self.log.info(
                f"Загрузка завершена успешно. "
                f"Загружено строк: {rows_loaded}, "
                f"Колонок: {len(columns)}"
            )

            return {
                'success': True,
                'rows_loaded': rows_loaded,
                'rows_filtered': result.get('NumberFilteredRows', 0),
                'columns': columns,
                'chunks_count': result.get('ChunksCount', 1)
            }

        except Exception as e:
            self.log.error(f"Ошибка загрузки данных: {e}", exc_info=True)

            # Обновляем статус: ошибка
            try:
                self.update_file_status(
                    self.file_id,
                    "processing_failed"
                )
            except Exception as status_error:
                self.log.error(
                    f"Ошибка обновления статуса: {status_error}"
                )

            return {
                'success': False,
                'rows_loaded': 0,
                'error': str(e)
            }

        finally:
            # Закрываем подключение
            self.close_starrocks_connection()

    def _get_file_from_s3(self) -> bytes:
        """
        Получает файл из S3.

        Returns:
            bytes: Содержимое файла
        """
        self.log.info(
            f"Получение файла из S3: {self.s3_bucket}/{self.s3_key}"
        )

        hook = S3Hook(aws_conn_id=self.s3_conn_id)
        file_obj = hook.get_key(
            key=self.s3_key,
            bucket_name=self.s3_bucket
        )

        if not file_obj:
            raise FileNotFoundError(
                f"Файл не найден: {self.s3_bucket}/{self.s3_key}"
            )

        file_content = file_obj.get()['Body'].read()
        self.log.info(
            f"Файл получен, размер: {len(file_content)} байт"
        )

        return file_content
