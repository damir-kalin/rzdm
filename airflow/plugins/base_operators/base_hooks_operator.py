"""
Базовый класс для операторов с переиспользуемыми методами работы с хуками
"""
from typing import Optional

from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.providers.mysql.hooks.mysql import MySqlHook


class BaseHooksOperator(BaseOperator):
    """
    Базовый класс с переиспользуемыми методами для работы с хуками.
    Предоставляет централизованный доступ к S3, PostgreSQL и StarRocks.
    """

    def __init__(
        self,
        s3_conn_id: str = "minio_default",
        postgres_conn_id: str = "airflow_db",
        starrocks_conn_id: str = "starrocks_default",
        *args,
        **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.starrocks_conn_id = starrocks_conn_id

    def get_s3_hook(self) -> S3Hook:
        """Получить S3Hook для работы с MinIO/S3"""
        return S3Hook(aws_conn_id=self.s3_conn_id)

    def get_postgres_hook(self, database: str = "service_db") -> PostgresHook:
        """Получить PostgresHook для работы с PostgreSQL"""
        return PostgresHook(
            postgres_conn_id=self.postgres_conn_id,
            database=database
        )

    def get_starrocks_hook(self) -> MySqlHook:
        """Получить MySqlHook для работы с StarRocks"""
        return MySqlHook(mysql_conn_id=self.starrocks_conn_id)
