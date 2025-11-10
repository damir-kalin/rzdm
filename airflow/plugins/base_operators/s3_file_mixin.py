"""
Миксин для работы с файлами в S3/MinIO
"""
import io
from typing import Optional

import pandas as pd
from openpyxl import load_workbook


class S3FileMixin:
    """
    Миксин для работы с файлами в S3/MinIO.
    Предоставляет методы для чтения и удаления файлов.
    """

    def get_file_from_s3(self, bucket: str, key: str) -> bytes:
        """
        Получить файл из S3/MinIO

        Args:
            bucket: Имя бакета
            key: Ключ файла

        Returns:
            bytes: Содержимое файла

        Raises:
            FileNotFoundError: Если файл не найден
        """
        hook = self.get_s3_hook()
        obj = hook.get_key(key=key, bucket_name=bucket)
        if not obj:
            raise FileNotFoundError(
                f"Файл не найден: s3://{bucket}/{key}"
            )
        return obj.get()["Body"].read()

    def get_excel_dataframe(
        self,
        bucket: str,
        key: str,
        sheet_name: int = 0
    ) -> pd.DataFrame:
        """
        Получить DataFrame из Excel файла

        Args:
            bucket: Имя бакета
            key: Ключ файла
            sheet_name: Номер листа (по умолчанию 0)

        Returns:
            pd.DataFrame: Данные из Excel
        """
        file_content = self.get_file_from_s3(bucket, key)
        with io.BytesIO(file_content) as buf:
            return pd.read_excel(
                buf,
                sheet_name=sheet_name,
                engine="openpyxl"
            )

    def get_excel_workbook(
        self,
        bucket: str,
        key: str,
        read_only: bool = True
    ):
        """
        Получить openpyxl Workbook

        Args:
            bucket: Имя бакета
            key: Ключ файла
            read_only: Режим только для чтения

        Returns:
            Workbook: openpyxl Workbook объект
        """
        file_content = self.get_file_from_s3(bucket, key)
        with io.BytesIO(file_content) as buf:
            return load_workbook(
                buf,
                read_only=read_only,
                data_only=True
            )

    def delete_file_from_s3(self, bucket: str, key: str) -> bool:
        """
        Удалить файл из S3/MinIO

        Args:
            bucket: Имя бакета
            key: Ключ файла

        Returns:
            bool: True если успешно, False в случае ошибки
        """
        try:
            hook = self.get_s3_hook()
            s3 = hook.get_client_type('s3')
            s3.delete_object(Bucket=bucket, Key=key)
            self.log.info(f"Файл {key} удален из бакета {bucket}")
            return True
        except Exception as e:
            self.log.error(f"Ошибка при удалении файла {key}: {e}")
            return False

    def list_files_in_bucket(self, bucket: str) -> list:
        """
        Получить список файлов в бакете

        Args:
            bucket: Имя бакета

        Returns:
            list: Список ключей файлов
        """
        hook = self.get_s3_hook()
        keys = hook.list_keys(bucket_name=bucket)
        return keys if keys else []
