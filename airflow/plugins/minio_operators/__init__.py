"""
MinIO операторы для Airflow.
"""
from minio_operators.minio_file_sensor import MinioFileSensor
from minio_operators.minio_delete_file_operator import MinioDeleteFileOperator

__all__ = [
    'MinioFileSensor',
    'MinioDeleteFileOperator',
]
