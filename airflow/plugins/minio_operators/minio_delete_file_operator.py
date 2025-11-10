"""
Оператор для удаления файлов из MinIO
"""
from base_operators.base_hooks_operator import BaseHooksOperator
from base_operators.s3_file_mixin import S3FileMixin
from base_operators.file_status_mixin import FileStatusMixin


class MinioDeleteFileOperator(
    BaseHooksOperator,
    S3FileMixin,
    FileStatusMixin
):
    """
    Оператор для удаления файла из бакета MinIO.
    Использует миксины для переиспользования кода.
    """

    def __init__(
        self,
        s3_bucket: str,
        file_name: str,
        status: str,
        **kwargs
    ):
        """
        Args:
            s3_bucket: Имя бакета
            file_name: Имя файла
            status: Статус файла (changed/new)
            **kwargs: Дополнительные параметры для BaseOperator
        """
        super().__init__(**kwargs)
        self.s3_bucket = s3_bucket
        self.file_name = file_name
        self.status = status

    def execute(self, context):
        """
        Удаляет старый файл из MinIO если статус = 'changed'

        Returns:
            bool: True если файл удален, False если удаление не требуется
        """
        if self.status != "changed":
            self.log.info(
                f"Статус не требует удаления файла: {self.status}"
            )
            return False

        # Получаем ключ старого файла из базы данных
        old_key = self.get_file_key_from_db(self.file_name, self.s3_bucket)

        if not old_key:
            self.log.warning(
                f"Старый ключ не найден для файла {self.file_name}"
            )
            return False

        self.log.info(
            f"Найден старый ключ: {old_key}. Удаляем файл..."
        )

        # Удаляем файл из S3
        return self.delete_file_from_s3(self.s3_bucket, old_key)
