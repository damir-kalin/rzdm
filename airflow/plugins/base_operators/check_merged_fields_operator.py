"""
Оператор для проверки объединенных ячеек в Excel файлах
"""
from base_operators.base_hooks_operator import BaseHooksOperator
from base_operators.s3_file_mixin import S3FileMixin
from base_operators.file_status_mixin import FileStatusMixin
from base_operators.file_validation_mixin import FileValidationMixin


class CheckMergedFieldsOperator(
    BaseHooksOperator,
    S3FileMixin,
    FileStatusMixin,
    FileValidationMixin
):
    """
    Оператор для проверки объединенных ячеек в Excel файлах.
    Использует миксины для переиспользования кода.
    """

    def __init__(
        self,
        bucket_name: str,
        key: str,
        field: str,
        **kwargs
    ):
        """
        Args:
            bucket_name: Имя бакета в S3/MinIO
            key: Ключ файла
            field: ID файла (GUID)
            **kwargs: Дополнительные параметры для BaseOperator
        """
        super().__init__(**kwargs)
        self.bucket_name = bucket_name
        self.key = key
        self.field = field

    def execute(self, context):
        """
        Проверяет наличие объединённых ячеек на первом листе Excel файла

        Raises:
            ValueError: Если найдены объединённые ячейки
        """
        self.log.info(
            f"Проверка объединенных ячеек в файле "
            f"{self.key} из бакета {self.bucket_name}"
        )

        # Получаем Excel workbook
        workbook = self.get_excel_workbook(
            self.bucket_name,
            self.key,
            read_only=False
        )

        # Проверяем наличие объединенных ячеек
        if self.check_merged_cells(workbook):
            # Обновляем статус в базе данных
            self.update_file_status(
                self.field,
                'merged_cells_detected'
            )
            raise ValueError(
                f"Объединённые ячейки найдены в файле {self.key}"
            )

        self.log.info("Объединённых ячеек не найдено")
        return True
