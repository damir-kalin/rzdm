"""
Оператор для проверки множественных листов в Excel
"""
from base_operators.base_hooks_operator import BaseHooksOperator
from base_operators.s3_file_mixin import S3FileMixin
from base_operators.file_status_mixin import FileStatusMixin
from base_operators.file_validation_mixin import FileValidationMixin


class CheckMultipleSheetsOperator(
    BaseHooksOperator,
    S3FileMixin,
    FileStatusMixin,
    FileValidationMixin
):
    """
    Оператор для проверки множественных листов в Excel файлах.
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
        Проверяет, содержит ли Excel-файл больше одного листа

        Returns:
            bool: True если проверка прошла успешно
        """
        self.log.info(
            f"Проверка количества листов в файле "
            f"{self.key} из бакета {self.bucket_name}"
        )

        # Получаем Excel workbook
        workbook = self.get_excel_workbook(self.bucket_name, self.key)

        # Проверяем наличие множественных листов
        if self.check_multiple_sheets(workbook):
            # Обновляем статус в базе данных
            self.update_file_status(
                self.field,
                'more_than_one_list_detected'
            )
            self.log.warning(
                f"Файл содержит больше одного листа: "
                f"{workbook.sheetnames}"
            )
        else:
            self.log.info(
                f"Файл содержит один лист: '{workbook.sheetnames[0]}'"
            )

        return True
