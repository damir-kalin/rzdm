"""
Миксин для валидации файлов
"""
import re
from typing import Tuple


class FileValidationMixin:
    """
    Миксин для валидации файлов.
    Предоставляет методы для проверки формата, парсинга имен и валидации.
    """

    @staticmethod
    def validate_file_name_format(file_name: str) -> bool:
        """
        Проверка формата имени файла

        Args:
            file_name: Имя файла для проверки

        Returns:
            bool: True если формат корректный
        """
        pattern = (r'^[a-zA-Zа-яА-ЯёЁ0-9\._ -]+_--_[a-zA-Z0-9\._-]+'
                   r'_--_[a-zA-Z0-9-]{36}\.[a-z]+$')
        return bool(re.match(pattern, file_name))

    @staticmethod
    def parse_file_name(key: str) -> Tuple[str, str, str]:
        """
        Разбор имени файла на компоненты

        Ожидаемый формат: file_name_--_user_name_--_guid.ext

        Args:
            key: Ключ файла (имя в S3)

        Returns:
            Tuple[str, str, str]: (file_name, user_name, guid)
        """
        parts = key.split("_--_")
        if (len(parts) == 3 and
                FileValidationMixin.validate_file_name_format(key)):
            file_name = parts[-3] + '.' + parts[-1].split(".")[-1]
            user_name = parts[-2]
            guid = parts[-1].split(".")[0]
        else:
            # Если формат не соответствует, возвращаем key как file_name
            file_name = key
            user_name = "unknown"
            guid = "unknown"
        return file_name, user_name, guid

    def check_merged_cells(self, workbook) -> bool:
        """
        Проверка наличия объединенных ячеек на первом листе

        Args:
            workbook: openpyxl Workbook объект

        Returns:
            bool: True если найдены объединенные ячейки
        """
        first_sheet = workbook.sheetnames[0]
        ws = workbook[first_sheet]
        merged_ranges = ws.merged_cells.ranges

        if merged_ranges:
            self.log.error(
                f"Объединённые ячейки найдены на листе '{first_sheet}'"
            )
            return True
        return False

    def check_multiple_sheets(self, workbook) -> bool:
        """
        Проверка наличия множественных листов

        Args:
            workbook: openpyxl Workbook объект

        Returns:
            bool: True если более одного листа
        """
        sheet_count = len(workbook.sheetnames)
        if sheet_count > 1:
            self.log.warning(
                f"Файл содержит больше одного листа: "
                f"{workbook.sheetnames}"
            )
            return True
        return False

    @staticmethod
    def is_excel_file(file_name: str) -> bool:
        """
        Проверка, является ли файл Excel

        Args:
            file_name: Имя файла

        Returns:
            bool: True если это Excel файл
        """
        return file_name.lower().endswith(('.xlsx', '.xls'))
