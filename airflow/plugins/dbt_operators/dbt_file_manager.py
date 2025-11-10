"""
Менеджер для работы с dbt файлами.
"""
import logging
from pathlib import Path
from typing import Optional


class DbtFileManager:
    """
    Управляет файлами dbt моделей: очистка, копирование, создание.
    """

    def __init__(
        self,
        project_path: str,
        models_path: str = "models/staging",
        logger: Optional[logging.Logger] = None
    ):
        """
        Args:
            project_path: Путь к dbt проекту
            models_path: Относительный путь к папке с моделями
            logger: Логгер для вывода сообщений
        """
        self.project_path = Path(project_path)
        self.models_path = self.project_path / models_path
        self.log = logger or logging.getLogger(__name__)

    def cleanup_old_files(
        self,
        base_name: str,
        exclude_template: bool = True
    ) -> int:
        """
        Удаляет старые SQL файлы по базовому имени.

        Args:
            base_name: Базовое имя файла для поиска
            exclude_template: Исключить template файлы

        Returns:
            int: Количество удаленных файлов
        """
        # Обработка имени файла с скобками
        search_name = self._extract_core_name(base_name)

        pattern = f"{search_name}*.sql"
        if exclude_template:
            exclude_pattern = "template_tmp.sql"
        else:
            exclude_pattern = None

        deleted_count = 0

        try:
            for file_path in self.models_path.glob(pattern):
                if exclude_pattern and file_path.name == exclude_pattern:
                    continue

                file_path.unlink()
                self.log.info(f"Удален старый файл: {file_path.name}")
                deleted_count += 1

        except Exception as e:
            self.log.warning(f"Ошибка при очистке файлов: {e}")

        self.log.info(
            f"Очистка завершена. Удалено файлов: {deleted_count}"
        )
        return deleted_count

    def copy_template(
        self,
        template_name: str,
        destination_name: str
    ) -> Path:
        """
        Копирует template файл с новым именем.

        Args:
            template_name: Имя template файла
            destination_name: Имя нового файла

        Returns:
            Path: Путь к созданному файлу
        """
        template_path = self.models_path / f"{template_name}.sql"
        destination_path = self.models_path / f"{destination_name}.sql"

        if not template_path.exists():
            raise FileNotFoundError(
                f"Template файл не найден: {template_path}"
            )

        try:
            destination_path.write_text(
                template_path.read_text(encoding='utf-8'),
                encoding='utf-8'
            )
            self.log.info(
                f"Template скопирован: {template_name} -> {destination_name}"
            )
            return destination_path

        except Exception as e:
            self.log.error(f"Ошибка копирования template: {e}")
            raise

    def file_exists(self, file_name: str) -> bool:
        """
        Проверяет существование файла модели.

        Args:
            file_name: Имя файла (без расширения)

        Returns:
            bool: True если файл существует
        """
        file_path = self.models_path / f"{file_name}.sql"
        return file_path.exists()

    def _extract_core_name(self, base_name: str) -> str:
        """
        Извлекает базовое имя без суффиксов типа _(2) или (2).

        Args:
            base_name: Исходное имя

        Returns:
            str: Базовое имя без суффиксов
        """
        # Удаляем _tmp суффикс
        name = base_name.replace('_tmp', '')

        # Удаляем (N) или _(N) в конце
        if name.endswith(')'):
            paren_pos = name.rfind('(')
            if paren_pos > 0:
                name = name[:paren_pos].rstrip('_ ')

        return name
