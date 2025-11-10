"""
Оператор для очистки таблиц StarRocks
"""
from base_operators.base_hooks_operator import BaseHooksOperator

from starrocks_operators.starrocks_table_manager import StarRocksTableManager


class StarRocksTruncateOperator(BaseHooksOperator):
    """
    Оператор для очистки таблицы в StarRocks.
    Использует базовый класс для работы с хуками.
    """

    def __init__(
        self,
        starrocks_database: str,
        starrocks_table: str,
        **kwargs
    ):
        """
        Args:
            starrocks_database: Имя базы данных StarRocks
            starrocks_table: Имя таблицы
            status: Статус файла (changed/new)
            **kwargs: Дополнительные параметры для BaseOperator
        """
        super().__init__(**kwargs)
        self.starrocks_database = starrocks_database
        self.starrocks_table = starrocks_table

    def execute(self, context):
        """
        Очищает таблицу в StarRocks если статус = 'changed'

        Returns:
            bool: True если таблица очищена, False если очистка не требуется
        """

        try:
            starrocks_table_manager = StarRocksTableManager(
                starrocks_database=self.starrocks_database,
                starrocks_table=self.starrocks_table
            )
            starrocks_table_manager.truncate_table(self.starrocks_table)
            return True
        except Exception as e:
            self.log.error(f"Ошибка при очистке таблицы {self.starrocks_table}: {e}")
            raise
