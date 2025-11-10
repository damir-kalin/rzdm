"""
Оператор для удаления представления в StarRocks
"""
from base_operators.base_hooks_operator import BaseHooksOperator
from starrocks_operators.starrocks_table_manager import StarRocksTableManager


class StarRocksDropViewOperator(BaseHooksOperator):
    """
    Удаляет представление в указанной БД StarRocks
    """

    def __init__(
        self,
        starrocks_database: str,
        starrocks_view: str,
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.starrocks_database = starrocks_database
        self.starrocks_view = starrocks_view

    def execute(self, context):
        manager = StarRocksTableManager(
            mysql_conn_id=self.starrocks_conn_id,
            database=self.starrocks_database,
            logger=self.log,
        )
        manager.drop_view(self.starrocks_view)
        return True
