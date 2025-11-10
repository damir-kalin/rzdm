"""
Класс для управления таблицами в StarRocks.
"""
import logging
import pandas as pd
from typing import List
from airflow.providers.mysql.hooks.mysql import MySqlHook


class StarRocksTableManager:
    """
    Менеджер для создания и управления таблицами в StarRocks.
    """

    def __init__(
        self,
        mysql_conn_id: str,
        database: str,
        logger: logging.Logger = None
    ):
        """
        Args:
            mysql_conn_id: ID подключения MySQL/StarRocks в Airflow
            database: Имя базы данных
            logger: Логгер для вывода сообщений
        """
        self.mysql_conn_id = mysql_conn_id
        self.database = database
        self.log = logger or logging.getLogger(__name__)

    def create_table_if_not_exists(
        self,
        table_name: str,
        columns: List[str],
        column_type: str = "VARCHAR(65333)"
    ):
        """
        Создает таблицу, если она не существует.

        Args:
            table_name: Имя таблицы
            columns: Список имен колонок
            column_type: Тип данных для всех колонок
        """
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            full_table_name = f"{self.database}.{table_name}"

            # Экранируем имена колонок
            columns_def = ', '.join([
                f"`{col.replace('`', '``')}` {column_type}"
                for col in columns
            ])

            sql = f"""
                CREATE TABLE IF NOT EXISTS {full_table_name} (
                    {columns_def}
                )
            """

            self.log.info(
                f"Создание таблицы {full_table_name} "
                f"с {len(columns)} колонками"
            )

            cur.execute(sql)
            conn.commit()

            self.log.info(f"Таблица {full_table_name} успешно создана")

        except Exception as e:
            conn.rollback()
            self.log.error(
                f"Ошибка создания таблицы {table_name}: {e}"
            )
            raise
        finally:
            cur.close()
            conn.close()
    
    def drop_table(self, table_name: str):
        """
        Удаляет таблицу.

        Args:
            table_name: Имя таблицы
        """
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            full_table_name = f"{self.database}.{table_name}"
            sql = f"DROP TABLE IF EXISTS {full_table_name}"
            cur.execute(sql)
            conn.commit()
            self.log.info(f"Таблица {full_table_name} успешно удалена")
        except Exception as e:
            conn.rollback()
            self.log.error(f"Ошибка удаления таблицы {table_name}: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def truncate_table(self, table_name: str):
        """
        Очищает таблицу.

        Args:
            table_name: Имя таблицы
        """
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            full_table_name = f"{self.database}.{table_name}"
            sql = f"TRUNCATE TABLE {full_table_name}"
            cur.execute(sql)
            conn.commit()
            self.log.info(f"Таблица {full_table_name} успешно очищена")
        except Exception as e:
            conn.rollback()
            self.log.error(f"Ошибка очистки таблицы {table_name}: {e}")
            raise
        finally:
            cur.close()
            conn.close()
    
    def drop_view(self, view_name: str):
        """
        Удаляет представление.

        Args:
            view_name: Имя представления
        """
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            full_view_name = f"{self.database}.{view_name}"
            sql = f"DROP VIEW IF EXISTS {full_view_name}"
            cur.execute(sql)
            conn.commit()
            self.log.info(f"Представление {full_view_name} успешно удалено")
        except Exception as e:
            conn.rollback()
            self.log.error(f"Ошибка удаления представления {view_name}: {e}")
            raise
        finally:
            cur.close()
            conn.close()

    def get_columns_from_dataframe(
        self,
        df: pd.DataFrame,
        exclude_unnamed: bool = True
    ) -> List[str]:
        """
        Извлекает список имен колонок из DataFrame.

        Args:
            df: DataFrame с данными
            exclude_unnamed: Исключить колонки типа 'Unnamed: N'

        Returns:
            List[str]: Список имен колонок
        """
        if df.empty:
            self.log.warning("DataFrame пустой, колонки не найдены")
            return []

        columns = df.columns.tolist()

        if exclude_unnamed:
            # Фильтруем колонки вида 'Unnamed: N'
            columns = [
                col for col in columns
                if not str(col).startswith('Unnamed:')
            ]

        self.log.info(f"Найдено {len(columns)} колонок")

        return columns

    def table_exists(self, table_name: str) -> bool:
        """
        Проверяет существование таблицы.

        Args:
            table_name: Имя таблицы

        Returns:
            bool: True если таблица существует
        """
        hook = MySqlHook(mysql_conn_id=self.mysql_conn_id)
        conn = hook.get_conn()
        cur = conn.cursor()

        try:
            sql = """
                SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = %s AND table_name = %s
            """
            cur.execute(sql, (self.database, table_name))
            result = cur.fetchone()
            return result[0] > 0

        except Exception as e:
            self.log.error(
                f"Ошибка проверки существования таблицы {table_name}: {e}"
            )
            return False
        finally:
            cur.close()
            conn.close()
