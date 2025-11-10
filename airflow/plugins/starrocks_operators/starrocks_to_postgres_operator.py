"""
Оператор для синхронизации данных из StarRocks в PostgreSQL
"""
from typing import Any
import pandas as pd
import time
from base_operators.base_hooks_operator import BaseHooksOperator


class StarRocksToPostgresOperator(BaseHooksOperator):
    """
    Оператор для синхронизации структуры и данных таблиц
    из StarRocks в PostgreSQL.
    Использует базовый класс для работы с хуками.
    """

    # Маппинг типов данных StarRocks -> PostgreSQL
    TYPE_MAPPING = {
        "boolean": "BOOLEAN",
        "bool": "BOOLEAN",
        "smallint": "INT",
        "int2": "INT",
        "integer": "BIGINT",
        "int": "BIGINT",
        "int4": "BIGINT",
        "bigint": "BIGINT",
        "int8": "BIGINT",
        "real": "FLOAT",
        "float4": "FLOAT",
        "double precision": "DOUBLE",
        "float8": "DOUBLE",
        "numeric": "DECIMAL",
        "decimal": "DECIMAL",
        "date": "DATE",
        "timestamp without time zone": "DATETIME",
        "timestamp": "DATETIME",
        "text": "STRING",
        "char": "CHAR",
        "character": "CHAR",
        "varchar": "VARCHAR",
        "character varying": "VARCHAR",
        "json": "JSON",
        "jsonb": "JSON"
    }

    def _normalize_view_sql_for_postgres(self, *, view_name: str, source_database: str, starrocks_create_sql: str) -> str:
        """
        Преобразовать DDL StarRocks (SHOW CREATE VIEW ...) в валидный DDL PostgreSQL.

        Делает следующее:
        - Заменяет обратные кавычки ` на двойные "
        - Отбрасывает список колонок в CREATE VIEW (Postgres позволяет без него)
        - Меняет CREATE VIEW ... AS на CREATE OR REPLACE VIEW public."name" AS ...
        - Заменяет ссылки вида source_db.table на public."table" (мы подключаемся к БД = source_db)
        """
        # Замена обратных кавычек на двойные
        sql = starrocks_create_sql.replace('`', '"')

        # Ищем позицию AS (граница между заголовком и SELECT ...)
        upper_sql = sql.upper()
        as_pos = upper_sql.find(' AS ')
        if as_pos == -1:
            # На всякий случай, если формат иной — создаём простейшую обёртку
            select_sql = sql
        else:
            select_sql = sql[as_pos + 4:].strip().rstrip(';')

        # Подменяем ссылки на исходную БД SR на целевую схему в Postgres
        # Пример: main_bdv."table" -> "<dest_schema>"."table"
        select_sql = select_sql.replace(f'{source_database}.', f'"{self.postgres_schema}".')
        select_sql = select_sql.replace(f'"{source_database}".', f'"{self.postgres_schema}".')

        # Финальный DDL для Postgres
        pg_sql = f'CREATE OR REPLACE VIEW public."{view_name}" AS\n{select_sql};'
        return pg_sql

    def __init__(
        self,
        starrocks_database: str,
        postgres_database: str,
        postgres_schema: str,
        starrocks_conn_id: str = "starrocks_default",
        postgres_conn_id: str = "airflow_db",
        size_batch: int = 50000,
        exclude_views: bool = False,
        **kwargs
    ):
        """
        Args:
            source_databases: Список баз данных StarRocks для синхронизации
            **kwargs: Дополнительные параметры для BaseOperator
        """
        super().__init__(**kwargs)
        self.starrocks_database = starrocks_database
        self.postgres_database = postgres_database
        self.postgres_schema = postgres_schema
        self.starrocks_conn_id = starrocks_conn_id
        self.postgres_conn_id = postgres_conn_id
        self.size_batch = size_batch
        self.exclude_views = exclude_views

    def _map_type(self, sr_type: str) -> str:
        """
        Преобразовать тип данных StarRocks в тип PostgreSQL

        Args:
            sr_type: Тип данных StarRocks

        Returns:
            str: Соответствующий тип PostgreSQL
        """
        base_type = sr_type.split('(')[0].strip().lower()
        mapped_type = self.TYPE_MAPPING.get(base_type, 'VARCHAR(255)')

        # Если тип содержит размер в скобках, сохраняем его
        if any(c in sr_type for c in "()") and mapped_type != 'VARCHAR(255)':
            size = sr_type.split('(')[1].split(')')[0]
            return f"{mapped_type}({size})"

        return mapped_type

    def _get_starrocks_tables(self) -> pd.DataFrame:
        """
        Получить список таблиц из StarRocks

        Returns:
            pd.DataFrame: DataFrame с таблицами и их базами данных
        """
        sr_hook = self.get_starrocks_hook()
        conn = sr_hook.get_conn()
        all_tables = pd.DataFrame(columns=["table_name", "database"])

        self.log.info(f"Получение объектов из базы {self.starrocks_database}")
        with conn.cursor() as cursor:
            cursor.execute(
                """
                SELECT TABLE_NAME, TABLE_TYPE
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_SCHEMA = %s;
                """,
                (self.starrocks_database,)
            )
            result = cursor.fetchall()
            if result:
                df = pd.DataFrame(result, columns=["table_name", "table_type"])
                df['database'] = self.starrocks_database
                tables = df[df['table_type'] == 'BASE TABLE'].copy()[['table_name', 'database']]
                views = df[df['table_type'] == 'VIEW'].copy()[['table_name', 'database']]
            else:
                tables = pd.DataFrame(columns=["table_name", "database"]) 
                views = pd.DataFrame(columns=["table_name", "database"]) 

        # Финальная гарантия наличия нужных колонок
        for df in (tables, views):
            for col in ("table_name", "database"):
                if col not in df.columns:
                    df[col] = pd.Series(dtype=str)

        return tables.reset_index(drop=True), views.reset_index(drop=True)

    def _get_postgres_tables(self, schema_name: str) -> pd.DataFrame:
        """
        Получить список существующих таблиц из PostgreSQL

        Args:
            database_name: Имя базы данных

        Returns:
            pd.DataFrame: DataFrame с именами таблиц
        """
        pg_hook = self.get_postgres_hook(database=self.postgres_database)
        conn = pg_hook.get_conn()

        sql = """
            SELECT tablename
            FROM pg_catalog.pg_tables
            WHERE schemaname = %s;
        """

        with conn.cursor() as cursor:
            cursor.execute(sql, (schema_name,))
            result = cursor.fetchall()
            return pd.DataFrame(result, columns=['table_name'])

    def _drop_missing_postgres_tables(
        self,
        schema_name: str,
        sr_tables_for_db: pd.Series,
        pg_tables_df: pd.DataFrame
    ) -> int:
        """
        Удалить из PostgreSQL таблицы, которых больше нет в StarRocks

        Args:
            schema_name: Имя БД в Postgres (sandbox_db_*)
            sr_tables_for_db: Series с именами таблиц, существующих в StarRocks
            pg_tables_df: DataFrame с колонкой table_name из Postgres

        Returns:
            int: Количество удалённых таблиц в Postgres
        """
        if pg_tables_df.empty:
            return 0

        sr_set = set(sr_tables_for_db.tolist())
        pg_set = set(pg_tables_df['table_name'].tolist())

        to_drop = sorted(list(pg_set - sr_set))
        if not to_drop:
            return 0

        self.log.info(
            "В Postgres(%s) найдено %d устаревших таблиц: %s",
            schema_name,
            len(to_drop),
            to_drop,
        )

        pg_hook = self.get_postgres_hook(database=self.postgres_database)
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cur:
                for table_name in to_drop:
                    cur.execute(
                        f'DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;'
                    )
            conn.commit()

        return len(to_drop)

    def _get_table_structure(
        self,
        database: str,
        table_name: str
    ) -> pd.DataFrame:
        """
        Получить структуру таблицы из StarRocks

        Args:
            database: Имя базы данных
            table_name: Имя таблицы

        Returns:
            pd.DataFrame: DataFrame со структурой таблицы
        """
        sr_hook = self.get_starrocks_hook()
        conn = sr_hook.get_conn()

        with conn.cursor() as cursor:
            cursor.execute(
                f"SHOW FULL COLUMNS FROM {database}.{table_name};"
            )
            result = cursor.fetchall()

            columns_df = pd.DataFrame(
                result,
                columns=[
                    'Field', 'Type', 'Collation', 'Null', 'Key',
                    'Default', 'Extra', 'Privileges', 'Comment'
                ]
            )

            # Преобразуем типы
            columns_df['TypePostgres'] = (
                columns_df['Type'].apply(self._map_type)
            )

            return columns_df
    
    def _get_views_from_starrocks(
            self,
            database: str,
            view_name: str,
    ) -> pd.DataFrame | None:
        """
        Получить список представлений из StarRocks

        Returns:
            pd.DataFrame: DataFrame с представлениями
        """
        sr_hook = self.get_starrocks_hook()
        conn = sr_hook.get_conn()
        try:
            with conn.cursor() as cursor:
                cursor.execute(f"SHOW CREATE VIEW {database}.{view_name};")
                result = cursor.fetchall()

                if not result:
                    return None

                row = result[0]
                # Попробуем получить реальные имена колонок из cursor.description
                col_names = [d[0] for d in (cursor.description or [])]

                # Приводим к DataFrame c фактическим числом колонок
                if col_names and len(col_names) == len(row):
                    df_raw = pd.DataFrame(result, columns=col_names)
                else:
                    df_raw = pd.DataFrame(result, columns=[f"col_{i}" for i in range(len(row))])

                # Нормализуем к ожидаемому формату: колонки 'View' и 'Create View'
                if 'Create View' in df_raw.columns:
                    create_view_sql = df_raw['Create View'].iloc[0]
                else:
                    # Берём последнюю колонку как DDL
                    create_view_sql = df_raw.iloc[0, -1]

                return pd.DataFrame(
                    {
                        'View': [f'{database}.{view_name}'],
                        'Create View': [create_view_sql],
                    }
                )
        except Exception as e:
            self.log.error(f"Ошибка получения представления {view_name}: {e}")
            return None

    def _create_table_in_postgres(
        self,
        schema_name: str,
        table_name: str,
        columns_df: pd.DataFrame
    ):
        """
        Создать таблицу в PostgreSQL

        Args:
            schema_name: Имя базы данных (используется вместо схемы)
            table_name: Имя таблицы
            columns_df: DataFrame со структурой таблицы
        """
        pg_hook = self.get_postgres_hook(database=self.postgres_database)
        conn = pg_hook.get_conn()

        # Гарантируем наличие схемы
        with conn.cursor() as cursor:
            cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')

        # Формируем определения колонок с экранированием
        columns_def = ', '.join([
            f'"{row["Field"]}" {row["TypePostgres"]}'
            for _, row in columns_df.iterrows()
        ])

        # Определяем пользователя на основе имени БД
        # sandbox_db_hr -> hr_user; для main_* используем airflow
        if schema_name.startswith('sandbox_db_'):
            db_suffix = schema_name.replace('sandbox_db_', '')
            target_user = f'{db_suffix}_user'
        else:
            target_user = 'airflow'

        create_table_sql = f"""
                   DROP TABLE IF EXISTS "{schema_name}"."{table_name}" CASCADE;
            CREATE TABLE IF NOT EXISTS "{schema_name}"."{table_name}" (
                {columns_def}
            );
            GRANT ALL PRIVILEGES ON TABLE "{schema_name}"."{table_name}"
                TO {target_user};
        """

        with conn.cursor() as cursor:
            cursor.execute(create_table_sql)
        conn.commit()

        self.log.info(f"Таблица {schema_name}.{table_name} создана в БД {self.postgres_database}. Права предоставлены {target_user}.")
    
    def _create_view_in_postgres(
        self,
        schema_name: str,
        view_name: str,
        view_df: pd.DataFrame,
        source_database: str,
    ):
        """
        Создать представление в PostgreSQL
        """
        try:
            pg_hook = self.get_postgres_hook(database=self.postgres_database)
            conn = pg_hook.get_conn()

            with conn.cursor() as cursor:
                cursor.execute(f'CREATE SCHEMA IF NOT EXISTS "{schema_name}";')
                cursor.execute(f'DROP VIEW IF EXISTS "{schema_name}"."{view_name}" CASCADE;')
                raw_sql = str(view_df['Create View'].iloc[0])
                pg_sql = self._normalize_view_sql_for_postgres(
                    view_name=view_name,
                    source_database=source_database,
                    starrocks_create_sql=raw_sql,
                )
                # Переносим VIEW в нужную схему
                # Заменяем CREATE OR REPLACE VIEW public."name" на целевую схему
                pg_sql = pg_sql.replace('CREATE OR REPLACE VIEW public', f'CREATE OR REPLACE VIEW "{schema_name}"')
                cursor.execute(pg_sql)
            conn.commit()

            self.log.info(f"Представление {view_name} создано в схеме {schema_name} базы {self.postgres_database}.")

            return True
        except Exception as e:
            self.log.error(f"Ошибка создания представления {view_name}: {e}")
            import traceback
            self.log.error(traceback.format_exc())
            return False

    def _load_data_to_postgres(
        self,
        source_database: str,
        table_name: str,
        schema_name: str
    ) -> int:
        """
        Загрузить данные из StarRocks в PostgreSQL

        Args:
            source_database: База данных источник (StarRocks)
            table_name: Имя таблицы
            schema_name: Схема назначения (PostgreSQL)

        Returns:
            int: Количество загруженных строк
        """
        # Получаем данные из StarRocks
        sr_hook = self.get_starrocks_hook()
        sr_conn = sr_hook.get_conn()

        with sr_conn.cursor() as cursor:
            cursor.execute(f"SELECT * FROM {source_database}.{table_name};")
            result = cursor.fetchall()

            if not result:
                self.log.info(f"Таблица {table_name} пуста")
                return 0

            column_names = [col[0] for col in cursor.description]
            data = pd.DataFrame(result, columns=column_names)

        # Загружаем данные в PostgreSQL
        pg_hook = self.get_postgres_hook(database=self.postgres_database)
        pg_conn = pg_hook.get_conn()

        # Экранируем '%', чтобы не ломать плейсхолдеры %s
        columns_list_items = []
        for col in data.columns:
            columns_list_items.append(
                f'"{str(col).replace('%', '%%')}"'
            )
        columns_list = ', '.join(columns_list_items)
        placeholders = ', '.join(['%s'] * len(data.columns))

        insert_sql = f"""
            INSERT INTO "{schema_name}"."{table_name}" ({columns_list})
            VALUES ({placeholders})
        """
        placeholders_count = insert_sql.count('%s')

        # Конвертируем данные для PostgreSQL
        data_cleaned = data.replace({pd.NA: None, pd.NaT: None})
        data_cleaned = data_cleaned.where(pd.notna(data_cleaned), None)

        def convert_value(val):
            """Конвертировать значение в формат PostgreSQL"""
            if pd.isna(val) or val is pd.NA or val is pd.NaT:
                return None
            if hasattr(val, 'item'):
                return val.item()
            return val

        data_tuples = [
            tuple[Any | None, ...](convert_value(val) for val in row)
            for row in data_cleaned.values
        ]

        # Валидация: длина кортежей должна равняться числу колонок
        expected_len = len(data.columns)
        bad_indices: list[int] = []
        for idx, tuple_values in enumerate(data_tuples):
            if len(tuple_values) != expected_len:
                bad_indices.append(idx)
        if bad_indices:
            sample_idx = bad_indices[0]
            sample_tuple = data_tuples[sample_idx]
            raise ValueError(
                "Несоответствие числа параметров вставки количеству колонок: "
                f"ожидалось {expected_len}, получено {len(sample_tuple)} "
                f"в строке {sample_idx}. Колонки: {list(data.columns)}"
            )

        start_time = time.time()

        for i in range(0, len(data_tuples), self.size_batch):
            data_batch = data_tuples[i:i + self.size_batch]
            try:
                with pg_conn.cursor() as cursor:
                    cursor.executemany(insert_sql, data_batch)
            except Exception as e:
                # Диагностика проблем с маппингом параметров/плейсхолдеров
                self.log.error(
                    "Ошибка вставки батча: %s. Пример строки (первые 3): %s",
                    str(e), data_batch[:3]
                )
                self.log.error(
                    "SQL: %s | Колонки (%d): %s",
                    insert_sql, len(data.columns), list(data.columns)
                )
                self.log.error(
                    "Число плейсхолдеров %%s: %d",
                    placeholders_count
                )
                # Поиск конкретной проблемной строки
                with pg_conn.cursor() as cursor:
                    for rel_idx, row_params in enumerate(data_batch):
                        if len(row_params) != placeholders_count:
                            self.log.error(
                                "Строка %d в батче имеет %d параметров "
                                "вместо %d",
                                rel_idx,
                                len(row_params),
                                placeholders_count,
                            )
                            break
                        try:
                            cursor.execute(insert_sql, row_params)
                        except Exception as row_err:
                            self.log.error(
                                "Ошибка на строке батча %d: %s | Данные: %r",
                                rel_idx, str(row_err), row_params
                            )
                            break
                raise
            pg_conn.commit()
            self.log.info(f"Загружено строк: {len(data_batch)}")
            remaining = max(0, len(data_tuples) - (i + self.size_batch))
            self.log.info(f"Осталось строк: {remaining}")
            self.log.info(
                "Прогресс: %.2f%%",
                (
                    i / len(data_tuples) * 100
                    if len(data_tuples)
                    else 100.0
                ),
            )
            self.log.info(
                "Время: %.2f секунд",
                time.time() - start_time,
            )
            self.log.info(
                "Скорость загрузки: %.2f строк/сек",
                (len(data_batch) / (time.time() - start_time))
                if (time.time() - start_time)
                else 0.0,
            )
            start_time = time.time()
            time.sleep(0.1)
            self.log.info(
                "Время ожидания: %.2f сек",
                time.time() - start_time,
            )
            start_time = time.time()

        return len(data_tuples)

    def execute(self, context):
        """
        Выполнить синхронизацию данных из StarRocks в PostgreSQL

        Returns:
            dict: Статистика синхронизации
        """
        self.log.info("Начало синхронизации данных StarRocks -> PostgreSQL")

        # Получаем список таблиц из StarRocks
        sr_tables, sr_views = self._get_starrocks_tables()

        self.log.info(
            f"Найдено таблиц в StarRocks: {len(sr_tables)}, "
            f"представлений: {len(sr_views)}"
        )

        # Синхронизируем ВСЕ таблицы из StarRocks
        # (и новые, и существующие - для обновления данных)
        tables_to_sync = sr_tables
        tables_count = len(tables_to_sync)

        self.log.info(
            f"Всего таблиц для синхронизации: {tables_count}"
        )

        total_rows = 0
        total_dropped_pg = 0
        total_views_synced = 0

        # Удаляем в Postgres таблицы, которых уже нет в StarRocks
        if not sr_tables.empty:
            sr_subset = sr_tables[sr_tables['database'] == self.starrocks_database]
            pg_tables = self._get_postgres_tables(self.postgres_schema)
            dropped = self._drop_missing_postgres_tables(
                schema_name=self.postgres_schema,
                sr_tables_for_db=(
                    sr_subset['table_name'] if not sr_subset.empty else pd.Series([], dtype=str)
                ),
                pg_tables_df=pg_tables,
            )
            total_dropped_pg += dropped

        # Синхронизируем каждую таблицу
        for index, row in tables_to_sync.iterrows():
            table_name = row['table_name']
            database = row['database']
            schema_name = self.postgres_schema

            self.log.info(
                "Синхронизация таблицы %d/%d: %s.%s",
                index + 1,
                tables_count,
                database,
                table_name,
            )

            # Получаем структуру таблицы
            columns_df = self._get_table_structure(database, table_name)

            # Создаем таблицу в PostgreSQL
            self._create_table_in_postgres(schema_name, table_name, columns_df)

            # Загружаем данные
            rows_loaded = self._load_data_to_postgres(
                database,
                table_name,
                schema_name
            )

            total_rows += rows_loaded
            self.log.info(f"Загружено строк: {rows_loaded}")
        
        # Синхронизируем представления из StarRocks (если не отключено)
        if not self.exclude_views:
            self.log.info(f"Начало синхронизации представлений из {self.starrocks_database} в {self.postgres_database}.{self.postgres_schema}")
            for index, row in sr_views.iterrows():
                view_name = row['table_name']
                database = row['database']
                schema_name = self.postgres_schema
                
                self.log.info(
                    "Синхронизация представления %d/%d: %s.%s -> %s.%s",
                    index + 1,
                    len(sr_views),
                    database,
                    view_name,
                    self.postgres_database,
                    schema_name,
                )
                
                view_df = self._get_views_from_starrocks(database, view_name)
                if view_df is None:
                    self.log.error(
                        f"Представление {view_name} не найдено в StarRocks. Пропускаем."
                    )
                    continue
                created = self._create_view_in_postgres(
                    schema_name=schema_name,
                    view_name=view_name,
                    view_df=view_df,
                    source_database=database,
                )
                if created:
                    total_views_synced += 1
                else:
                    self.log.error(
                        f"Представление {view_name} не удалось создать в БД {self.postgres_database}, схема {schema_name}. "
                        "Проверьте логи выше для деталей."
                    )
        else:
            self.log.info("Синхронизация представлений отключена (exclude_views=True)")

        self.log.info(
            "Синхронизация завершена. Таблиц: %d, Представлений: %d, Строк: %d, "
            "Удалено таблиц в Postgres: %d",
            tables_count,
            total_views_synced,
            total_rows,
            total_dropped_pg,
        )

        return {
            'status': 'success',
            'tables_synced': tables_count,
            'views_synced': total_views_synced,
            'rows_loaded': total_rows,
            'tables_dropped_in_postgres': total_dropped_pg,
        }
        
        