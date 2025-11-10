"""
Оператор для типизации данных через dbt модели.
"""
from airflow.utils.context import Context

from base_operators.base_hooks_operator import BaseHooksOperator
from dbt_operators.dbt_file_manager import DbtFileManager
from dbt_operators.dbt_runner import DbtRunner
from base_operators.file_status_mixin import FileStatusMixin


class DbtTypingOperator(BaseHooksOperator, FileStatusMixin):
    """
    Оператор для типизации данных через dbt:
    1. Очищает старые файлы моделей
    2. Копирует template модель
    3. Выполняет dbt run с параметрами
    4. Обновляет статус обработки файла
    """

    def __init__(
        self,
        source_schema: str,
        source_table: str,
        destination_schema: str,
        destination_table: str,
        field: str,
        pg_conn_id: str = "airflow_db",
        dbt_project_path: str = '/opt/airflow/dbt/sandbox_dbt_project',
        template_name: str = 'template_tmp',
        debug: bool = True,
        **kwargs
    ):
        """
        Args:
            source_schema: Исходная схема
            source_table: Исходная таблица
            destination_schema: Целевая схема
            destination_table: Целевая таблица
            field: ID файла для обновления статуса
            pg_conn_id: ID подключения PostgreSQL
            dbt_project_path: Путь к dbt проекту
            template_name: Имя template файла
            debug: Режим отладки dbt
        """
        super().__init__(postgres_conn_id=pg_conn_id, **kwargs)
        self.source_schema = source_schema
        self.source_table = source_table
        self.destination_schema = destination_schema
        self.destination_table = destination_table
        self.field = field
        self.pg_conn_id = pg_conn_id
        self.dbt_project_path = dbt_project_path
        self.template_name = template_name
        self.debug = debug

        # Инициализация компонентов
        self.file_manager = DbtFileManager(
            project_path=dbt_project_path,
            logger=self.log
        )

        self.dbt_runner = DbtRunner(
            project_path=dbt_project_path,
            logger=self.log
        )

    def execute(self, context: Context):
        """
        Выполняет типизацию данных через dbt.

        Returns:
            dict: Результат выполнения с метриками
        """
        try:
            self.log.info(
                f"Начало типизации: {self.source_schema}.{self.source_table} "
                f"-> {self.destination_schema}.{self.destination_table}"
            )

            # 1. Обновляем статус: начало загрузки в sandbox
            self.update_file_status(
                self.field,
                'sandbox_db_load'
            )

            # 2. Очищаем старые файлы моделей
            deleted_count = self.file_manager.cleanup_old_files(
                base_name=self.source_table,
                exclude_template=True
            )

            self.log.info(f"Удалено старых файлов: {deleted_count}")

            # 3. Копируем template
            model_file = self.file_manager.copy_template(
                template_name=self.template_name,
                destination_name=self.destination_table
            )

            self.log.info(f"Template скопирован: {model_file.name}")

            # 4. Выполняем dbt run
            dbt_variables = {
                'source_schema': self.source_schema,
                'source_table': self.source_table
            }

            dbt_result = self.dbt_runner.run_model(
                model_name=self.destination_table,
                target=self.destination_schema,
                variables=dbt_variables,
                debug=self.debug
            )

            self.log.info("dbt типизация завершена успешно")

            # 5. Обновляем статус: успешно завершено
            self.update_file_status(
                self.field,
                'processing_completed'
            )

            return {
                'success': True,
                'source_table': f"{self.source_schema}.{self.source_table}",
                'destination_table': (
                    f"{self.destination_schema}.{self.destination_table}"
                ),
                'deleted_old_files': deleted_count,
                'dbt_stdout': dbt_result.get('stdout', '')[:500]
            }

        except Exception as e:
            self.log.error(f"Ошибка типизации данных: {e}", exc_info=True)

            # Обновляем статус: ошибка
            try:
                self.update_file_status(
                    self.field,
                    'processing_failed'
                )
            except Exception as status_error:
                self.log.error(
                    f"Ошибка обновления статуса: {status_error}"
                )

            return {
                'success': False,
                'error': str(e)
            }
