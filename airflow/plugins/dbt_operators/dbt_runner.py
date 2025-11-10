"""
Класс для выполнения dbt команд.
"""
import logging
import subprocess
from pathlib import Path
from typing import Dict, Any, Optional


class DbtRunner:
    """
    Выполняет dbt команды с настройкой параметров и переменных.
    """

    def __init__(
        self,
        project_path: str,
        logger: Optional[logging.Logger] = None
    ):
        """
        Args:
            project_path: Путь к dbt проекту
            logger: Логгер для вывода сообщений
        """
        self.project_path = Path(project_path)
        self.log = logger or logging.getLogger(__name__)

    def run_model(
        self,
        model_name: str,
        target: str,
        variables: Optional[Dict[str, str]] = None,
        debug: bool = True
    ) -> Dict[str, Any]:
        """
        Выполняет dbt run для указанной модели.

        Args:
            model_name: Имя модели для выполнения
            target: Целевая схема/профиль
            variables: Переменные для передачи в dbt
            debug: Включить debug режим

        Returns:
            Dict[str, Any]: Результат выполнения
        """
        cmd = self._build_run_command(
            model_name=model_name,
            target=target,
            variables=variables,
            debug=debug
        )

        self.log.info(f"Выполнение dbt команды: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.project_path),
                capture_output=True,
                text=True,
                check=True
            )

            self.log.info("dbt run успешно выполнен")
            self.log.debug(f"stdout: {result.stdout}")

            return {
                'success': True,
                'stdout': result.stdout,
                'stderr': result.stderr,
                'returncode': result.returncode
            }

        except subprocess.CalledProcessError as e:
            self.log.error(f"Ошибка выполнения dbt: {e}")
            self.log.error(f"stdout: {e.stdout}")
            self.log.error(f"stderr: {e.stderr}")

            raise RuntimeError(
                f"Ошибка выполнения dbt для модели {model_name}: {e.stderr}"
            ) from e

    def test_model(
        self,
        model_name: str,
        target: str
    ) -> Dict[str, Any]:
        """
        Выполняет dbt test для модели.

        Args:
            model_name: Имя модели
            target: Целевая схема

        Returns:
            Dict[str, Any]: Результат тестирования
        """
        cmd = [
            'dbt', 'test',
            '--select', model_name,
            '--target', target
        ]

        self.log.info(f"Выполнение dbt test: {' '.join(cmd)}")

        try:
            result = subprocess.run(
                cmd,
                cwd=str(self.project_path),
                capture_output=True,
                text=True,
                check=True
            )

            return {
                'success': True,
                'stdout': result.stdout,
                'stderr': result.stderr
            }

        except subprocess.CalledProcessError as e:
            self.log.warning(f"dbt test завершен с ошибками: {e.stderr}")

            return {
                'success': False,
                'stdout': e.stdout,
                'stderr': e.stderr
            }

    def _build_run_command(
        self,
        model_name: str,
        target: str,
        variables: Optional[Dict[str, str]],
        debug: bool
    ) -> list:
        """
        Формирует команду dbt run.

        Args:
            model_name: Имя модели
            target: Целевая схема
            variables: Переменные
            debug: Debug режим

        Returns:
            list: Команда в виде списка аргументов
        """
        cmd = [
            'dbt', 'run',
            '--select', model_name,
            '--target', target,
            '--profiles-dir', str(self.project_path)
        ]

        if debug:
            cmd.append('-d')

        if variables:
            vars_str = self._format_variables(variables)
            cmd.extend(['--vars', vars_str])

        return cmd

    @staticmethod
    def _format_variables(variables: Dict[str, str]) -> str:
        """
        Форматирует переменные для dbt --vars.

        Args:
            variables: Словарь переменных

        Returns:
            str: Строка вида '{"key": "value", ...}'
        """
        items = [f'"{k}": "{v}"' for k, v in variables.items()]
        return '{' + ', '.join(items) + '}'
