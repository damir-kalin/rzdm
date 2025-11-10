"""
Класс для выполнения Stream Load операций в StarRocks.
"""
import time
import logging
import pandas as pd
from typing import Dict, Any, Optional, List
from requests import Session
from requests.auth import HTTPBasicAuth


class StarRocksStreamLoader:
    """
    Выполняет загрузку данных в StarRocks через Stream Load API.
    """

    def __init__(
        self,
        session: Session,
        host: str,
        port: int,
        user: str,
        password: str,
        timeout: int = 600,
        logger: logging.Logger = None
    ):
        """
        Args:
            session: HTTP сессия для выполнения запросов
            host: Хост StarRocks
            port: Порт HTTP API (обычно 8030)
            user: Имя пользователя
            password: Пароль
            timeout: Таймаут операций в секундах
            logger: Логгер для вывода сообщений
        """
        self.session = session
        self.host = host
        self.port = port
        self.user = user
        self.password = password
        self.timeout = timeout
        self.log = logger or logging.getLogger(__name__)

    def execute_stream_load(
        self,
        database: str,
        table: str,
        payload: bytes,
        label: Optional[str] = None,
        columns: Optional[List[str]] = None,
        format: str = "CSV",
        column_separator: str = "|",
        max_filter_ratio: float = 0.0,
        timeout: Optional[int] = None,
        **properties: Any,
    ) -> Dict[str, Any]:
        """
        Выполняет HTTP PUT запрос для Stream Load.

        Args:
            database: Имя базы данных
            table: Имя таблицы
            payload: Данные для загрузки в bytes
            label: Уникальная метка загрузки
            columns: Список колонок для загрузки
            format: Формат данных (CSV, JSON и т.д.)
            column_separator: Разделитель колонок
            max_filter_ratio: Максимальный процент отфильтрованных строк
            timeout: Таймаут операции
            **properties: Дополнительные свойства для Stream Load

        Returns:
            Dict[str, Any]: Результат загрузки от StarRocks
        """
        url = (
            f"http://{self.host}:{self.port}/api/"
            f"{database}/{table}/_stream_load"
        )

        headers = self._build_headers(
            label=label or self._generate_label(),
            columns=columns,
            format=format,
            column_separator=column_separator,
            max_filter_ratio=max_filter_ratio,
            timeout=timeout or self.timeout,
            **properties,
        )

        headers["charset"] = "utf8"

        self.log.info(
            f"Stream Load: {len(payload)} bytes -> "
            f"{database}.{table} (label: {headers['label']})"
        )

        response = self.session.put(
            url,
            headers=headers,
            data=payload,
            auth=HTTPBasicAuth(self.user, self.password),
            timeout=(timeout or self.timeout) + 30,
            allow_redirects=False,
        )

        # Обрабатываем перенаправление 307
        if response.status_code == 307:
            redirect_url = response.headers.get('location')
            self.log.info(f"Перенаправление на: {redirect_url}")

            response = self.session.put(
                redirect_url,
                headers=headers,
                data=payload,
                auth=HTTPBasicAuth(self.user, self.password),
                timeout=(timeout or self.timeout) + 30,
                allow_redirects=False,
            )

        self._validate_response(response)

        result = response.json()
        self._validate_load_result(result)

        self.log.info(
            f"Stream load успешно завершена. "
            f"Загружено строк: {result.get('NumberLoadedRows', 0)}, "
            f"Отфильтровано строк: {result.get('NumberFilteredRows', 0)}"
        )

        return result

    def execute_chunked_load(
        self,
        database: str,
        table: str,
        df: pd.DataFrame,
        chunk_size: int,
        label_prefix: Optional[str] = None,
        **kwargs
    ) -> Dict[str, Any]:
        """
        Загружает DataFrame чанками.

        Args:
            database: Имя базы данных
            table: Имя таблицы
            df: DataFrame для загрузки
            chunk_size: Размер чанка
            label_prefix: Префикс для меток чанков
            **kwargs: Дополнительные параметры для stream load

        Returns:
            Dict[str, Any]: Агрегированный результат загрузки
        """
        total_loaded = 0
        total_filtered = 0
        chunks_count = (len(df) + chunk_size - 1) // chunk_size

        self.log.info(
            f"Загрузка чанками: {len(df)} строк, "
            f"{chunks_count} чанков по {chunk_size} строк"
        )

        for i in range(0, len(df), chunk_size):
            chunk_df = df.iloc[i:i + chunk_size]
            chunk_num = i // chunk_size + 1

            chunk_label = (
                f"{label_prefix or 'chunk'}_{chunk_num}_{int(time.time())}"
            )

            # Конвертируем chunk в CSV
            from .starrocks_data_converter import StarRocksDataConverter
            converter = StarRocksDataConverter(self.log)
            payload = converter.dataframe_to_csv(
                chunk_df,
                kwargs.get('column_separator', '|')
            )

            result = self.execute_stream_load(
                database=database,
                table=table,
                payload=payload,
                label=chunk_label,
                **kwargs,
            )

            total_loaded += result.get("NumberLoadedRows", 0)
            total_filtered += result.get("NumberFilteredRows", 0)

            self.log.info(
                f"Чанк {chunk_num}/{chunks_count}: "
                f"загружено {result.get('NumberLoadedRows', 0)} строк"
            )

        return {
            "Status": "Success",
            "NumberLoadedRows": total_loaded,
            "NumberFilteredRows": total_filtered,
            "ChunksCount": chunks_count
        }

    def _build_headers(
        self,
        label: str,
        columns: Optional[List[str]],
        format: str,
        column_separator: str,
        max_filter_ratio: float,
        timeout: int,
        **properties: Any,
    ) -> Dict[str, str]:
        """Формирует заголовки для Stream Load запроса."""
        headers = {
            "label": label,
            "format": format,
            "column_separator": column_separator,
            "max_filter_ratio": str(max_filter_ratio),
            "timeout": str(timeout),
        }

        # Не передаем заголовок columns для автоматического определения
        # StarRocks сам сопоставит колонки из CSV с колонками таблицы
        # по порядку следования
        
        # Добавляем дополнительные свойства
        for key, value in properties.items():
            headers[key] = str(value)

        return headers

    def _validate_response(self, response):
        """Проверяет HTTP ответ от StarRocks."""
        self.log.info(f"Статус ответа: {response.status_code}")
        self.log.debug(f"Заголовки ответа: {response.headers}")
        self.log.debug(f"Текст ответа: {response.text[:500]}")

        if response.status_code != 200:
            self.log.error(
                f"Ошибка HTTP {response.status_code}: {response.text}"
            )
            raise RuntimeError(
                f"Ошибка Stream load HTTP {response.status_code}: "
                f"{response.text}"
            )

        try:
            response.json()
        except Exception as e:
            self.log.error(
                f"Ошибка парсинга JSON ответа: {response.text[:1000]}"
            )
            raise RuntimeError(
                f"Ошибка Stream load - неверный JSON ответ: "
                f"{response.text[:500]}"
            ) from e

    def _validate_load_result(self, result: Dict[str, Any]):
        """Проверяет результат загрузки от StarRocks."""
        status = result.get("Status", "Unknown")
        if status != "Success":
            error_msg = result.get("Message", "Unknown error")
            self.log.error(
                f"Ошибка Stream load: {status} - {error_msg}"
            )
            self.log.error(f"Полный ответ: {result}")
            raise RuntimeError(
                f"Ошибка Stream load: {status} - {error_msg}"
            )

    @staticmethod
    def _generate_label() -> str:
        """Генерирует уникальную метку для загрузки."""
        return f"stream_load_{int(time.time() * 1000)}"
