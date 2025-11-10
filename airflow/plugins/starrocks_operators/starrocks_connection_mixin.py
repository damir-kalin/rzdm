"""
Миксин для работы с подключением к StarRocks.
"""
from requests import Session
from requests.auth import HTTPBasicAuth
from requests.adapters import HTTPAdapter, Retry
from airflow.hooks.base import BaseHook


class StarRocksConnectionMixin:
    """
    Миксин для управления подключением к StarRocks через HTTP API.
    Предоставляет переиспользуемую сессию с настроенной аутентификацией.
    """

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._session = None
        self._starrocks_host = None
        self._starrocks_port = None
        self._starrocks_user = None
        self._starrocks_password = None

    def _initialize_starrocks_connection(
        self,
        conn_id: str,
        port: int = 8030,
        max_retries: int = 3
    ):
        """
        Инициализирует подключение к StarRocks.

        Args:
            conn_id: ID подключения в Airflow
            port: HTTP порт для Stream Load (по умолчанию 8030)
            max_retries: Количество повторных попыток при ошибках
        """
        conn = BaseHook.get_connection(conn_id)
        self._starrocks_host = conn.host
        self._starrocks_port = port
        self._starrocks_user = conn.login
        self._starrocks_password = conn.password

        self._session = Session()
        self._session.auth = HTTPBasicAuth(
            self._starrocks_user,
            self._starrocks_password
        )
        self._session.headers.update({"Expect": "100-continue"})

        retry_strategy = Retry(
            total=max_retries,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self._session.mount("http://", adapter)
        self._session.mount("https://", adapter)

        self.log.info(
            f"Инициализировано подключение к StarRocks: "
            f"{self._starrocks_host}:{self._starrocks_port}"
        )

    @property
    def starrocks_session(self) -> Session:
        """Возвращает HTTP сессию для StarRocks."""
        if self._session is None:
            raise RuntimeError(
                "StarRocks connection not initialized. "
                "Call _initialize_starrocks_connection first."
            )
        return self._session

    @property
    def starrocks_host(self) -> str:
        """Возвращает хост StarRocks."""
        if self._starrocks_host is None:
            raise RuntimeError("StarRocks connection not initialized.")
        return self._starrocks_host

    @property
    def starrocks_port(self) -> int:
        """Возвращает порт StarRocks."""
        if self._starrocks_port is None:
            raise RuntimeError("StarRocks connection not initialized.")
        return self._starrocks_port

    @property
    def starrocks_credentials(self) -> tuple:
        """Возвращает кортеж (user, password) для StarRocks."""
        if self._starrocks_user is None or self._starrocks_password is None:
            raise RuntimeError("StarRocks connection not initialized.")
        return (self._starrocks_user, self._starrocks_password)

    def close_starrocks_connection(self):
        """Закрывает HTTP сессию StarRocks."""
        if self._session:
            self._session.close()
            self.log.info("StarRocks connection closed")
