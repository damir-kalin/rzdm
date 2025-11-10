"""
Класс для конвертации данных в форматы, поддерживаемые StarRocks.
"""
import io
import logging
import pandas as pd


class StarRocksDataConverter:
    """
    Конвертирует данные (DataFrame, Excel) в форматы для загрузки в StarRocks.
    """

    def __init__(self, logger: logging.Logger = None):
        """
        Args:
            logger: Логгер для вывода сообщений
        """
        self.log = logger or logging.getLogger(__name__)

    def dataframe_to_csv(
        self,
        df: pd.DataFrame,
        separator: str = "|"
    ) -> bytes:
        """
        Конвертирует DataFrame в CSV bytes для Stream Load.

        Args:
            df: DataFrame для конвертации
            separator: Разделитель колонок

        Returns:
            bytes: CSV данные
        """
        if df.empty:
            self.log.warning("DataFrame пустой")
            return b""

        df_clean = self._clean_dataframe(df, separator)

        buffer = io.StringIO()
        df_clean.to_csv(
            buffer,
            index=False,
            header=False,
            sep=separator,
            na_rep="",
            encoding='utf-8',
            escapechar='\\',
            quoting=3,  # QUOTE_NONE - не использовать кавычки
            doublequote=False,
            lineterminator='\n',
        )

        csv_content = buffer.getvalue()
        csv_content = csv_content.replace('\r\n', '\n').replace('\r', '\n')

        self.log.debug(
            f"CSV конвертация: {len(df)} строк, "
            f"{len(csv_content)} байт"
        )
        self.log.debug(
            f"Пример CSV (первые 500 символов): {csv_content[:500]}"
        )

        return csv_content.encode("utf-8")

    def _clean_dataframe(
        self,
        df: pd.DataFrame,
        separator: str
    ) -> pd.DataFrame:
        """
        Очищает DataFrame от проблемных символов и значений.

        Args:
            df: DataFrame для очистки
            separator: Разделитель (для удаления из данных)

        Returns:
            pd.DataFrame: Очищенный DataFrame
        """
        df_clean = df.copy()

        for col in df_clean.columns:
            # Конвертируем все в строки
            df_clean[col] = df_clean[col].astype(str)

            # Заменяем все варианты null/nan на пустую строку
            df_clean[col] = df_clean[col].replace([
                'nan', 'NaN', 'None', 'NULL', 'null', '<NA>',
                'nat', 'NaT', 'NONE', 'Null', 'NAN',
                'N/A', 'n/a', '#N/A', 'NA'
            ], '')

            # Удаляем управляющие символы
            df_clean[col] = df_clean[col].str.replace(
                r'[\x00-\x1f\x7f-\x9f]', '', regex=True
            )

            # Удаляем переводы строк и табуляцию
            df_clean[col] = df_clean[col].str.replace(
                r'[\r\n\t]', ' ', regex=True
            )

            # Удаляем null байты
            df_clean[col] = df_clean[col].str.replace(
                '\x00', '', regex=False
            )

            # Нормализуем кавычки
            df_clean[col] = df_clean[col].str.replace(
                '"', '\"', regex=False
            )

            # Удаляем разделитель из данных
            if separator in ['-', ',', ';', '|', '\t']:
                df_clean[col] = df_clean[col].str.replace(
                    separator, ' ', regex=False
                )

        return df_clean

    def read_excel_from_bytes(
        self,
        file_content: bytes,
        sheet_name: int = 0
    ) -> pd.DataFrame:
        """
        Читает Excel файл из bytes.

        Args:
            file_content: Содержимое Excel файла
            sheet_name: Номер или имя листа (по умолчанию первый)

        Returns:
            pd.DataFrame: Данные из Excel
        """
        try:
            df = pd.read_excel(
                io.BytesIO(file_content),
                sheet_name=sheet_name,
                engine='openpyxl'
            )

            self.log.info(
                f"Excel файл прочитан: {len(df)} строк, "
                f"{len(df.columns)} колонок"
            )

            return df

        except Exception as e:
            self.log.error(f"Ошибка чтения Excel файла: {e}")
            raise
