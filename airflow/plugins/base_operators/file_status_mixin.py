"""
Миксин для управления статусами файлов в PostgreSQL
"""
from typing import Optional


class FileStatusMixin:
    """
    Миксин для управления статусами файлов в PostgreSQL.
    Предоставляет методы для обновления статусов и получения метаданных.
    """

    def update_file_status(self, file_id: str, status_code: str) -> bool:
        """
        Обновить статус файла в service_db

        Args:
            file_id: ID файла (GUID)
            status_code: Код статуса

        Returns:
            bool: True если успешно, False в случае ошибки
        """
        try:
            hook = self.get_postgres_hook(database="service_db")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    sql = "SELECT update_status_file_s3(%s, %s);"
                    cur.execute(sql, (file_id, status_code))
                    result = cur.fetchone()
                    conn.commit()
                    self.log.info(
                        f"Статус файла {file_id} изменен на {status_code}"
                    )
                    return result[0] if result else False
        except Exception as e:
            self.log.error(
                f"Ошибка обновления статуса файла {file_id}: {e}"
            )
            return False

    def get_file_key_from_db(
        self,
        file_name: str,
        bucket: str,
        rn: int = 2,
    ) -> Optional[str]:
        """
        Получить ключ файла из базы данных

        Args:
            file_name: Имя файла
            bucket: Имя бакета

        Returns:
            Optional[str]: Ключ файла или None если не найден
        """
        try:
            hook = self.get_postgres_hook(database="service_db")
            sql = (
                """
                SELECT q.key
                FROM (
                    SELECT
                        ROW_NUMBER() over(
                            ORDER BY fm.last_modified DESC
                        ) as rn,
                        fm.key
                    FROM public.file_metadata as fm
                        inner join public.buckets as b
                            on fm.bucket_id = b.bucket_id
                    WHERE fm.file_name = %s
                        AND b.bucket_name = %s
                ) as q
                WHERE q.rn = %s;
                """
            )
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (file_name, bucket, rn))
                    result = cur.fetchone()
                    return result[0] if result else None
        except Exception as e:
            self.log.error(f"Ошибка получения ключа файла: {e}")
            return None

    def check_file_change(
        self,
        file_name: str,
        user_name: str,
        guid: str,
        bucket: str,
        last_modified,
        size: int,
        key: str
    ) -> int:
        """
        Проверить изменение файла и обновить метаданные

        Args:
            file_name: Имя файла
            user_name: Имя пользователя
            guid: GUID файла
            bucket: Имя бакета
            last_modified: Время последнего изменения
            size: Размер файла
            key: Ключ файла

        Returns:
            int: Код результата (1-новый, 2-изменен, 3-ошибка, 4-без изм.)
        """
        hook = self.get_postgres_hook(database="service_db")
        sql = (
            "SELECT check_update_file_s3(%s::VARCHAR, %s::VARCHAR, "
            "%s::VARCHAR, %s::VARCHAR, %s::TIMESTAMP, %s::BIGINT, "
            "%s::VARCHAR);"
        )
        conn = hook.get_conn()
        cur = conn.cursor()
        try:
            cur.execute(
                sql,
                (file_name, user_name, guid, bucket, last_modified, size, key)
            )
            result = cur.fetchone()
            conn.commit()
            return result[0]
        except Exception as e:
            conn.rollback()
            self.log.error(
                f"Ошибка проверки изменения файла {file_name}: {e}"
            )
            raise
        finally:
            cur.close()
            conn.close()

    def delete_info_metadata(self, key: str) -> bool:
        """
        Удалить информацию о файле из базы данных

        Args:
            key: Ключ файла в таблице метаданных

        Returns:
            bool: True если успешно, False в случае ошибки
        """
        try:
            hook = self.get_postgres_hook(database="service_db")
            with hook.get_conn() as conn:
                with conn.cursor() as cur:
                    sql = "DELETE FROM public.file_metadata WHERE key = %s;"
                    cur.execute(sql, (key,))
                    conn.commit()
                    return True
        except Exception as e:
            self.log.error(f"Ошибка удаления информации о файле: {e}")
            return False
