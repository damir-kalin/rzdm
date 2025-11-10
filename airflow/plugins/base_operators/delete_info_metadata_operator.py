from base_operators.base_hooks_operator import BaseHooksOperator
from base_operators.file_status_mixin import FileStatusMixin


class DeleteInfoMetadataOperator(FileStatusMixin, BaseHooksOperator):
    def __init__(self, file_name: str, bucket: str, rn: int = 1, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.file_name = file_name
        self.bucket = bucket
        self.rn = rn

    def execute(self, context):
        # Для удаления "пропавшего" файла берём актуальный ключ (rn=1)
        old_key = self.get_file_key_from_db(self.file_name, self.bucket, self.rn)
        if not old_key:
            self.log.warning(
                f"Старый ключ не найден для файла {self.file_name}"
            )
            return None
        else:
            self.log.info(
                f"Найден старый ключ: {old_key}. Удаляем информацию о файле..."
            )
            return self.delete_info_metadata(old_key)
