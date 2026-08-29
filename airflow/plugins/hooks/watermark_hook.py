from datetime import datetime, timezone
from typing import Optional

from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

EPOCH = datetime(1970, 1, 1, tzinfo=timezone.utc)

class WatermarkHook(BaseHook):
    """
    Hook to manage ingestion watermarks in the audit.watermarks table.
    """

    def __init__(self, postgres_conn_id: str = 'postgres_default', *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id

    def get_conn(self):
        return PostgresHook(postgres_conn_id=self.postgres_conn_id).get_conn()

    def get_watermark(self, source_name: str) -> datetime:
        """
        Retrieves the current watermark for the given source.
        Returns the epoch if no watermark is found.
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = "SELECT watermark_ts FROM audit.watermarks WHERE source_name = %s"
        result = hook.get_first(sql, parameters=(source_name,))
        if result and result[0]:
            return result[0]
        return EPOCH

    def set_watermark(self, source_name: str, new_watermark: datetime) -> None:
        """
        Upserts the watermark for the given source with the provided new_watermark.
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = """
            INSERT INTO audit.watermarks (source_name, watermark_ts, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (source_name) DO UPDATE SET
                watermark_ts = EXCLUDED.watermark_ts,
                updated_at = EXCLUDED.updated_at
        """
        hook.run(sql, parameters=(source_name, new_watermark))

    def initialize_if_missing(self, source_name: str) -> None:
        """
        Inserts the initial watermark row with the epoch timestamp only if it does not already exist.
        """
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        sql = """
            INSERT INTO audit.watermarks (source_name, watermark_ts, updated_at)
            VALUES (%s, %s, now())
            ON CONFLICT (source_name) DO NOTHING
        """
        hook.run(sql, parameters=(source_name, EPOCH))
