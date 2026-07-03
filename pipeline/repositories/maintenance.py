"""DB メンテナンス: 各テーブルの定期 purge を 1 か所に集約。

新しいテーブルを追加したい場合は purge_* メソッドを追加し、
purge_all() に組み込むだけでよい。
呼び出し元は server.py の _maintenance_loop() のみ。
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from pipeline.db.base import Database


class MaintenanceRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    # ------------------------------------------------------------------ #
    # service_logs — 行数ベース (threshold 超えで keep_rows にトリム)
    # ------------------------------------------------------------------ #

    def purge_service_logs(
        self,
        *,
        threshold: int = 500_000,
        keep_rows: int = 300_000,
    ) -> int:
        """行数が threshold を超えていれば古い行を削除して keep_rows 件に切り詰める。"""
        with self.db.transaction() as conn:
            count = conn.execute("SELECT COUNT(*) AS n FROM service_logs").fetchone()["n"]
            if count <= threshold:
                return 0
            cur = conn.execute(
                """
                DELETE FROM service_logs
                WHERE id <= (
                    SELECT id FROM (
                        SELECT id FROM service_logs ORDER BY id DESC LIMIT 1 OFFSET :keep
                    ) AS t
                )
                """,
                {"keep": int(keep_rows)},
            )
            return cur.rowcount or 0

    # ------------------------------------------------------------------ #
    # runs — 時間ベース (完了済みで retain_days より古いものを削除)
    # ------------------------------------------------------------------ #

    def purge_runs(self, *, retain_days: int = 3) -> int:
        """finished_at が retain_days より古い完了済み run を削除する。"""
        cutoff = (datetime.now(timezone.utc) - timedelta(days=retain_days)).isoformat()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "DELETE FROM runs WHERE finished_at IS NOT NULL AND finished_at < :cut",
                {"cut": cutoff},
            )
            return cur.rowcount or 0

    # ------------------------------------------------------------------ #
    # plugin_runtime_blob — TTL ベース (画像キャッシュ)
    # ------------------------------------------------------------------ #

    def purge_plugin_runtime_blob(self, *, ttl_minutes: int = 30) -> int:
        """ttl_minutes より古い blob キャッシュを削除する。"""
        cutoff = (datetime.now(timezone.utc) - timedelta(minutes=ttl_minutes)).isoformat()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "DELETE FROM plugin_runtime_blob WHERE updated_at < :cut",
                {"cut": cutoff},
            )
            return cur.rowcount or 0

    # ------------------------------------------------------------------ #
    # plugin_runtime_state — TTL ベース (処理済み ID 等の state)
    # ------------------------------------------------------------------ #

    def purge_plugin_runtime_state(self, *, ttl_hours: int = 24) -> int:
        """ttl_hours より古い runtime state を削除する。"""
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=ttl_hours)).isoformat()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "DELETE FROM plugin_runtime_state WHERE updated_at < :cut",
                {"cut": cutoff},
            )
            return cur.rowcount or 0

    # ------------------------------------------------------------------ #
    # 一括実行
    # ------------------------------------------------------------------ #

    def purge_all(self) -> dict[str, Any]:
        """全テーブルを一括パージし、削除件数を返す。"""
        return {
            "service_logs": self.purge_service_logs(),
            "runs": self.purge_runs(),
            "plugin_runtime_blob": self.purge_plugin_runtime_blob(),
            "plugin_runtime_state": self.purge_plugin_runtime_state(),
        }
