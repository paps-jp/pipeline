"""データアクセスレイヤ (Repository パターン)。

各 Repository は DB アダプタ (pipeline.db.Database) を受け取って
CRUD を実装する。FastAPI router は Repository を介して DB に触る。
"""

from pipeline.repositories.workloads import WorkloadRepository

__all__ = ["WorkloadRepository"]
