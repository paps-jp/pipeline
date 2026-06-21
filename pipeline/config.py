"""Pipeline 設定ローダ。

env > config file > default の優先順で settings を組み立てる。
将来 YAML / TOML サポート追加候補。
"""

from __future__ import annotations

import os
from dataclasses import dataclass, field
from pathlib import Path


@dataclass(frozen=True)
class Settings:
    """ランタイム全体で共有される設定 (immutable)。"""

    db_url: str = "sqlite:///./pipeline.db"
    bind_host: str = "0.0.0.0"
    bind_port: int = 8000
    mode: str = "control"  # "dev" | "control" | "worker" | "single"
    log_level: str = "INFO"
    admin_password: str | None = None  # 初回起動時のみ env で指定
    secret_key: str | None = None  # session HMAC 用
    plugin_dirs: tuple[Path, ...] = field(default_factory=tuple)

    @classmethod
    def from_env(cls, **overrides) -> Settings:
        """環境変数 + overrides から Settings を組み立てる。"""

        def env(name: str, default=None):
            return os.environ.get(f"PIPELINE_{name}", default)

        defaults = dict(
            db_url=env("DB_URL", "sqlite:///./pipeline.db"),
            bind_host=env("BIND_HOST", "0.0.0.0"),
            bind_port=int(env("BIND_PORT", "8000")),
            mode=env("MODE", "control"),
            log_level=env("LOG_LEVEL", "INFO"),
            admin_password=env("ADMIN_PASSWORD"),
            secret_key=env("SECRET_KEY"),
            plugin_dirs=tuple(
                Path(p) for p in env("PLUGIN_DIRS", "").split(os.pathsep) if p
            ),
        )
        defaults.update(overrides)
        return cls(**defaults)
