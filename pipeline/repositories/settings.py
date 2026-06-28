"""settings の key-value CRUD + LLM 設定の便利ラッパ。

is_secret=1 のキー値は API 経由で取り出すときマスクされる
(repo の get_masked) が、 supervisor 等 server プロセス内部からは get_raw で生値を取れる。
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


# LLM 設定の既定値 (= 初回起動時に seed)。 「設定画面」 に出すキー一覧。
LLM_DEFAULTS: dict[str, dict[str, Any]] = {
    "llm.enabled": {
        "value": "0", "is_secret": 0,
        "description": "LLM advisor を有効化 (= supervisor が定期的に LLM に最適化提案を求める)",
    },
    "llm.apply_mode": {
        "value": "0", "is_secret": 0,
        "description": "LLM の提案を実際に適用 (1) するか dry-run (0)",
    },
    "llm.provider": {
        "value": "deepseek", "is_secret": 0,
        "description": "LLM プロバイダ名 (deepseek/openai/anthropic 等)",
    },
    "llm.endpoint": {
        "value": "https://api.deepseek.com/v1/chat/completions", "is_secret": 0,
        "description": "OpenAI 互換 chat completions エンドポイント",
    },
    "llm.api_key": {
        "value": "", "is_secret": 1,
        "description": "API キー (Bearer)。 マスク表示、 値は変更時のみ送信",
    },
    "llm.model": {
        "value": "deepseek-chat", "is_secret": 0,
        "description": "モデル名 (deepseek-chat / deepseek-reasoner 等)",
    },
    "llm.interval_min": {
        "value": "15", "is_secret": 0,
        "description": "LLM コール間隔 (分)。 短いほど反応速いが API コスト増",
    },
    "llm.max_actions_per_cycle": {
        "value": "5", "is_secret": 0,
        "description": "1 回の LLM 提案で適用する action 上限",
    },
    "llm.confidence_threshold": {
        "value": "0.7", "is_secret": 0,
        "description": "LLM 提案の confidence がこの値未満なら適用しない",
    },
    "llm.timeout_s": {
        "value": "60", "is_secret": 0,
        "description": "LLM HTTP リクエスト timeout (秒)",
    },
}


def _mask_value(value: str | None) -> str | None:
    """secret value を `sk-***...XXX` 形式にマスク。 None / 空 はそのまま。"""
    if not value:
        return value
    if len(value) <= 8:
        return "***"
    return f"{value[:3]}***{value[-4:]}"


class SettingsRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def ensure_seed(self) -> None:
        """LLM_DEFAULTS の未登録キーだけ作成。 既存値は上書きしない。"""
        now = _utcnow_iso()
        with self.db.transaction() as conn:
            for k, spec in LLM_DEFAULTS.items():
                cur = conn.execute("SELECT 1 FROM settings WHERE key = :k", {"k": k})
                if cur.fetchone() is not None:
                    continue
                conn.execute(
                    "INSERT INTO settings (key, value, description, is_secret, updated_at) "
                    "VALUES (:k, :v, :d, :s, :t)",
                    {"k": k, "v": spec["value"], "d": spec.get("description"),
                     "s": int(spec.get("is_secret", 0)), "t": now},
                )

    def list_all_masked(self) -> list[dict[str, Any]]:
        """全 settings を返す。 is_secret=1 の値はマスク済。 UI 取得用。"""
        out: list[dict[str, Any]] = []
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT key, value, description, is_secret, updated_at, updated_by "
                "FROM settings ORDER BY key",
            )
            for r in cur.fetchall():
                row = dict(r)
                if row.get("is_secret"):
                    row["value_masked"] = _mask_value(row.get("value"))
                    row["value"] = None  # 生値は返さない
                out.append(row)
        return out

    def get_raw(self, key: str, default: str | None = None) -> str | None:
        """生値を返す (= supervisor 等の server-side で API key を実際に使うとき)。
        UI / 公開 API は絶対呼ばないこと。"""
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT value FROM settings WHERE key = :k", {"k": key})
            row = cur.fetchone()
            return (row["value"] if row else default) or default

    def set_value(self, key: str, value: str | None,
                  updated_by: str | None = None) -> dict[str, Any]:
        """既存キーの値を更新。 未登録キーは LLM_DEFAULTS で seed されたものに限る
        (= 任意キー追加は別 API、 ここは管理キーだけ)。"""
        now = _utcnow_iso()
        with self.db.transaction() as conn:
            cur = conn.execute("SELECT key, is_secret FROM settings WHERE key = :k", {"k": key})
            existing = cur.fetchone()
            if existing is None:
                raise KeyError(f"unknown setting key: {key}")
            conn.execute(
                "UPDATE settings SET value = :v, updated_at = :t, updated_by = :u "
                "WHERE key = :k",
                {"k": key, "v": value, "t": now,
                 "u": (updated_by or "operator")[:64]},
            )
            cur = conn.execute(
                "SELECT key, value, description, is_secret, updated_at, updated_by "
                "FROM settings WHERE key = :k", {"k": key},
            )
            row = dict(cur.fetchone())
        if row.get("is_secret"):
            row["value_masked"] = _mask_value(row.get("value"))
            row["value"] = None
        return row

    def get_llm_config(self) -> dict[str, Any]:
        """LLM advisor が使う設定を 1 dict にまとめて返す (生 API key 込み)。
        server-side 専用 (= secret 漏れに注意)。"""
        cfg: dict[str, Any] = {}
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT key, value FROM settings WHERE key LIKE 'llm.%'",
            )
            for r in cur.fetchall():
                key = r["key"].removeprefix("llm.")
                cfg[key] = r["value"]
        # 型変換
        for k in ("enabled", "apply_mode"):
            cfg[k] = bool(int(cfg.get(k) or 0))
        for k in ("interval_min", "max_actions_per_cycle", "timeout_s"):
            try: cfg[k] = int(cfg.get(k) or 0)
            except Exception: cfg[k] = 0
        for k in ("confidence_threshold",):
            try: cfg[k] = float(cfg.get(k) or 0)
            except Exception: cfg[k] = 0.0
        return cfg
