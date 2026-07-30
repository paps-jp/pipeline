"""api_keys の CRUD + 照合 (= 外部公開 API の認証面)。

`schema.py` の `api_keys` 表は F4 で定義されたまま配線されていなかった。 外部投入 API
([[pipeline/api_public]]) が最初の利用者なのでここで実装する。

キー書式: `plk_<key_id>_<secret>`
  - `key_id` を平文に埋めることで **1 行 SELECT + scrypt 1 回**で照合できる
    (全行スキャンして順に hash 比較する必要が無い)。
  - `secret` だけを scrypt で hash して保存。 平文は発行時に 1 度だけ返す。

hash 形式: `scrypt$<n>$<r>$<p>$<salt_hex>$<dk_hex>` (hashlib.scrypt = stdlib のみ)。
"""

from __future__ import annotations

import hashlib
import hmac
import secrets
from datetime import datetime, timezone
from typing import Any

from pipeline.db.base import Database

# scrypt パラメータ。 API キーは高エントロピー (192bit) なので、 人間のパスワード用ほど
# 重くする必要が無い。 リクエスト毎に 1 回走るので n=2^14 (~15ms) に抑える。
_SCRYPT_N = 1 << 14
_SCRYPT_R = 8
_SCRYPT_P = 1
_DKLEN = 32

KEY_PREFIX = "plk"


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="seconds")


def _is_expired(expires_at: str | None) -> bool:
    """期限切れ判定。 パース不能な値は「期限切れ」= fail-closed に倒す。"""
    if not expires_at:
        return False
    try:
        dt = datetime.fromisoformat(str(expires_at))
    except ValueError:
        return True
    if dt.tzinfo is None:               # naive は UTC 指定とみなす
        dt = dt.replace(tzinfo=timezone.utc)
    return dt <= datetime.now(timezone.utc)


def hash_secret(secret: str, *, salt: bytes | None = None) -> str:
    salt = salt or secrets.token_bytes(16)
    dk = hashlib.scrypt(secret.encode("utf-8"), salt=salt,
                        n=_SCRYPT_N, r=_SCRYPT_R, p=_SCRYPT_P, dklen=_DKLEN)
    return f"scrypt${_SCRYPT_N}${_SCRYPT_R}${_SCRYPT_P}${salt.hex()}${dk.hex()}"


def verify_secret(secret: str, stored: str) -> bool:
    """定数時間比較。 形式不正 / パラメータ不正は False (= 例外を投げない)。"""
    try:
        scheme, n_s, r_s, p_s, salt_hex, dk_hex = stored.split("$")
        if scheme != "scrypt":
            return False
        dk = hashlib.scrypt(secret.encode("utf-8"), salt=bytes.fromhex(salt_hex),
                            n=int(n_s), r=int(r_s), p=int(p_s),
                            dklen=len(dk_hex) // 2)
    except (ValueError, TypeError):
        return False
    return hmac.compare_digest(dk.hex(), dk_hex)


def parse_key(raw: str) -> tuple[str, str] | None:
    """`plk_<id>_<secret>` → (key_id, secret)。 書式不正は None。

    secret は `token_urlsafe` 由来で `_` を含みうるので maxsplit=2 で切る
    (単純な split では secret に `_` がある鍵が全て弾かれる)。
    """
    parts = (raw or "").strip().split("_", 2)
    if len(parts) != 3 or parts[0] != KEY_PREFIX:
        return None
    key_id, secret = parts[1], parts[2]
    if not key_id or not secret:
        return None
    return key_id, secret


class ApiKeyRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    # ---------------- 発行 / 一覧 / 失効 ----------------

    def create(self, *, user_slug: str, name: str,
               expires_at: str | None = None) -> tuple[str, dict[str, Any]]:
        """新規キーを発行。 戻り値 = (平文キー, メタ情報)。 平文は二度と取得できない。"""
        key_id = secrets.token_hex(8)
        secret = secrets.token_urlsafe(24)
        row = {
            "id": key_id,
            "user_slug": user_slug,
            "name": name,
            "key_hash": hash_secret(secret),
            "enabled": 1,
            "expires_at": expires_at,
            "created_at": _utcnow_iso(),
        }
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT INTO api_keys (id, user_slug, name, key_hash, enabled, expires_at, created_at) "
                "VALUES (:id, :user_slug, :name, :key_hash, :enabled, :expires_at, :created_at)",
                row,
            )
        meta = {k: v for k, v in row.items() if k != "key_hash"}
        return f"{KEY_PREFIX}_{key_id}_{secret}", meta

    def list(self) -> list[dict[str, Any]]:
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT id, user_slug, name, enabled, expires_at, created_at, last_used_at "
                "FROM api_keys ORDER BY created_at DESC"
            )
            return cur.fetchall()

    def revoke(self, key_id: str) -> bool:
        with self.db.transaction() as conn:
            cur = conn.execute("UPDATE api_keys SET enabled = 0 WHERE id = :id", {"id": key_id})
            return cur.rowcount > 0

    # ---------------- 照合 ----------------

    def verify(self, raw_key: str) -> dict[str, Any] | None:
        """平文キーを検証。 有効なら行 (key_hash 除く) を返す。 不正/失効/期限切れは None。"""
        parsed = parse_key(raw_key)
        if parsed is None:
            return None
        key_id, secret = parsed
        with self.db.transaction() as conn:
            cur = conn.execute(
                "SELECT id, user_slug, name, key_hash, enabled, expires_at FROM api_keys "
                "WHERE id = :id",
                {"id": key_id},
            )
            row = cur.fetchone()
        if row is None or not int(row["enabled"] or 0):
            return None
        if not verify_secret(secret, row["key_hash"] or ""):
            return None
        if _is_expired(row["expires_at"]):
            return None
        return {"id": row["id"], "user_slug": row["user_slug"], "name": row["name"]}

    def touch(self, key_id: str) -> None:
        """last_used_at 更新 (best-effort — 失敗しても認証結果に影響させない)。"""
        try:
            with self.db.transaction() as conn:
                conn.execute("UPDATE api_keys SET last_used_at = :t WHERE id = :id",
                             {"t": _utcnow_iso(), "id": key_id})
        except Exception:  # noqa: BLE001
            pass
