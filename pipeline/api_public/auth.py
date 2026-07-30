"""外部公開 API の認証 (`Authorization: Bearer plk_...`) + 簡易クォータ。

control plane 本体 (8001) は LAN 信頼で無認証だが、 **外部投入だけは必ずキーを要求する**。
理由は 2 つ:
  - `site='create-<user_slug>'` の attribution (誰が入れたかを後から SQL で辿れる)
  - 1 キーが `crawl_image` (既に 1.25 億行) を無制限に膨らませられないようにする

クォータはプロセス内カウンタ。 再起動でリセットされる = 厳格な課金には使えないが、
「暴走した呼び出し元がフリートを埋める」ことは防げる。 厳格化が必要になった時点で
`audit_log` か専用表に移す。
"""

from __future__ import annotations

import os
import threading
from datetime import date

from fastapi import Depends, HTTPException, Request, status
from fastapi.security import HTTPAuthorizationCredentials, HTTPBearer

from pipeline.repositories.api_keys import ApiKeyRepository

# 認証を切れるのは開発用。 既定は必須 (= 明示的に 0 を入れない限り閉じている)。
REQUIRE_AUTH = os.environ.get("PIPELINE_CREATE_REQUIRE_AUTH", "1") not in ("0", "false", "False")
# 1 キーあたり 1 日の最大投入件数。 0 で無制限。
DAILY_LIMIT = int(os.environ.get("PIPELINE_CREATE_DAILY_LIMIT", "20000"))

_bearer = HTTPBearer(auto_error=False)

_quota_lock = threading.Lock()
_quota: dict[tuple[str, str], int] = {}      # (key_id, YYYY-MM-DD) → 件数


def _quota_key(key_id: str) -> tuple[str, str]:
    return (key_id, date.today().isoformat())


def consume_quota(key_id: str, n: int = 1) -> None:
    """クォータを n 件消費。 超過なら 429。"""
    if DAILY_LIMIT <= 0:
        return
    k = _quota_key(key_id)
    with _quota_lock:
        used = _quota.get(k, 0)
        if used + n > DAILY_LIMIT:
            raise HTTPException(
                status_code=status.HTTP_429_TOO_MANY_REQUESTS,
                detail=f"daily create limit reached ({DAILY_LIMIT})",
            )
        _quota[k] = used + n
        # 日付が変わった分のエントリを掃除 (キー数は少ないので全走査で十分)。
        if len(_quota) > 64:
            today = k[1]
            for stale in [kk for kk in _quota if kk[1] != today]:
                del _quota[stale]


def quota_used(key_id: str) -> int:
    with _quota_lock:
        return _quota.get(_quota_key(key_id), 0)


def require_api_key(
    request: Request,
    creds: HTTPAuthorizationCredentials | None = Depends(_bearer),
) -> dict:
    """有効な API キーを要求し、 {'id','user_slug','name'} を返す。

    `PIPELINE_CREATE_REQUIRE_AUTH=0` の時のみ匿名 (`user_slug='anon'`) を許す。
    """
    if creds is None or not (creds.credentials or "").strip():
        if not REQUIRE_AUTH:
            return {"id": "anon", "user_slug": "anon", "name": "anonymous (auth disabled)"}
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Authorization: Bearer <api key> が必要",
            headers={"WWW-Authenticate": "Bearer"},
        )
    repo = ApiKeyRepository(request.app.state.db)
    principal = repo.verify(creds.credentials)
    if principal is None:
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="API キーが無効 / 失効 / 期限切れ",
            headers={"WWW-Authenticate": "Bearer"},
        )
    repo.touch(principal["id"])
    return principal
