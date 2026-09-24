"""/api/v1/create/* だけに掛ける CORS (オリジン無制限)。

## なぜオリジンを絞らないのか

create API の認証は `Authorization: Bearer <plk_...>` で、 ブラウザが勝手に付ける
資格情報 (cookie) には一切依存しない。 なので:

  - キーを持たないページから叩かれても 401 で終わる (オリジン制限が守るものが無い)
  - キーが漏れたら curl 等の非ブラウザから叩けるので、 オリジン制限では止まらない

つまり Bearer 認証の API にとってオリジン許可リストは防御になっておらず、 拡張が
対応 SNS を増やすたびに `PIPELINE_CORS_ORIGINS` と nginx の map を書き換える運用
コストだけが残っていた (2026-09-24 に廃止)。

## なぜパスを絞るのか

control plane (:8001) には認証なしの管理 API がある (`POST /api/v1/api-keys` 等)。
アプリ全体に `*` を出すと、 LAN 内の利用者が開いた任意サイトの JS がそれらの応答を
読めてしまう。 よって `*` を出すのは create API のパス配下だけにする。
`allow_credentials` は `*` と両立しないので当然 False (cookie は載らない)。
"""

from __future__ import annotations

from starlette.middleware.cors import CORSMiddleware
from starlette.types import ASGIApp, Receive, Scope, Send

CREATE_API_PREFIX = "/api/v1/create/"


class CreateApiCors:
    """`CREATE_API_PREFIX` 配下のリクエストだけ CORSMiddleware を通す ASGI ミドルウェア。"""

    def __init__(self, app: ASGIApp) -> None:
        self.app = app
        self.cors = CORSMiddleware(
            app,
            allow_origins=["*"],
            allow_credentials=False,
            allow_methods=["GET", "POST", "OPTIONS"],
            allow_headers=["authorization", "content-type"],
            max_age=86400,
        )

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] == "http" and scope.get("path", "").startswith(CREATE_API_PREFIX):
            await self.cors(scope, receive, send)
        else:
            await self.app(scope, receive, send)
