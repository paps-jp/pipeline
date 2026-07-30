"""外部公開 API (= 認証付きで外から叩かせる面)。

現在は control plane (8001) に相乗りしているが、 いずれ別プロセスへ分離できるよう
**このパッケージ内で完結**させている (control plane 側の変更は include_router 2 行だけ)。
分離する時は `create_app()` を足して systemd unit を 1 本増やせば済む。

分離を検討する目安: control plane のイベントループが外部アップロードで詰まり始めた時
(過去に重クエリで 60-90s ブロックし heartbeat が滞留した事故がある)。
"""

from __future__ import annotations

from pipeline.api_public import api_keys, create

__all__ = ["api_keys", "create"]
