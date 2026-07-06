"""P2-1: agent 内のローカル HTTP アグリゲータ (リバースプロキシ)。

目的: 1 host の N 個の worker 子が各自 control plane を高頻度に叩く (list_workloads は
drain ループ毎、 heartbeat は 5s 毎) のを agent が畳んで上流負荷を N 分の1に落とし、
制御plane の event-loop stall を緩和する。

方式 (worker/server とも無改修):
- 子は `--control-url http://127.0.0.1:<port>` で agent の proxy を向く (agent が spawn 時に指定)。
- proxy は:
  - GET `/workers/{id}/workloads` (list_workloads) と `/workers/{id}/config` を worker_id 毎に
    `refresh_s` 秒キャッシュ (= 上流フェッチは worker あたり最大 1/refresh_s)。 キャッシュ応答は
    サーバの実応答そのものなので filter ロジック複製不要=常に正しい。
  - PUT `/workers/{id}/heartbeat` は `refresh_s` 秒に 1 回だけ上流へ (残りはローカル 200 ACK)。
    control plane の lost 判定閾値 (>refresh_s) 内なので liveness は保たれる。
  - それ以外 (register/claim/complete/fail/runs/vram/higher-pending 等 データプレーン) は透過転送。
- どの段でも失敗したら素の上流転送にフォールバック (子の疎通を絶対に切らない)。

P2-2 で per-worker キャッシュ→全子分を 1 上流 packet に集約 (server の /agents/{host}/sync) へ発展。
"""
from __future__ import annotations

import json
import logging
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer

log = logging.getLogger("pipeline.agent")

# worker_id 毎にキャッシュする GET パスの suffix (control plane 実装に合わせる)
_CACHE_SUFFIXES = ("/workloads", "/config")


class _Cache:
    """(method, path) → (expires_monotonic, status, headers, body) の TTL キャッシュ。"""

    def __init__(self) -> None:
        self._d: dict[str, tuple[float, int, bytes]] = {}
        self._lock = threading.Lock()

    def get(self, key: str) -> tuple[int, bytes] | None:
        now = time.monotonic()
        with self._lock:
            v = self._d.get(key)
            if v and v[0] > now:
                return v[1], v[2]
            if v:
                self._d.pop(key, None)
        return None

    def put(self, key: str, ttl: float, status: int, body: bytes) -> None:
        with self._lock:
            self._d[key] = (time.monotonic() + ttl, status, body)


class AggregatorProxy:
    def __init__(
        self,
        *,
        upstream: str,
        port: int = 8799,
        refresh_s: float = 10.0,
        upstream_timeout: float = 35.0,
        coalesce_heartbeat: bool = False,
    ) -> None:
        self.upstream = upstream.rstrip("/")
        self.port = port
        self.refresh_s = refresh_s
        self.upstream_timeout = upstream_timeout
        # heartbeat 合体は control plane の lost 判定閾値 (> refresh_s) を確認してから有効化する。
        # 既定 off: P2-1 は list_workloads/config キャッシュのみで安全に効果を出す。
        self.coalesce_heartbeat = coalesce_heartbeat
        self._cache = _Cache()
        self._hb_last: dict[str, float] = {}   # worker_id → 最終上流 heartbeat monotonic
        self._hb_lock = threading.Lock()
        self._httpd: ThreadingHTTPServer | None = None
        self._thread: threading.Thread | None = None
        # 統計 (観測用)
        self.n_upstream = 0
        self.n_served_cache = 0
        self.n_hb_absorbed = 0
        self._stat_lock = threading.Lock()

    @property
    def base_url(self) -> str:
        return f"http://127.0.0.1:{self.port}"

    # ---------------- 上流呼び出し ----------------

    def _forward(self, method: str, path: str, body: bytes | None, headers: dict) -> tuple[int, bytes]:
        url = self.upstream + path
        req = urllib.request.Request(url, data=body if body else None, method=method)
        for k, v in headers.items():
            if k.lower() in ("content-type", "accept"):
                req.add_header(k, v)
        if body and "content-type" not in {h.lower() for h in headers}:
            req.add_header("Content-Type", "application/json")
        with self._stat_lock:
            self.n_upstream += 1
        try:
            with urllib.request.urlopen(req, timeout=self.upstream_timeout) as r:
                return r.status, r.read()
        except urllib.error.HTTPError as e:
            return e.code, e.read()

    # ---------------- リクエスト処理 (ハンドラから呼ぶ) ----------------

    def handle(self, method: str, path: str, body: bytes | None, headers: dict) -> tuple[int, bytes]:
        # heartbeat: refresh_s に 1 回だけ上流へ、 それ以外はローカル ACK
        if self.coalesce_heartbeat and method == "PUT" and path.endswith("/heartbeat"):
            wid = path.rsplit("/", 2)[-2] if "/workers/" in path else path
            now = time.monotonic()
            with self._hb_lock:
                last = self._hb_last.get(wid, 0.0)
                due = (now - last) >= self.refresh_s
                if due:
                    self._hb_last[wid] = now
            if not due:
                with self._stat_lock:
                    self.n_hb_absorbed += 1
                return 200, b'{"ok":true,"absorbed":true}'
            return self._forward(method, path, body, headers)

        # cacheable GET (list_workloads / config): worker_id 毎に TTL キャッシュ
        if method == "GET" and any(path.endswith(s) for s in _CACHE_SUFFIXES):
            key = f"GET {path}"
            hit = self._cache.get(key)
            if hit is not None:
                with self._stat_lock:
                    self.n_served_cache += 1
                return hit
            status, resp = self._forward(method, path, body, headers)
            if status == 200:
                self._cache.put(key, self.refresh_s, status, resp)
            return status, resp

        # それ以外は透過転送 (データプレーン)
        return self._forward(method, path, body, headers)

    # ---------------- サーバ起動/停止 ----------------

    def start(self) -> None:
        proxy = self

        class Handler(BaseHTTPRequestHandler):
            protocol_version = "HTTP/1.1"

            def log_message(self, *a):  # journald ノイズ抑制
                pass

            def _do(self, method: str) -> None:
                try:
                    length = int(self.headers.get("Content-Length") or 0)
                    body = self.rfile.read(length) if length else None
                    hdrs = {k: v for k, v in self.headers.items()}
                    status, resp = proxy.handle(method, self.path, body, hdrs)
                except Exception as e:  # 何があっても子を落とさない: 503 で返す
                    log.warning("[proxy] handle error %s %s: %s", method, self.path, e)
                    status, resp = 502, json.dumps({"error": str(e)}).encode()
                self.send_response(status)
                self.send_header("Content-Type", "application/json")
                self.send_header("Content-Length", str(len(resp)))
                self.end_headers()
                if resp:
                    self.wfile.write(resp)

            def do_GET(self):    self._do("GET")
            def do_POST(self):   self._do("POST")
            def do_PUT(self):    self._do("PUT")
            def do_DELETE(self): self._do("DELETE")

        self._httpd = ThreadingHTTPServer(("127.0.0.1", self.port), Handler)
        self._thread = threading.Thread(target=self._httpd.serve_forever, daemon=True, name="agent-proxy")
        self._thread.start()
        log.info("[proxy] listening on %s → upstream %s (refresh=%.0fs)",
                 self.base_url, self.upstream, self.refresh_s)

    def stats(self) -> dict:
        with self._stat_lock:
            return {"upstream": self.n_upstream, "cache_hits": self.n_served_cache,
                    "hb_absorbed": self.n_hb_absorbed}

    def stop(self) -> None:
        if self._httpd:
            self._httpd.shutdown()
            self._httpd.server_close()
