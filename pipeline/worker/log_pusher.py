"""Python logging stream を control plane の /api/v1/service-logs へ batch POST する Handler.

- threading.Thread で batch queue を flush (logging is sync; we mustn't block).
- batch interval / max batch / max queue size をチューニング可。
- network 失敗時は drop (logging shouldn't crash app).
"""

from __future__ import annotations

import json
import logging
import queue
import socket
import threading
import time
import urllib.error
import urllib.request
from datetime import datetime, timezone


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class ControlPlaneLogHandler(logging.Handler):
    """log record を control plane (`POST /api/v1/service-logs`) へ batch 送信する Handler.

    - 各 record を bounded queue (maxsize=10000) に enqueue → drain thread が batch flush。
    - flush_interval_s 毎、 もしくは batch_max に達したら POST。
    - 失敗時は静かに drop (logging stack overflow を避けるため log しない)。
    - 同一 (level, logger, record.msg) の spam は rate_limit_window_s 内 1 件だけ通し、
      期間内の抑制件数は次の通過時に "+N suppressed" として合成表示する
      (2026-07-03: pipeline-oss slow 時 poll_admin_cmd failed 等が秒 1500 件出て
      service_logs が 30分で 260万行 → nas disk 100% 事故対策)。
    """

    def __init__(
        self,
        *,
        control_url: str,
        service: str,
        host: str,
        worker_id_getter=lambda: None,
        flush_interval_s: float = 2.0,
        batch_max: int = 200,
        queue_maxsize: int = 10000,
        token: str | None = None,
        level: int = logging.INFO,
        rate_limit_window_s: float = 10.0,
        rate_limit_cache_max: int = 500,
    ) -> None:
        super().__init__(level=level)
        self._url = control_url.rstrip("/") + "/api/v1/service-logs"
        self._service = service
        self._host = host
        self._worker_id_getter = worker_id_getter
        self._token = token
        self._flush_interval_s = flush_interval_s
        self._batch_max = batch_max
        self._queue: queue.Queue[dict] = queue.Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        # rate limit: (level, logger, record.msg) -> (last_ts, suppressed_count)
        # record.msg は format 前の template (poll_admin_cmd failed 等) を使う
        # ことで、 pk 等が入る動的メッセージも同一キーで束ねられる。
        self._rate_lock = threading.Lock()
        self._rate_seen: dict[tuple[str, str, str], tuple[float, int]] = {}
        self._rate_window_s = float(rate_limit_window_s)
        self._rate_cache_max = int(rate_limit_cache_max)
        self._thread = threading.Thread(
            target=self._run, name="LogPusher", daemon=True
        )
        self._thread.start()

    def _rate_gate(self, record: logging.LogRecord) -> tuple[bool, int]:
        """rate limit 判定。 (通すか, 抑制されていた件数) を返す。

        window 内 2 回目以降は False (drop) + suppressed++。
        window を越えたら True (通す) + prev_suppressed を返して 0 に reset。
        """
        try:
            key = (record.levelname, record.name, str(record.msg))
        except Exception:
            return True, 0
        now = time.time()
        with self._rate_lock:
            entry = self._rate_seen.get(key)
            if entry is None:
                # 未観測 → 通す + cache
                if len(self._rate_seen) >= self._rate_cache_max:
                    # 溢れそう: 一番古いキーを 1 個追い出す (簡易 LRU)
                    oldest = min(self._rate_seen.items(), key=lambda kv: kv[1][0])
                    self._rate_seen.pop(oldest[0], None)
                self._rate_seen[key] = (now, 0)
                return True, 0
            last_ts, suppressed = entry
            if now - last_ts < self._rate_window_s:
                # window 内 → 抑制
                self._rate_seen[key] = (last_ts, suppressed + 1)
                return False, 0
            # window 越え → 通す + 抑制回数を報告 + reset
            self._rate_seen[key] = (now, 0)
            return True, suppressed

    def emit(self, record: logging.LogRecord) -> None:
        try:
            msg = self.format(record)
        except Exception:
            return
        allow, suppressed = self._rate_gate(record)
        if not allow:
            return
        if suppressed > 0:
            msg = f"{msg} (+{suppressed} suppressed in last {int(self._rate_window_s)}s)"
        item = {
            "ts": _utcnow_iso(),
            "host": self._host,
            "service": self._service,
            "worker_id": self._worker_id_getter() if self._worker_id_getter else None,
            "level": record.levelname,
            "logger": record.name,
            "message": msg,
            "exc_info": self.formatter.formatException(record.exc_info) if record.exc_info and self.formatter else None,
        }
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            pass  # drop oldest behavior is too expensive; just drop new

    def _run(self) -> None:
        last_flush = time.time()
        batch: list[dict] = []
        while not self._stop.is_set():
            timeout = max(0.05, self._flush_interval_s - (time.time() - last_flush))
            try:
                item = self._queue.get(timeout=timeout)
                batch.append(item)
            except queue.Empty:
                pass
            now = time.time()
            if batch and (len(batch) >= self._batch_max or now - last_flush >= self._flush_interval_s):
                self._post(batch)
                batch = []
                last_flush = now
        # final flush
        try:
            while True:
                batch.append(self._queue.get_nowait())
        except queue.Empty:
            pass
        if batch:
            self._post(batch)

    def _post(self, batch: list[dict]) -> None:
        payload = json.dumps({"records": batch}).encode("utf-8")
        headers = {"Content-Type": "application/json"}
        if self._token:
            headers["Authorization"] = f"Bearer {self._token}"
        req = urllib.request.Request(self._url, data=payload, headers=headers, method="POST")
        try:
            with urllib.request.urlopen(req, timeout=5):
                pass
        except (urllib.error.URLError, urllib.error.HTTPError, socket.timeout, OSError):
            pass  # network down or control plane restart; drop silently

    def close(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=3)
        except Exception:
            pass
        super().close()


def attach_log_pusher(
    *,
    control_url: str,
    service: str,
    host: str | None = None,
    worker_id_getter=lambda: None,
    level: int = logging.INFO,
    token: str | None = None,
) -> ControlPlaneLogHandler | None:
    """root logger に handler を attach。 重複防止 + 環境変数で disable 可能。

    PIPELINE_LOG_PUSH=off で完全に無効化。
    """
    import os
    if os.environ.get("PIPELINE_LOG_PUSH", "on").lower() in ("off", "0", "false", "no"):
        return None
    host = host or socket.gethostname()
    handler = ControlPlaneLogHandler(
        control_url=control_url,
        service=service,
        host=host,
        worker_id_getter=worker_id_getter,
        level=level,
        token=token,
    )
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    # 既存の handler に同種のものがあれば skip (再起動なしに reattach されると重複する)
    for h in root.handlers:
        if isinstance(h, ControlPlaneLogHandler):
            return None
    root.addHandler(handler)
    return handler
