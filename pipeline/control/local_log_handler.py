"""control plane (= .7 pipeline-oss.service) の log を service_logs テーブルに直接書く Handler.

daemon は HTTP POST で送信するが、 control plane は同一プロセス内で DB を持ってるので
直接 INSERT する方が軽い + 循環 (自分の log 送信が log になる) を避けられる。

filter:
  - httpx / urllib3 / uvicorn.access の noise は除外
  - service_logs repository 自身の log も除外 (= 無限ループ防止)
"""

from __future__ import annotations

import logging
import queue
import socket
import threading
import time
from datetime import datetime, timezone

_NOISY_LOGGERS = {
    "httpx", "httpcore", "urllib3",
    "uvicorn.access",
    "pipeline.repositories.service_logs",     # 自身の INSERT を log すると無限ループ
    "pipeline.api.service_logs",
}


def _utcnow_iso() -> str:
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds")


class LocalDbLogHandler(logging.Handler):
    """同一プロセスの DB に service_logs を INSERT する handler.

    別 thread で batch 化して flush するため、 logging 呼出は blocking しない。
    """

    def __init__(
        self,
        *,
        db,
        service: str = "pipeline-oss-control",
        host: str | None = None,
        flush_interval_s: float = 2.0,
        batch_max: int = 200,
        queue_maxsize: int = 10000,
        level: int = logging.INFO,
    ) -> None:
        super().__init__(level=level)
        self._db = db
        self._service = service
        self._host = host or socket.gethostname()
        self._flush_interval_s = flush_interval_s
        self._batch_max = batch_max
        self._queue: queue.Queue[dict] = queue.Queue(maxsize=queue_maxsize)
        self._stop = threading.Event()
        self._thread = threading.Thread(target=self._run, name="LocalDbLogger", daemon=True)
        self._thread.start()

    def emit(self, record: logging.LogRecord) -> None:
        # noisy logger は drop
        for prefix in _NOISY_LOGGERS:
            if record.name == prefix or record.name.startswith(prefix + "."):
                return
        try:
            msg = self.format(record)
        except Exception:
            return
        item = {
            "ts": _utcnow_iso(),
            "host": self._host,
            "service": self._service,
            "worker_id": None,
            "level": record.levelname,
            "logger": record.name,
            "message": msg,
            "exc_info": (
                self.formatter.formatException(record.exc_info)
                if record.exc_info and self.formatter else None
            ),
        }
        try:
            self._queue.put_nowait(item)
        except queue.Full:
            pass

    def _run(self) -> None:
        from pipeline.repositories.service_logs import ServiceLogsRepository
        repo = ServiceLogsRepository(self._db)
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
                try:
                    repo.insert_many(batch)
                except Exception:
                    # DB エラー時は drop (= log を log すると loop)
                    pass
                batch = []
                last_flush = now
        # final flush
        try:
            while True:
                batch.append(self._queue.get_nowait())
        except queue.Empty:
            pass
        if batch:
            try:
                repo.insert_many(batch)
            except Exception:
                pass

    def close(self) -> None:
        self._stop.set()
        try:
            self._thread.join(timeout=3)
        except Exception:
            pass
        super().close()


def attach_control_plane_logger(db, *, service: str = "pipeline-oss-control",
                                level: int = logging.INFO) -> LocalDbLogHandler | None:
    import os
    if os.environ.get("PIPELINE_LOG_PUSH", "on").lower() in ("off", "0", "false", "no"):
        return None
    handler = LocalDbLogHandler(db=db, service=service, level=level)
    handler.setFormatter(logging.Formatter("%(message)s"))
    root = logging.getLogger()
    for h in root.handlers:
        if isinstance(h, LocalDbLogHandler):
            return None
    root.addHandler(handler)
    return handler
