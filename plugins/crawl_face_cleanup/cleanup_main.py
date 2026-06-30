"""crawl-face-cleanup: un-embeddable な死蔵 face を 1 日 1 回まとめて削除する self_loop。

背景 (2026-06-30):
  crawler が作る face crop の一部 (極小 crop / 偽 bbox) は embed 不可能で、
  embed worker が QC drop → `crawl_face.adaface_ready=2` にマークする。 これらは
  person_id NULL / adaface_norm NULL / 検索 index 未登録の純粋なゴミだが、
  放置すると crawl_face 行 (DB) と MinIO crop オブジェクトが無制限に増える。

動作:
  self_loop で check_interval_s 毎に tick。 marker テーブル (ai_cleanup_marker) の
  last_run から run_interval_hours (既定 24) 経過していれば、 adaface_ready=2 の
  face を batch で:
    1. MinIO crop オブジェクトを削除 (minio_key)
    2. crawl_face の行を DELETE
  まとめて消す。 経過していなければ即 skip (= 軽い marker SELECT のみ)。

安全:
  - 削除対象は adaface_ready=2 のみ (= embed worker が「embed 不可」 と確定したもの)。
  - dry_run=1 で件数カウントのみ (削除しない)。
  - max_deletes_per_run で 1 回あたりの上限。 超えたら次回継続。
  - 可逆性: 削除済は復元不可だが、 adaface_ready=2 自体は
    `UPDATE crawl_face SET adaface_ready=0 WHERE adaface_ready=2` で「マーク取消」 可能
    (= 削除前にこれを実行すれば対象から外れる)。
"""

from __future__ import annotations

import json
import logging
import os
import time
import urllib.request
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

log = logging.getLogger(__name__)

DEFAULT_CONTROL_URL = "http://127.0.0.1:8001"
MARKER_NAME = "crawl_face_adaface2"


def _load_env_file(path: str | None) -> dict[str, str]:
    if not path:
        return {}
    p = Path(path).expanduser()
    if not p.exists():
        return {}
    out: dict[str, str] = {}
    for line in p.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out


def _setup_minio(env: dict[str, str], kwargs: dict[str, Any]):
    endpoint = env.get("MINIO_ENDPOINT") or kwargs.get("minio_endpoint")
    access = env.get("MINIO_ACCESS_KEY") or kwargs.get("minio_access_key")
    secret = env.get("MINIO_SECRET_KEY") or kwargs.get("minio_secret_key")
    bucket = env.get("MINIO_BUCKET") or kwargs.get("minio_bucket")
    if not all([endpoint, access, secret, bucket]):
        return None, None
    try:
        from minio import Minio  # type: ignore
        secure = (env.get("MINIO_VERIFY_TLS", "true").lower() in ("true", "1", "yes"))
        client = Minio(endpoint, access_key=access, secret_key=secret, secure=secure)
        return client, bucket
    except Exception as e:
        log.warning("MinIO setup failed: %s", e)
        return None, None


def _self_enqueue_next_tick(control_url: str, workload_slug: str, tick_id: int) -> None:
    pk = f"tick-{tick_id}-{int(time.time())}"
    req = urllib.request.Request(
        f"{control_url}/api/v1/workloads/{workload_slug}/tasks",
        data=json.dumps({"pk": pk}).encode("utf-8"),
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    try:
        with urllib.request.urlopen(req, timeout=5) as r:
            r.read()
    except Exception as e:
        log.warning("cleanup self-enqueue failed: %s", e)


def _ensure_db_alive(state: dict) -> Any:
    db = state.get("db")
    try:
        cur = db.cursor()
        cur.execute("SELECT 1")
        cur.fetchone()
        cur.close()
        return db
    except Exception as e:
        log.warning("DB dead (%s); reconnecting", e)
        import mariadb  # type: ignore
        db = mariadb.connect(**state["db_cfg"], autocommit=True)
        state["db"] = db
        return db


def _ensure_marker_table(db) -> None:
    cur = db.cursor()
    cur.execute(
        "CREATE TABLE IF NOT EXISTS ai_cleanup_marker ("
        "  name VARCHAR(64) NOT NULL PRIMARY KEY,"
        "  last_run DATETIME NULL,"
        "  last_deleted BIGINT NOT NULL DEFAULT 0"
        ") ENGINE=InnoDB DEFAULT CHARSET=utf8mb4"
    )
    cur.close()


def _read_last_run(db) -> datetime | None:
    cur = db.cursor()
    cur.execute("SELECT last_run FROM ai_cleanup_marker WHERE name=%s", (MARKER_NAME,))
    row = cur.fetchone()
    cur.close()
    if not row or row[0] is None:
        return None
    val = row[0]
    if isinstance(val, datetime):
        return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
    try:
        return datetime.fromisoformat(str(val).replace("Z", "+00:00")).replace(tzinfo=timezone.utc)
    except Exception:
        return None


def _write_marker(db, deleted: int) -> None:
    cur = db.cursor()
    cur.execute(
        "INSERT INTO ai_cleanup_marker (name, last_run, last_deleted) "
        "VALUES (%s, UTC_TIMESTAMP(), %s) "
        "ON DUPLICATE KEY UPDATE last_run=UTC_TIMESTAMP(), last_deleted=%s",
        (MARKER_NAME, deleted, deleted),
    )
    cur.close()


def setup(**kwargs) -> dict[str, Any]:
    env = _load_env_file(kwargs.get("db_env_file"))
    db_cfg = {
        "host": env.get("DB_HOST") or kwargs.get("db_host"),
        "port": int(env.get("DB_PORT") or kwargs.get("db_port") or 3306),
        "user": env.get("DB_USER") or kwargs.get("db_user"),
        "password": env.get("DB_PASS") or kwargs.get("db_pass"),
        "database": env.get("DB_NAME") or kwargs.get("db_name"),
    }
    if not all(db_cfg[k] for k in ("host", "user", "password", "database")):
        raise RuntimeError("crawl-face-cleanup: DB credentials required")
    import mariadb  # type: ignore
    db = mariadb.connect(**db_cfg)
    db.autocommit = True
    _ensure_marker_table(db)
    log.info("cleanup: connected MariaDB %s/%s", db_cfg["host"], db_cfg["database"])

    minio_client, minio_bucket = _setup_minio(env, kwargs)
    if minio_client is None:
        log.warning("cleanup: MinIO 未設定 — crop 削除は skip (DB 行のみ削除)")

    state = {
        "db": db,
        "db_cfg": db_cfg,
        "minio": minio_client,
        "minio_bucket": minio_bucket,
        "control_url": (kwargs.get("control_url") or DEFAULT_CONTROL_URL).rstrip("/"),
        "workload_slug": kwargs.get("workload_slug") or "crawl-face-cleanup",
        # 削除対象の adaface_ready 値 (= embed worker が「embed 不可」 と確定する値)
        "target_state": int(kwargs.get("target_state") or 2),
        "run_interval_hours": int(kwargs.get("run_interval_hours") or 24),
        "check_interval_s": int(kwargs.get("check_interval_s") or 300),
        # 削除対象を「bbox 短辺 < delete_max_face_px の真の極小 face」 に限定する安全弁。
        # adaface_ready=2 の大半は image-embed の再検出バグ (= face crop を再 detect して
        # 99% silent skip、 [[image-embed-video-redetect-bug]]) による誤マークで、 bbox>=50
        # + kps_json 有りなら修正後 re-embed で回収可能。 それらを消さないため bbox で絞る。
        # 0 で無効 (= 全 adaface_ready=2 を対象、 非推奨)。
        "delete_max_face_px": int(kwargs.get("delete_max_face_px") or 50),
        "batch_size": int(kwargs.get("batch_size") or 1000),
        "max_deletes_per_run": int(kwargs.get("max_deletes_per_run") or 2_000_000),
        "dry_run": bool(int(kwargs.get("dry_run") or 0)),
        # failed queue reaper: 各 <slug>_queue (mariadb backend) の state='failed' 行を
        # retention 日数より古ければ削除。 complete は即 DELETE されるが failed は max_attempts
        # 到達で永久残置するため (reaper 無し)、 ここで掃除する。 0 で無効。
        "reap_failed_enabled": bool(int(kwargs.get("reap_failed_enabled") or 1)),
        "failed_retention_days": int(kwargs.get("failed_retention_days") or 7),
        "counter": 0,
        "hostname": os.environ.get("PIPELINE_WORKER_HOSTNAME") or os.uname().nodename,
    }
    try:
        _self_enqueue_next_tick(state["control_url"], state["workload_slug"], 1)
        log.info("cleanup: bootstrap tick-1 enqueued (interval=%dh, check=%ds, dry_run=%s)",
                 state["run_interval_hours"], state["check_interval_s"], state["dry_run"])
    except Exception as e:
        log.warning("cleanup: bootstrap enqueue failed: %s", e)
    return state


def _run_cleanup_pass(state: dict, out: dict) -> int:
    """adaface_ready=target_state の face を MinIO crop + DB 行ごと batch 削除。
    削除した DB 行数を返す。 dry_run なら件数カウントのみ。"""
    db = _ensure_db_alive(state)
    target = state["target_state"]
    batch = state["batch_size"]
    cap = state["max_deletes_per_run"]
    minio = state["minio"]
    bucket = state["minio_bucket"]
    dry = state["dry_run"]

    # 削除対象は「真の極小 face」 のみに限定する WHERE 条件。
    # bbox 短辺 < delete_max_face_px の face だけ消す (= 回収可能な誤マークを守る)。
    max_px = int(state.get("delete_max_face_px") or 0)
    if max_px > 0:
        size_cond = ("AND LEAST(bbox_x2-bbox_x1, bbox_y2-bbox_y1) < %s" % max_px)
    else:
        size_cond = ""

    total_db = 0
    total_minio = 0
    started = time.time()
    while total_db < cap:
        cur = db.cursor()
        cur.execute(
            "SELECT id, minio_key FROM crawl_face "
            f"WHERE adaface_ready=%s {size_cond} ORDER BY id DESC LIMIT %s",
            (target, batch),
        )
        rows = cur.fetchall()
        cur.close()
        if not rows:
            break
        ids = [r[0] for r in rows]
        keys = [r[1] for r in rows if r[1]]
        if dry:
            total_db += len(ids)
            # dry_run は 1 batch だけ見て概算 (= 全件 scan しない)
            out["dry_run_sample"] = {"batch_rows": len(ids), "with_key": len(keys),
                                     "size_filter": size_cond or "(none)"}
            break
        # 1. MinIO crop 削除 (best-effort、 失敗しても DB 行は消す)
        if minio is not None and keys:
            try:
                from minio.deleteobjects import DeleteObject  # type: ignore
                errs = minio.remove_objects(bucket, [DeleteObject(k) for k in keys])
                err_n = sum(1 for _ in errs)  # iterator を消費 (= 実際に削除実行)
                total_minio += len(keys) - err_n
            except Exception as e:
                log.warning("cleanup: minio remove_objects failed: %s", str(e)[:120])
        # 2. DB 行削除 (= 同じ極小条件で二重ガード)
        cur = db.cursor()
        ph = ",".join(["%s"] * len(ids))
        cur.execute(
            f"DELETE FROM crawl_face WHERE id IN ({ph}) AND adaface_ready=%s {size_cond}",
            (*ids, target),
        )
        total_db += cur.rowcount
        cur.close()

    out["db_deleted"] = total_db
    out["minio_deleted"] = total_minio
    out["elapsed_s"] = round(time.time() - started, 1)
    return total_db


def _valid_queue_table(name: str) -> bool:
    """インジェクション防止: slug 由来の queue_table 名を簡易チェック。"""
    return bool(name) and name.replace("_", "").isalnum()


def _reap_failed_queues(state: dict, out: dict) -> int:
    """各 workload の <slug>_queue (mariadb backend) から、 retention 日数より古い
    state='failed' 行を削除する。 削除総数を返す。 dry_run なら件数カウントのみ。

    sqlite backend queue (= control plane の primary DB 上) は worker から直接触れない
    ため skip (= supervisor/cleanup 等の小さい queue のみ、 影響軽微)。
    """
    db = _ensure_db_alive(state)
    retention = int(state["failed_retention_days"])
    dry = state["dry_run"]
    # control plane から workload 一覧 (= queue_table + queue_backend) を取得
    try:
        with urllib.request.urlopen(
            f"{state['control_url']}/api/v1/workloads", timeout=10
        ) as r:
            wls = json.loads(r.read()).get("workloads", [])
    except Exception as e:
        out["reap_error"] = f"workloads fetch failed: {e}"[:160]
        return 0

    total = 0
    per_queue: dict[str, int] = {}
    skipped_sqlite = 0
    for w in wls:
        if (w.get("queue_backend") or "sqlite") != "mariadb":
            skipped_sqlite += 1
            continue
        qt = w.get("queue_table") or ""
        if not _valid_queue_table(qt):
            continue
        try:
            cur = db.cursor()
            if dry:
                cur.execute(
                    f"SELECT COUNT(*) FROM {qt} WHERE state='failed' "
                    f"AND updated_at < UTC_TIMESTAMP() - INTERVAL %s DAY",
                    (retention,),
                )
                n = int(cur.fetchone()[0])
            else:
                cur.execute(
                    f"DELETE FROM {qt} WHERE state='failed' "
                    f"AND updated_at < UTC_TIMESTAMP() - INTERVAL %s DAY",
                    (retention,),
                )
                n = cur.rowcount
            cur.close()
            if n:
                per_queue[qt] = n
                total += n
        except Exception as e:
            log.debug("reap %s failed: %s", qt, str(e)[:80])
    out["reap_failed"] = {"total": total, "per_queue": per_queue,
                          "retention_days": retention, "dry_run": dry,
                          "skipped_sqlite": skipped_sqlite}
    return total


def process(task, ctx, state):
    state["counter"] += 1
    started = time.time()
    out: dict[str, Any] = {"tick": state["counter"], "host": state["hostname"]}
    db = _ensure_db_alive(state)

    # 前回実行からの経過時間で daily 判定
    last_run = _read_last_run(db)
    now = datetime.now(timezone.utc)
    interval_s = state["run_interval_hours"] * 3600
    due = (last_run is None) or ((now - last_run).total_seconds() >= interval_s)
    out["last_run"] = last_run.isoformat() if last_run else None
    out["due"] = due

    if due:
        log.info("cleanup: due — running pass (target adaface_ready=%d, dry_run=%s)",
                 state["target_state"], state["dry_run"])
        deleted = _run_cleanup_pass(state, out)
        # failed queue reaper (= 同じ日次 cycle に相乗り)
        reaped = 0
        if state.get("reap_failed_enabled"):
            try:
                reaped = _reap_failed_queues(state, out)
            except Exception as e:
                log.warning("cleanup: reap_failed_queues failed: %s", str(e)[:120])
                out["reap_error"] = str(e)[:160]
        if not state["dry_run"]:
            _write_marker(db, deleted)
        log.info("cleanup: pass done db_deleted=%d minio_deleted=%d failed_reaped=%d elapsed=%.1fs",
                 out.get("db_deleted", 0), out.get("minio_deleted", 0), reaped, out.get("elapsed_s", 0))
    else:
        remain = int(interval_s - (now - last_run).total_seconds())
        out["next_due_in_s"] = remain
        log.debug("cleanup: not due (next in %ds)", remain)

    out["dispatch_secs"] = round(time.time() - started, 2)
    # pacing: check_interval_s 毎に再 tick
    sleep_s = max(1, state["check_interval_s"] - int(out["dispatch_secs"]))
    time.sleep(sleep_s)
    _self_enqueue_next_tick(state["control_url"], state["workload_slug"], state["counter"] + 1)
    out["next_tick_scheduled"] = True
    return out


def teardown(state) -> None:
    try:
        db = state.get("db")
        if db is not None:
            db.close()
    except Exception:
        pass
