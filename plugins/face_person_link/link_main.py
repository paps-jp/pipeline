"""face-person-link plugin: 1 face → person assign / create + 拡散統計 update.

入力: face_person_link_queue (face_id) — image-embed → embed-write 完了で投入
処理:
  1. crawl_embedding_index で shard_id/row_index 取得
  2. shard memmap (= .17 NAS の emb_shard_XX.bin) から 512f embedding read
  3. FAISS HTTP API (= .27:9000) で knn 検索
  4. 候補 face_id → person_id 確認、 cosine > threshold で assign / 該当なしで create
  5. crawl_person stats (face_count, last_seen, centroid moving avg) update
  6. crawl_person_appearance (person × site) UPSERT
出力: {face_id, action: assigned|created, person_id, score}

Phase 1 = 1 face 単位 simple (= 動画 sibling bundle は Phase 2 で local_pid 列追加後)。
"""
from __future__ import annotations

import logging
import os
import sys
from pathlib import Path
from typing import Any

import httpx
import numpy as np

log = logging.getLogger(__name__)

SLUG = "face-person-link"


def _parse_env(path: str) -> dict[str, str]:
    data: dict[str, str] = {}
    p = Path(path)
    if not p.is_file():
        return data
    for line in p.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        v = v.strip()
        if len(v) >= 2 and v[0] in ("'", '"') and v[0] == v[-1]:
            v = v[1:-1]
        data[k.strip()] = v
    return data


def setup(**kwargs) -> dict[str, Any]:
    env_file = kwargs.get("db_env_file", "/mnt/paps-ai/ai/.env")
    env = _parse_env(env_file)
    for k in ("DB_HOST", "DB_PORT", "DB_USER", "DB_PASS", "FAISS_EMBEDS_DIR"):
        if k not in env:
            raise RuntimeError(f"face-person-link: .env に {k} が無い ({env_file})")

    import mariadb
    db_cfg = dict(host=env["DB_HOST"], port=int(env["DB_PORT"]),
                  user=env["DB_USER"], password=env["DB_PASS"],
                  database="delian", autocommit=True)
    db = mariadb.connect(**db_cfg)

    state: dict[str, Any] = {
        "db": db,
        "db_cfg": db_cfg,
        "env": env,
        "shard_dir": env["FAISS_EMBEDS_DIR"],
        "shard_cache": {},
        "http": httpx.Client(timeout=30),
        "faiss_api_url": kwargs.get("faiss_api_url", "http://10.10.50.27:9000").rstrip("/"),
        "threshold_video": float(kwargs.get("threshold_video", 0.65)),
        "threshold_image": float(kwargs.get("threshold_image", 0.75)),
        "topk": int(kwargs.get("topk", 10)),
        "ctrl_url": kwargs.get("control_url", "http://10.10.50.7:8001"),
        "worker_id": f"face-person-link-{os.getpid()}",
        "counter": 0,
    }
    log.info("face-person-link setup done: shard_dir=%s faiss=%s",
             state["shard_dir"], state["faiss_api_url"])
    return state


def _ensure_db(state):
    """DB connection が切れてたら再接続 (= 既存 plugin と同型: autocommit=True)。"""
    try:
        state["db"].ping(reconnect=True)
    except Exception:
        import mariadb
        state["db"] = mariadb.connect(**state["db_cfg"])
    return state["db"]


def _load_shard_embedding(state, shard_id: int, row_index: int) -> np.ndarray | None:
    """shard memmap から 1 row (= 512 float32) を取得。"""
    cache = state["shard_cache"]
    if shard_id not in cache:
        db = _ensure_db(state)
        cur = db.cursor()
        try:
            cur.execute("SELECT size FROM crawl_embedding_shard WHERE id=%s", (shard_id,))
            row = cur.fetchone()
        finally:
            cur.close()
        if not row:
            log.warning("shard %s not in crawl_embedding_shard", shard_id)
            return None
        size = int(row[0])
        path = f"{state['shard_dir']}/emb_shard_{shard_id}.bin"
        try:
            mm = np.memmap(path, dtype=np.float32, mode="r", shape=(size, 512))
            cache[shard_id] = {"mm": mm, "size": size}
        except FileNotFoundError:
            log.warning("shard file not found: %s", path)
            return None
        except Exception as e:
            log.warning("shard memmap open failed %s: %s", path, e)
            return None

    entry = cache[shard_id]
    if row_index >= entry["size"]:
        log.warning("row_index %s >= shard size %s (shard %s)",
                    row_index, entry["size"], shard_id)
        return None
    return np.array(entry["mm"][row_index], dtype=np.float32)


def _faiss_knn(state, embedding: np.ndarray, topk: int) -> tuple[list[int], list[float]]:
    """FAISS HTTP API で knn 検索 → (face_ids, scores)."""
    payload = {
        "vector": [embedding.tolist()],
        "topk": int(topk),
        "nprobe": 64,
    }
    try:
        r = state["http"].post(f"{state['faiss_api_url']}/search", json=payload, timeout=15)
        if r.status_code == 503:
            log.warning("faiss API 503 (not ready)")
            return [], []
        r.raise_for_status()
        data = r.json()
        # faiss_api は単一 vector でも flat list で返す (= [id, id, id], [score, score, score])
        ids = data.get("ids") or []
        scores = data.get("scores") or []
        out_ids: list[int] = []
        out_scores: list[float] = []
        for i, s in zip(ids, scores):
            if i is None or int(i) < 0:
                continue
            out_ids.append(int(i))
            out_scores.append(float(s))
        return out_ids, out_scores
    except Exception as e:
        log.warning("faiss knn failed: %s", e)
        return [], []


def _create_new_person(state, embedding: np.ndarray, face_id: int, is_video: bool) -> int:
    db = _ensure_db(state)
    cur = db.cursor()
    blob = embedding.astype(np.float32).tobytes()
    try:
        cur.execute(
            """INSERT INTO crawl_person
                 (centroid_embedding, face_count, image_face_count, video_face_count,
                  first_seen_at, last_seen_at, representative_face_id)
               VALUES (%s, 1, %s, %s, NOW(), NOW(), %s)""",
            (blob, 0 if is_video else 1, 1 if is_video else 0, face_id),
        )
        return int(cur.lastrowid)
    finally:
        cur.close()


def _assign_face_to_person(state, face_id: int, person_id: int, score: float) -> None:
    db = _ensure_db(state)
    cur = db.cursor()
    try:
        cur.execute(
            "UPDATE crawl_face SET person_id=%s, person_score=%s WHERE id=%s",
            (person_id, float(score), face_id),
        )
    finally:
        cur.close()


def _update_person_stats(state, person_id: int, is_video: bool, embedding: np.ndarray) -> None:
    """face_count + image|video_face_count + last_seen + centroid moving avg。"""
    db = _ensure_db(state)
    cur = db.cursor()
    try:
        # 既存 centroid + face_count 取得
        cur.execute("SELECT centroid_embedding, face_count FROM crawl_person WHERE id=%s",
                    (person_id,))
        row = cur.fetchone()
        if not row:
            return
        centroid = np.frombuffer(row[0], dtype=np.float32).copy()
        n_prev = int(row[1])
        # n_prev は 「この face を含む 前の face_count」 なので、 incremental mean は
        # new_centroid = centroid_prev + (embed - centroid_prev) / (n_prev + 1)
        new_centroid = centroid + (embedding - centroid) / float(n_prev + 1)
        img_inc = 0 if is_video else 1
        vid_inc = 1 if is_video else 0
        cur.execute(
            """UPDATE crawl_person SET
                 centroid_embedding=%s,
                 face_count = face_count + 1,
                 image_face_count = image_face_count + %s,
                 video_face_count = video_face_count + %s,
                 last_seen_at = NOW()
               WHERE id=%s""",
            (new_centroid.astype(np.float32).tobytes(), img_inc, vid_inc, person_id),
        )
    finally:
        cur.close()


def _update_person_appearance(state, person_id: int, face_id: int) -> None:
    """crawl_person_appearance (= person × site) を UPSERT。"""
    db = _ensure_db(state)
    cur = db.cursor()
    try:
        cur.execute(
            """SELECT COALESCE(i.site, v.site) AS site,
                      f.image_id, f.video_id
               FROM crawl_face f
               LEFT JOIN crawl_image i ON f.image_id = i.id
               LEFT JOIN crawl_video v ON f.video_id = v.id
               WHERE f.id=%s""",
            (face_id,),
        )
        row = cur.fetchone()
        if not row or not row[0]:
            return
        site, image_id, video_id = row[0], row[1], row[2]
        img_inc = 1 if image_id else 0
        vid_inc = 1 if video_id else 0
        cur.execute(
            """INSERT INTO crawl_person_appearance
                 (person_id, site, face_count, image_count, video_count, first_seen, last_seen)
               VALUES (%s, %s, 1, %s, %s, NOW(), NOW())
               ON DUPLICATE KEY UPDATE
                 face_count = face_count + 1,
                 image_count = image_count + VALUES(image_count),
                 video_count = video_count + VALUES(video_count),
                 last_seen = NOW()""",
            (person_id, site, img_inc, vid_inc),
        )
    finally:
        cur.close()


def process(task, ctx, state):
    """1 face を処理 → person assign / create + 拡散統計 update."""
    state["counter"] += 1
    face_id = int(task.pk)
    out: dict[str, Any] = {"face_id": face_id, "host": os.uname().nodename if hasattr(os, "uname") else ""}

    db = _ensure_db(state)

    # 1. embedding 取得
    cur = db.cursor()
    cur.execute("SELECT shard_id, row_index FROM crawl_embedding_index WHERE face_id=%s",
                (face_id,))
    row = cur.fetchone()
    cur.close()
    if not row:
        out["skip"] = "no_embedding"
        return out
    shard_id, row_index = int(row[0]), int(row[1])
    embedding = _load_shard_embedding(state, shard_id, row_index)
    if embedding is None:
        out["skip"] = "shard_read_failed"
        return out

    # 2. face 情報 (= image_id / video_id)
    cur = db.cursor()
    cur.execute("SELECT image_id, video_id FROM crawl_face WHERE id=%s", (face_id,))
    fr = cur.fetchone()
    cur.close()
    if not fr:
        out["skip"] = "face_missing"
        return out
    image_id, video_id = fr[0], fr[1]
    is_video = video_id is not None
    threshold = state["threshold_video"] if is_video else state["threshold_image"]

    # 3. FAISS knn 検索
    cand_ids, cand_scores = _faiss_knn(state, embedding, state["topk"])

    # 4. 候補 face_id → person_id 確認
    best_pid: int | None = None
    best_score: float = 0.0
    candidates = [(fid, sc) for fid, sc in zip(cand_ids, cand_scores)
                  if fid != face_id and fid > 0]
    if candidates:
        cur = db.cursor()
        ph = ",".join(["%s"] * len(candidates))
        try:
            cur.execute(f"SELECT id, person_id FROM crawl_face WHERE id IN ({ph})",
                        [c[0] for c in candidates])
            person_map = {int(r[0]): r[1] for r in cur.fetchall()}
        finally:
            cur.close()
        for fid, sc in candidates:
            pid = person_map.get(fid)
            if pid is not None and sc > threshold:
                best_pid = int(pid)
                best_score = float(sc)
                break

    # 5. assign or create (= autocommit=True なので即時反映)
    if best_pid is not None:
        _assign_face_to_person(state, face_id, best_pid, best_score)
        person_id = best_pid
        out["action"] = "assigned"
        out["score"] = best_score
        out["candidates_n"] = len(candidates)
        # 既存 person への assign 時のみ stats を +1 update (= create flow は INSERT で確定済)
        _update_person_stats(state, person_id, is_video, embedding)
    else:
        person_id = _create_new_person(state, embedding, face_id, is_video)
        _assign_face_to_person(state, face_id, person_id, 1.0)
        out["action"] = "created"
        out["score"] = 1.0
        out["candidates_n"] = len(candidates)
    out["person_id"] = person_id

    # 6. 拡散統計 (= person × site) は create/assign 両方で increment
    _update_person_appearance(state, person_id, face_id)

    return out
