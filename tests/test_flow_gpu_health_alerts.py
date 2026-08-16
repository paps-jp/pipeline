"""Flow 赤ボックスの GPU 死活アラートのテスト (2026-08-16)。

なぜ専用の probe が要るか: 2026-08-15 に ai-gpu8 が Xid 119 (GSP timeout) で
`GPU requires reset` に落ちたが、 **19 時間誰にも見えなかった**。 このとき

  - nvidia-smi は rc=0 で応答し、 行も返す (= メトリクスは流れ続ける)
  - 温度/電力/クロックだけが `[N/A]` = DB 上 NULL になる
  - memory.used/total はハング直前の値で凍結するので、 UI の GPU カードは
    VRAM と Mem 帯域を「それらしい数字」で表示し続ける

つまり **既存のどの指標を見ても正常**で、 カードに数字が出ていること自体が
生存の証明にならなかった。

守る性質:
  - 温度が全サンプルで NULL の host だけを crit にする。
  - util_pct / mem_used_mb は判定に使わない。 前者は MPS 下で service.py が
    power から逆算して埋めるため正常時も NULL になり得る。 後者はハング中も
    凍結値が入り続けるため、 見ると必ず「正常」と誤答する。
  - サンプルが少ないうちは鳴らさない (= 起動直後の host を殺さない)。
  - 非 active worker の古い行を拾わない。
"""

from __future__ import annotations

import datetime as dt

import pytest

from pipeline.api import flow
from pipeline.db.sqlite import SqliteDatabase

WINDOW_MIN = 5
MIN_SAMPLES = 20


@pytest.fixture()
def db():
    d = SqliteDatabase("sqlite:///:memory:")
    d.ensure_schema()
    yield d
    d.close()


def _seed(db, host: str, *, n: int, temp: bool, state: str = "active",
          age_min: float = 0.0, worker: str = "a4") -> None:
    """host に n 件のメトリクスを入れる。 temp=False は Xid 119 型。

    worker_id は `w_<host の - を _ にしたもの>_<worker>` = 本番と同じ規約。
    同 host に複数 worker を置くときは worker を変える。
    """
    wid = "w_" + host.replace("-", "_") + "_" + worker
    now = dt.datetime.now(dt.timezone.utc)
    with db.transaction() as conn:
        conn.execute(
            "INSERT INTO workers (id, host, state, started_at) VALUES (?,?,?,?)",
            (wid, host, state, now.isoformat()),
        )
        for i in range(n):
            ts = (now - dt.timedelta(minutes=age_min, seconds=i)).isoformat()
            conn.execute(
                "INSERT INTO worker_metrics "
                "(worker_id, gpu_idx, ts, temp_c, util_pct, mem_used_mb, "
                " mem_total_mb, power_w) VALUES (?,?,?,?,?,?,?,?)",
                (wid, 0, ts,
                 43.0 if temp else None,          # temp_c   ← 唯一の正直な信号
                 0.0 if temp else None,           # util_pct
                 7789,                            # mem_used_mb (ハング中も凍結値)
                 16311,                           # mem_total_mb
                 12.4 if temp else None),         # power_w
            )


def _probe(db):
    return flow._probe_gpu_health(db, WINDOW_MIN, MIN_SAMPLES)


def test_incident_replay_flags_only_the_dead_host(db):
    """2026-08-15 の形: gpu8 だけ温度が全 NULL、 他は正常。"""
    _seed(db, "ai-gpu1", n=60, temp=True)
    _seed(db, "ai-gpu4", n=60, temp=True)
    _seed(db, "ai-gpu8", n=60, temp=False)
    _seed(db, "ai-gpu9", n=60, temp=True)

    alerts = _probe(db)
    assert [a["name"] for a in alerts] == ["ai-gpu8"]
    a = alerts[0]
    assert a["kind"] == "gpu" and a["severity"] == "crit"
    assert "qm stop" in a["detail"]          # 復旧手順を赤ボックスに載せる


def test_healthy_fleet_is_silent(db):
    """平常時に赤ボックスを鳴らさない。"""
    for h in ("ai-gpu1", "ai-gpu4", "ai-gpu8"):
        _seed(db, h, n=60, temp=True)
    assert _probe(db) == []


def test_frozen_vram_does_not_look_healthy(db):
    """mem_used/mem_total は入っているのに温度が無い = 今回の障害そのもの。
    VRAM が埋まっていることを健全さの根拠にしない。"""
    _seed(db, "ai-gpu8", n=60, temp=False)
    with db.transaction() as conn:
        row = conn.execute(
            "SELECT COUNT(*) c FROM worker_metrics WHERE mem_used_mb IS NOT NULL"
        ).fetchone()
    assert row["c"] == 60          # VRAM は 100% 埋まっている
    assert [a["name"] for a in _probe(db)] == ["ai-gpu8"]


def test_sample_poor_host_is_not_flagged(db):
    """起動直後で数件しか無い host を「温度ゼロ件」で殺さない。"""
    _seed(db, "ai-gpu8", n=MIN_SAMPLES - 1, temp=False)
    assert _probe(db) == []


def test_partial_temperature_is_not_flagged(db):
    """一部でも温度が取れていれば GPU は応答している (= 取りこぼしで鳴らさない)。

    同 host の別 worker が 1 件でも温度を報告していれば、 host 単位の temp_ok は
    非ゼロになる (= 判定は worker 単位でなく host 単位)。
    """
    _seed(db, "ai-gpu8", n=40, temp=False, worker="a4")
    _seed(db, "ai-gpu8", n=1, temp=True, worker="a5")
    assert _probe(db) == []


def test_stale_samples_outside_window_are_ignored(db):
    """窓外の古い行で判定しない (= 復旧後もいつまでも赤いままにしない)。"""
    _seed(db, "ai-gpu8", n=60, temp=False, age_min=WINDOW_MIN + 10)
    assert _probe(db) == []


def test_inactive_worker_rows_are_ignored(db):
    """deregister された worker の残骸で鳴らさない。"""
    _seed(db, "ai-gpu8", n=60, temp=False, state="stale")
    assert _probe(db) == []


def test_empty_db_is_silent(db):
    assert _probe(db) == []


def test_cached_alerts_never_block_the_request(db, monkeypatch):
    """_gpu_alerts は cache を即返し、 probe は background に投げる。"""
    calls: list[int] = []

    def slow(_db, _w, _m):
        calls.append(1)
        return [{"name": "gpu:x", "kind": "gpu", "severity": "crit"}]

    monkeypatch.setattr(flow, "_probe_gpu_health", slow)
    monkeypatch.setattr(flow, "_gpu_health",
                        {"ts": 0.0, "alerts": [], "refreshing": False})

    assert flow._gpu_alerts(db) == []      # 1 回目は cache 空を即返す
    for _ in range(200):                   # background thread の完了待ち
        if flow._gpu_health["alerts"]:
            break
        import time as _t
        _t.sleep(0.01)
    assert [a["name"] for a in flow._gpu_alerts(db)] == ["gpu:x"]
    assert len(calls) == 1                 # TTL 内は再 probe しない
