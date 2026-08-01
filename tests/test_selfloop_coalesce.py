"""self_loop の tick 積み増し抑止 (coalesce) が claimed も数えること。

unique pk (tick-N-epoch) の自己 enqueue は重複排除されないので、 control plane 側で
「既に cap 件 積まれていれば no-op」に畳む。 pending しか数えないと、 lease が長い
workload で claimed 側に無限に溜まる (2026-08-02 faiss-index-build: lease_secs=86400
のまま worker 世代交代を繰り返し pending 8 / claimed 522)。
"""

from __future__ import annotations

import pytest
from fastapi.testclient import TestClient

from pipeline.config import Settings
from pipeline.control.server import create_app


@pytest.fixture()
def client():
    app = create_app(Settings(db_url="sqlite:///:memory:", mode="dev"))
    with TestClient(app) as c:
        yield c


def _mk(client, slug: str) -> None:
    r = client.post("/api/v1/workloads", json={
        "slug": slug, "name": slug, "enabled": True,
        "executor_type": "shell",
        "executor_config": {"command": ["true"], "source_path": f"/nonexistent/{slug}"},
    })
    assert r.status_code in (200, 201), r.text


def _enq(client, slug: str, pk: str) -> dict:
    r = client.post(f"/api/v1/workloads/{slug}/tasks", json={"pk": pk})
    assert r.status_code in (200, 201), r.text
    return r.json()


def _states(client, slug: str) -> dict:
    return client.get(f"/api/v1/workloads/{slug}/queue").json()["by_state"]


def test_self_loopでなければ抑止しない(client, monkeypatch):
    import pipeline.api.workloads as W
    monkeypatch.setattr(W, "_is_self_loop", lambda w: False)
    _mk(client, "plain")
    for i in range(20):
        _enq(client, "plain", f"tick-{i}")
    assert _states(client, "plain")["pending"] == 20


def test_self_loopはcapで頭打ちになる(client, monkeypatch):
    import pipeline.api.workloads as W
    monkeypatch.setattr(W, "_is_self_loop", lambda w: True)
    monkeypatch.setattr(W, "_SELFLOOP_MAX_PENDING", 8)
    _mk(client, "loop")
    res = [_enq(client, "loop", f"tick-{i}") for i in range(30)]
    assert _states(client, "loop")["pending"] == 8, "cap を超えて積まれている"
    assert sum(r["inserted"] for r in res) == 8
    assert sum(r["duplicates"] for r in res) == 22, "coalesce 分が duplicates で返らない"


def test_claimedも数える(client, monkeypatch):
    """claim 済み (= lease 失効で再 claim される消化待ち) を数えないと claimed 側に溜まる。"""
    import pipeline.api.workloads as W
    monkeypatch.setattr(W, "_is_self_loop", lambda w: True)
    monkeypatch.setattr(W, "_SELFLOOP_MAX_PENDING", 8)
    _mk(client, "loop")
    for i in range(8):
        _enq(client, "loop", f"tick-{i}")

    # 8 件すべてを claim させて pending を 0 にする (= 旧実装ならここで積み増し再開)
    r = client.post("/api/v1/workers", json={"host": "h1", "resources": {}})
    wid = r.json()["id"]
    client.post(f"/api/v1/workers/{wid}/claim", json={"workload_slug": "loop", "limit": 8})
    st = _states(client, "loop")
    assert st.get("pending", 0) == 0 and st.get("claimed", 0) == 8

    for i in range(8, 40):
        _enq(client, "loop", f"tick-{i}")

    st = _states(client, "loop")
    assert st.get("pending", 0) == 0, f"claimed を無視して積み増している: {st}"
    assert st.get("claimed", 0) == 8


def test_消化が進めば再び積める(client, monkeypatch):
    import pipeline.api.workloads as W
    monkeypatch.setattr(W, "_is_self_loop", lambda w: True)
    monkeypatch.setattr(W, "_SELFLOOP_MAX_PENDING", 4)
    _mk(client, "loop")
    for i in range(4):
        _enq(client, "loop", f"tick-{i}")
    assert _states(client, "loop")["pending"] == 4

    r = client.post("/api/v1/workers", json={"host": "h1", "resources": {}})
    wid = r.json()["id"]
    tasks = client.post(f"/api/v1/workers/{wid}/claim",
                        json={"workload_slug": "loop", "limit": 2}).json()["tasks"]
    client.post(f"/api/v1/workers/{wid}/complete",
                json={"workload_slug": "loop", "pks": [t["pk"] for t in tasks]})

    _enq(client, "loop", "tick-new-1")
    _enq(client, "loop", "tick-new-2")
    _enq(client, "loop", "tick-new-3")
    st = _states(client, "loop")
    assert st.get("pending", 0) + st.get("claimed", 0) == 4, f"cap まで戻せていない: {st}"
