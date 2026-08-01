"""goodput allocator の agent 対応 (census + template 経由 actuator)。

goodput は control plane の workload_filter を actuator にしていたが、 agent 管理
ホストの子は spawn 時の env filter だけで動き workload_filter=None のままなので、
server の assigned_workers も filter ベースの cur も 0 に見える。 その状態で apply
すると全 agent 子を idle と誤認して別 slug の filter を差し、 1 プロセスが 2 系統の
モデルを抱えて VRAM 見積りが崩壊する。

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

_SM = Path(__file__).resolve().parents[1] / "plugins" / "pipeline_supervisor" / "supervisor_main.py"
pytestmark = pytest.mark.skipif(not _SM.exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def sm():
    spec = importlib.util.spec_from_file_location("supervisor_main_under_test", _SM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _census(hosts: dict, children: dict) -> dict:
    """hosts: {host: (vram_total, template)}, children: {(host, slug): n}"""
    per_slug: dict[str, int] = {}
    for (h, s), n in children.items():
        per_slug[s] = per_slug.get(s, 0) + n
    return {
        "hosts": {h: {"host": h, "last_vram_total_mb": vt, "template": {"workloads": tmpl}}
                  for h, (vt, tmpl) in hosts.items()},
        "per_slug": per_slug,
        "per_host": dict(children),
    }


def _gpu(count: int, vram: int) -> dict:
    return {"count": count, "gpu": True, "vram_mb": vram}


def test_不足分をagentホストへ均等に配る(sm):
    census = _census(
        {"ai-gpu1": (16311, {"image-embed": _gpu(0, 2500)}),
         "ai-gpu3": (16311, {"image-embed": _gpu(0, 2500)})},
        {},
    )
    acts = sm._goodput_agent_retarget("http://x", "image-embed", 4, census,
                                      wl={}, headroom_mb=1800, do_apply=False)
    by_host = {a["host"]: a["to"] for a in acts}
    assert sum(by_host.values()) == 4
    assert by_host == {"ai-gpu1": 2, "ai-gpu3": 2}, f"偏っている: {by_host}"
    assert all(a["applied"] is False for a in acts), "DRY なのに applied"


def test_VRAM予算を超える台数は書かない(sm):
    # 10240MB - 1800 = 8440 → 2500MB の子は 3 台まで
    census = _census({"ai-gpu4": (10240, {"image-embed": _gpu(0, 2500)})}, {})
    acts = sm._goodput_agent_retarget("http://x", "image-embed", 10, census,
                                      wl={}, headroom_mb=1800, do_apply=False)
    assert acts[0]["to"] == 3, f"予算超過を書いている: {acts}"


def test_他slugのtemplateも予算に勘定する(sm):
    # 8440 の予算のうち hash が 2*2500=5000 を占有 → embed は 1 台分しか残らない
    census = _census(
        {"ai-gpu4": (10240, {"image-embed": _gpu(0, 2500),
                             "image-hash-extract": _gpu(2, 2500)})}, {},
    )
    acts = sm._goodput_agent_retarget("http://x", "image-embed", 10, census,
                                      wl={}, headroom_mb=1800, do_apply=False)
    assert acts[0]["to"] == 1, f"他 slug の占有を無視している: {acts}"


def test_host_affinity外のホストには配らない(sm):
    census = _census(
        {"ai-gpu1": (16311, {"image-embed": _gpu(0, 2500)}),
         "ai-gpu9": (16311, {"image-embed": _gpu(0, 2500)})},
        {},
    )
    acts = sm._goodput_agent_retarget("http://x", "image-embed", 4, census,
                                      wl={"host_affinity": ["ai-gpu1"]},
                                      headroom_mb=1800, do_apply=False)
    assert {a["host"] for a in acts} == {"ai-gpu1"}


def test_max_concurrent_per_hostを尊重する(sm):
    census = _census({"ai-gpu1": (16311, {"image-embed": _gpu(0, 1000)})}, {})
    acts = sm._goodput_agent_retarget("http://x", "image-embed", 9, census,
                                      wl={"max_concurrent_per_host": 2},
                                      headroom_mb=1800, do_apply=False)
    assert acts[0]["to"] == 2


def test_過剰なら多いホストから減らす(sm):
    census = _census(
        {"ai-gpu1": (16311, {"image-embed": _gpu(5, 2500)}),
         "ai-gpu3": (16311, {"image-embed": _gpu(1, 2500)})},
        {("ai-gpu1", "image-embed"): 5, ("ai-gpu3", "image-embed"): 1},
    )
    acts = sm._goodput_agent_retarget("http://x", "image-embed", 2, census,
                                      wl={}, headroom_mb=1800, do_apply=False)
    by_host = {a["host"]: a["to"] for a in acts}
    assert sum(by_host.get(h, n) for h, n in (("ai-gpu1", 5), ("ai-gpu3", 1))) == 2
    assert by_host["ai-gpu1"] < 5, "多い方から削っていない"


def test_変化が無ければ何も書かない(sm):
    census = _census(
        {"ai-gpu1": (16311, {"image-embed": _gpu(2, 2500)})},
        {("ai-gpu1", "image-embed"): 2},
    )
    acts = sm._goodput_agent_retarget("http://x", "image-embed", 2, census,
                                      wl={}, headroom_mb=1800, do_apply=False)
    assert acts == [], "同値を書き直している (PUT とログのノイズ)"


def test_agentホストが無ければ何もしない(sm):
    census = _census({}, {})
    acts = sm._goodput_agent_retarget("http://x", "image-embed", 4, census,
                                      wl={}, headroom_mb=1800, do_apply=False)
    assert acts == []


def test_census_は停止中agentを除外する(sm, monkeypatch):
    fresh = "2026-08-01T12:00:00+00:00"
    payload = {"agents": [
        {"host": "alive", "last_seen_at": fresh,
         "last_children": [{"workload": "image-embed", "alive": True},
                           {"workload": "image-embed", "alive": False}]},
        {"host": "dead", "last_seen_at": "2026-07-31T00:00:00+00:00",
         "last_children": [{"workload": "image-embed", "alive": True}] * 5},
    ]}
    monkeypatch.setattr(sm, "_http_get_json", lambda *a, **k: payload)

    class _FakeNow(sm._dt.datetime):
        @classmethod
        def now(cls, tz=None):
            return sm._dt.datetime.fromisoformat("2026-08-01T12:00:30+00:00")

    monkeypatch.setattr(sm._dt, "datetime", _FakeNow)
    c = sm._goodput_agent_census("http://x", max_age_s=120.0)

    assert set(c["hosts"]) == {"alive"}, "停止中 agent を幻のキャパシティに数えている"
    assert c["per_slug"] == {"image-embed": 1}, "alive=False の子まで数えている"


def test_census_は取得失敗でも空を返す(sm, monkeypatch):
    def _boom(*a, **k):
        raise RuntimeError("control plane down")
    monkeypatch.setattr(sm, "_http_get_json", _boom)
    c = sm._goodput_agent_census("http://x")
    assert c == {"hosts": {}, "per_slug": {}, "per_host": {}}
