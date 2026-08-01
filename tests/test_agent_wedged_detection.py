"""wedged 子 (= 生きているが executor build が通らず仕事が成立しない子) の検知。

プロセスが生きているだけで台数カウントを満たしてしまうと、 VRAM を握ったまま
永久に温存され、 そのホストの GPU spawn が VRAM ゲートで全部弾かれる
(2026-08-01 ai-gpu1: wedged 2 体が 15.7GB を保持し image-embed が数時間 0 件成功)。
"""

from __future__ import annotations

import json
import time

import pytest

from pipeline.agent.desired import Desired, WorkloadDesired
from pipeline.agent.proxy import AggregatorProxy, ChildHealth
from pipeline.agent.supervisor import AgentSupervisor, Child


# ---------------- proxy: run 結果から健全性を組み立てる ----------------

def _proxy() -> AggregatorProxy:
    return AggregatorProxy(upstream="http://127.0.0.1:0", port=0)


def _run_body(success: bool, error: str | None = None) -> bytes:
    return json.dumps({"workload_slug": "image-embed", "pk": "x",
                       "success": success, "error": error}).encode()


def test_proxy_がbuild_errorを子ごとに数える():
    p = _proxy()
    path = "/api/v1/workers/w_ai_gpu1_a1/runs"
    for _ in range(3):
        p._observe_run(path, _run_body(False, "executor build error: CUBLAS_ALLOC_FAILED"))
    h = p.health("w_ai_gpu1_a1")
    assert h is not None and h.build_errors == 3
    assert h.last_success_at is None
    assert p.health("w_ai_gpu1_a2") is None, "他の子に混ざっている"


def test_proxy_は成功runでbuild_errorをリセットする():
    p = _proxy()
    path = "/api/v1/workers/w_x_a1/runs"
    p._observe_run(path, _run_body(False, "executor build error: boom"))
    p._observe_run(path, _run_body(True))
    h = p.health("w_x_a1")
    assert h.build_errors == 0, "1 件でも通ったなら build は成立している"
    assert h.last_success_at is not None


def test_proxy_はbuild以外の失敗をbuild_errorに数えない():
    p = _proxy()
    path = "/api/v1/workers/w_x_a1/runs"
    p._observe_run(path, _run_body(False, "画像が壊れています"))
    assert p.health("w_x_a1").build_errors == 0, "タスク起因の失敗を wedged 扱いしている"


@pytest.mark.parametrize("body", [None, b"", b"not json", b'{"no_success_key": 1}'])
def test_proxy_の観測は壊れたbodyで例外を出さない(body):
    p = _proxy()
    p._observe_run("/api/v1/workers/w_x_a1/runs", body)  # 中継を絶対に壊さない
    assert p.health("w_x_a1") is None


# ---------------- supervisor: wedged 判定と置換 ----------------

class _FakeProc:
    def __init__(self) -> None:
        self.pid = -1
        self.terminated = 0

    def poll(self):
        return None      # 常に生きている

    def terminate(self):
        self.terminated += 1

    def kill(self):
        self.terminated += 1


def _sup(health: dict[str, ChildHealth], **kw) -> AgentSupervisor:
    return AgentSupervisor(
        host="ai-gpu1",
        pipeline_exe="/bin/true",
        control_url="http://127.0.0.1:0",
        health_source=health.get,
        wedged_min_age_s=kw.pop("wedged_min_age_s", 0.0),
        wedged_no_success_s=kw.pop("wedged_no_success_s", 1.0),
        wedged_cooldown_s=kw.pop("wedged_cooldown_s", 600.0),
        **kw,
    )


def _adopt(sup: AgentSupervisor, cid: str, slug: str, age_s: float = 10.0) -> Child:
    c = Child(cid, slug, True, _FakeProc(), time.monotonic() - age_s)
    sup.children[cid] = c
    return c


def _desired(slug: str, count: int) -> Desired:
    return Desired(
        control_url="http://127.0.0.1:0",
        workloads=[WorkloadDesired(slug=slug, count=count, gpu=True, vram_mb=2000)],
    )


def test_build_error継続で成功無しならwedged():
    h = {"c1": ChildHealth(build_errors=5)}
    sup = _sup(h)
    c = _adopt(sup, "c1", "image-embed", age_s=10.0)
    assert sup._is_wedged(c) is True


def test_起動直後は誤検知しない():
    h = {"c1": ChildHealth(build_errors=5)}
    sup = _sup(h, wedged_min_age_s=60.0)
    c = _adopt(sup, "c1", "image-embed", age_s=5.0)
    assert sup._is_wedged(c) is False, "モデルロード中の子を wedged にしている"


def test_成功実績がある子はwedgedにしない():
    h = {"c1": ChildHealth(build_errors=2, last_success_at=time.monotonic())}
    sup = _sup(h)
    c = _adopt(sup, "c1", "image-embed", age_s=10.0)
    assert sup._is_wedged(c) is False


def test_単に暇なだけの子はwedgedにしない():
    h = {"c1": ChildHealth(build_errors=0)}   # run を 1 件も出していない
    sup = _sup(h)
    c = _adopt(sup, "c1", "image-embed", age_s=10_000.0)
    assert sup._is_wedged(c) is False, "アイドルを wedged と誤判定している"


def test_health_source無しなら判定オフ_従来挙動():
    sup = AgentSupervisor(host="h", pipeline_exe="/bin/true", control_url="http://x")
    c = _adopt(sup, "c1", "image-embed", age_s=10_000.0)
    assert sup._is_wedged(c) is False


def test_wedgedは台数から外され_killされ_cooldownが張られる(monkeypatch):
    h = {"c1": ChildHealth(build_errors=5)}
    sup = _sup(h)
    c = _adopt(sup, "c1", "image-embed", age_s=10.0)
    spawned: list[str] = []
    monkeypatch.setattr(sup, "_spawn", lambda slug, gpu, cvd=None: spawned.append(slug))
    monkeypatch.setattr(sup, "_vram_room_for", lambda mb: True)   # VRAM は潤沢とする

    sup.reconcile(_desired("image-embed", 1))

    assert c.terminating is True, "wedged を殺していない"
    assert c.proc.terminated == 1
    assert spawned == [], "cooldown 中なのに詰め直している (kill→spawn churn)"
    assert "image-embed" in sup._wedged_cooldown_until


def test_cooldown明けには再spawnする(monkeypatch):
    h = {"c1": ChildHealth(build_errors=5)}
    sup = _sup(h)
    _adopt(sup, "c1", "image-embed", age_s=10.0)
    spawned: list[str] = []
    monkeypatch.setattr(sup, "_spawn", lambda slug, gpu, cvd=None: spawned.append(slug))
    monkeypatch.setattr(sup, "_vram_room_for", lambda mb: True)

    sup.reconcile(_desired("image-embed", 1))
    assert spawned == []

    sup._wedged_cooldown_until["image-embed"] = time.monotonic() - 1  # 猶予明け
    sup.reconcile(_desired("image-embed", 1))
    assert spawned == ["image-embed"], "cooldown 明けても復旧しない"


def test_wedgedがいても健全な子は超過killの対象になる(monkeypatch):
    h = {"c1": ChildHealth(build_errors=5)}      # wedged
    sup = _sup(h)
    bad = _adopt(sup, "c1", "image-embed", age_s=10.0)
    good_old = _adopt(sup, "c2", "image-embed", age_s=100.0)
    good_new = _adopt(sup, "c3", "image-embed", age_s=5.0)
    monkeypatch.setattr(sup, "_spawn", lambda *a, **k: pytest.fail("spawn すべきでない"))

    sup.reconcile(_desired("image-embed", 1))    # 健全 2 体 > 1 → 新しい方を DRAIN

    assert bad.terminating is True
    assert good_new.terminating is True, "健全な超過分が DRAIN されていない"
    assert good_old.terminating is False, "古い方を残すべき"
