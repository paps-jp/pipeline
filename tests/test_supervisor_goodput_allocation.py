"""goodput allocator の配分ロジック回帰テスト (2026-08-09)。

事故の再現: image-hash-extract が backlog 8.8万 / 実 1 台のまま数日固定され、
img_hash_q が capacity_warn の 1.7 倍まで伸びた。 GPU の空き VRAM は
gpu8 13.5GB / gpu9 14.3GB あったのに 1 台も増えなかった。 原因は 4 つ:

  1. `_goodput_agent_retarget` が pin を無視して count を 0 に書き潰す。
     pin フラグは残るので、 pin を尊重する elastic/planner が以後その枠に触れず
     「0 が pin で守られる」ラチェットになる。
  2. 同関数が配分の基準に「生きている子の数」を使う。 spawn + モデルロード中の
     枠は子 0 なので、 増やした枠が次 tick で撤去され永遠に立ち上がらない。
  3. dwell (min_dwell_s) が増減の両方を現状値に固定するため、 +1 を書いた直後に
     dwell へ入り、 明けるまでに枠が消える 3 分半周期のループになる。
  4. その dwell が min_workers も踏み抜くので floor が永久に満たされない。

plugin は非公開リポジトリの入れ子 clone なので、 未配置なら skip する。
"""

from __future__ import annotations

import importlib.util
import pathlib
import time

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "pipeline_supervisor" / "supervisor_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(),
                                reason="pipeline_supervisor plugin が未配置")


@pytest.fixture(scope="module")
def sup():
    spec = importlib.util.spec_from_file_location("supervisor_main_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


HASH = "image-hash-extract"
VFE = "video-face-extract"

WL = {
    "slug": HASH,
    "host_affinity": ["ai-gpu1", "ai-gpu3", "ai-gpu4", "ai-gpu8", "ai-gpu9", "ai-gpu5"],
    "max_concurrent_per_host": 4,
}


def _census():
    """2026-08-08 19:11 の実配置。 gpu8/gpu9 は operator が pin、 子は未 spawn。"""
    def host(total, tmpl):
        return {"last_vram_total_mb": total, "template": {"workloads": tmpl}}

    hosts = {
        "ai-gpu1": host(16311, {
            HASH: {"count": 1, "gpu": True, "vram_mb": 2231},
            "image-embed": {"count": 1, "gpu": True, "vram_mb": 2528},
            VFE: {"count": 3, "gpu": True, "vram_mb": 670},
        }),
        "ai-gpu4": host(10240, {
            HASH: {"count": 0, "gpu": True, "vram_mb": 1202},
            "image-embed": {"count": 1, "gpu": True, "vram_mb": 1080},
            VFE: {"count": 4, "gpu": True, "vram_mb": 1076},
        }),
        "ai-gpu8": host(16311, {
            HASH: {"count": 2, "gpu": True, "vram_mb": 2844, "pin": True},
            "image-embed": {"count": 2, "gpu": True, "vram_mb": 2487, "pin": True},
            VFE: {"count": 3, "gpu": True, "vram_mb": 736, "pin": True},
        }),
        "ai-gpu9": host(16311, {
            HASH: {"count": 0, "gpu": True, "vram_mb": 2844, "pin": True},
            VFE: {"count": 2, "gpu": True, "vram_mb": 703, "pin": True},
        }),
    }
    return {"hosts": hosts, "per_host": {("ai-gpu1", HASH): 1}, "per_slug": {HASH: 1}}


def _retarget(sup, desired, census=None, raised_at=None, grace=180.0):
    return sup._goodput_agent_retarget(
        "http://control", HASH, desired, census or _census(), WL,
        headroom_mb=500, do_apply=False, raised_at=raised_at if raised_at is not None else {},
        spawn_grace_s=grace,
    )


# ---- 1. pin ---------------------------------------------------------------

def test_pinned_hosts_are_never_rewritten(sup):
    for desired in (0, 1, 2, 5, 99):
        acts = _retarget(sup, desired)
        assert [a for a in acts if a["host"] in ("ai-gpu8", "ai-gpu9")] == [], (
            f"desired={desired} で pin ホストが書き換えられた")


def test_pinned_slots_count_only_live_children(sup):
    """pin の template 2 は子 0 なら実容量 0。 唯一動く非 pin の 1 台を剥がさない。"""
    acts = _retarget(sup, 2)
    assert not any(a["host"] == "ai-gpu1" and a["to"] == 0 for a in acts)
    assert sum(a["to"] - a["from"] for a in acts) == 1


# ---- 2. template 基準 ------------------------------------------------------

def test_in_flight_slot_is_not_revoked(sup):
    """template を上げた直後 (子 0) の tick で、 その枠が撤去されないこと。"""
    census = _census()
    census["hosts"]["ai-gpu4"]["template"]["workloads"][HASH]["count"] = 1
    acts = _retarget(sup, 5, census=census)
    assert not any(a["host"] == "ai-gpu4" and a["to"] == 0 for a in acts)


def test_spawn_grace_blocks_shrink(sup):
    census = _census()
    census["hosts"]["ai-gpu4"]["template"]["workloads"][HASH]["count"] = 1
    raised = {f"ai-gpu4:{HASH}": time.monotonic()}
    acts = _retarget(sup, 1, census=census, raised_at=raised)
    assert not any(a["host"] == "ai-gpu4" and a["to"] < 1 for a in acts)

    # grace 明けなら縮小できる
    raised = {f"ai-gpu4:{HASH}": time.monotonic() - 999}
    acts = _retarget(sup, 1, census=census, raised_at=raised)
    assert any(a["host"] == "ai-gpu4" and a["to"] == 0 for a in acts)


def test_shrink_prefers_slots_without_children(sup):
    """空振りしている枠から先に削る (動いている worker を先に剥がさない)。"""
    census = _census()
    census["hosts"]["ai-gpu4"]["template"]["workloads"][HASH]["count"] = 1
    acts = _retarget(sup, 1, census=census, raised_at={})
    shrunk = {a["host"] for a in acts if a["to"] < a["from"]}
    assert shrunk == {"ai-gpu4"}, "子が立っている ai-gpu1 を先に削ってはいけない"


# ---- 3. VRAM ---------------------------------------------------------------

def test_never_overcommits_vram_or_per_host_cap(sup):
    census = _census()
    acts = _retarget(sup, 99, census=census)
    for a in acts:
        tmpl = census["hosts"][a["host"]]["template"]["workloads"]
        total = census["hosts"][a["host"]]["last_vram_total_mb"]
        used = sum((a["to"] if s == HASH else int(sp.get("count") or 0))
                   * int(sp.get("vram_mb") or 1500)
                   for s, sp in tmpl.items() if sp.get("gpu"))
        assert used <= total - 500
        assert a["to"] <= WL["max_concurrent_per_host"]


# ---- 4. plan / stabilization ----------------------------------------------

_CFG = {
    "enabled": True, "apply_mode": False, "kill_switch": False,
    "apply_slugs": [], "parked_slugs": [VFE], "supersedes_others": True,
    "min_demand_backlog": 200, "slope_window_s": 300.0, "saturated_gain_min": 1.0,
    "history_window_s": 1200.0, "scale_down_stabilization_window_s": 300.0,
    "deadband": 1, "min_dwell_s": 180.0, "scale_down_backlog_check_s": 180.0,
    "backlog_shrink_ratio": 0.95, "vram_headroom_mb": 500, "release_dwell_s": 180.0,
    "starve_backlog": 5000, "starve_step_up": 4, "spawn_grace_s": 180.0,
    "default_max_workers": 8, "queue_ceilings": {}, "park_alert_backlog": 10 ** 9,
}


def _workloads(backlog_hash):
    return [
        {"slug": HASH, "enabled": True, "supervisor_enabled": True, "requires_gpu": True,
         "backlog": backlog_hash, "backlog_ok": True, "assigned_workers": 0,
         "min_workers": 2, "max_workers": 20, "host_affinity": [],
         "max_concurrent_per_host": 4, "observed_vram_mb_peak": 2316},
        {"slug": VFE, "enabled": True, "supervisor_enabled": True, "requires_gpu": True,
         "backlog": 3477, "backlog_ok": True, "assigned_workers": 0,
         "min_workers": 15, "max_workers": 32, "host_affinity": [],
         "max_concurrent_per_host": 5, "observed_vram_mb_peak": 2082},
    ]


_RAW = [{"slug": HASH, "queue_backend": "mariadb"}, {"slug": VFE, "queue_backend": "mariadb"}]


def _tick(sup, monkeypatch, state, active_hash, active_vfe, backlog_hash, tp_hash=550.0):
    """1 tick 回して (plan, diagnostics)。 slope 窓より古い観測を仕込んでおく。"""
    t0 = time.monotonic() - 400.0
    state.setdefault("goodput_history", {})[HASH] = [(t0, active_hash, tp_hash, backlog_hash)]
    state["goodput_history"][VFE] = [(t0, active_vfe, 82.0, 3477)]
    monkeypatch.setattr(sup, "_goodput_agent_census", lambda *a, **k: {
        "hosts": {}, "per_host": {}, "per_slug": {HASH: active_hash, VFE: active_vfe}})
    monkeypatch.setattr(sup, "_goodput_fetch_throughput",
                        lambda *a, **k: ({HASH: tp_hash, VFE: 82.0}, False))
    monkeypatch.setattr(sup, "_goodput_adopt_existing_filters", lambda *a, **k: 0)
    r = sup._goodput_allocator_run(state, _workloads(backlog_hash), _RAW, [], [])
    return r["plan"], r["diagnostics"]


@pytest.fixture()
def state():
    return {"control_url": "http://control", "goodput_cfg": dict(_CFG)}


def test_starving_slug_ramps_by_more_than_one(sup, monkeypatch, state):
    plan, diag = _tick(sup, monkeypatch, state, 1, 11, 88187)
    assert diag[HASH]["state"] == "responsive"
    assert plan[HASH] == 5           # 1 + starve_step_up(4)


def test_dwell_does_not_block_scale_up(sup, monkeypatch, state):
    """直前に手を打っていても、 増やす方向は止めない (= 事故の中核)。"""
    _tick(sup, monkeypatch, state, 1, 11, 88187)
    state["goodput_last_action_mono"][HASH] = time.monotonic()
    plan, _ = _tick(sup, monkeypatch, state, 1, 11, 88187)
    assert plan[HASH] >= 5


def test_floor_is_reasserted_after_damping(sup, monkeypatch, state):
    """backlog が枯れても min_workers は割らない。"""
    plan, diag = _tick(sup, monkeypatch, state, 1, 11, 0)
    assert diag[HASH]["state"] == "no_demand"
    assert plan[HASH] == 2


def test_parked_slug_is_not_grown_to_its_floor(sup, monkeypatch, state):
    """park = 外部律速。 min_workers=15 まで引き上げて空き GPU を奪わない。"""
    plan, diag = _tick(sup, monkeypatch, state, 1, 11, 0)
    assert diag[VFE]["state"] == "parked"
    assert plan[VFE] == 11


def test_parked_slug_releases_down_to_floor(sup, monkeypatch, state):
    plan, _ = _tick(sup, monkeypatch, state, 1, 20, 0)
    assert plan[VFE] == 15


def test_parked_slug_never_goes_to_zero(sup, monkeypatch, state):
    """0 台にすると throughput も slope も測れず二度と発進できない (片道切符)。"""
    plan, _ = _tick(sup, monkeypatch, state, 1, 0, 0)
    assert plan[VFE] == 1


def test_small_backlog_still_ramps_by_one(sup, monkeypatch, state):
    plan, diag = _tick(sup, monkeypatch, state, 3, 11, 1200)
    assert diag[HASH]["state"] == "responsive"
    assert plan[HASH] == 4
