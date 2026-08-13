"""RAM ディスク予算ゲート (.48 video-ram) の回帰テスト (2026-08-14)。

事故の背景: `video-face-extract` は 1 claim あたり平均 875MB の動画を .48 の
tmpfs (90G) に載せるのに、 workload の資源宣言は `vram_mb` だけで、 この RAM
コストが supervisor の配分モデルに一切入っていなかった。
`max_concurrent_total=40` まで伸ばすと live だけで ~35GB、 そこに MinIO の
削除待ち (.minio.sys/tmp/.trash、 実測ピーク 47.9G) が重なる。 CT501 の
`memory.peak` は実際に 89.65 GiB = tmpfs ほぼ満杯 を記録していた。

このテストが守る性質:
  - probe は `capacity total - free` を使う (= df 相当、 trash 込み)。
    `usage_total_bytes` (live のみ) で予算判定すると trash 分を見落とし、
    実測で使用量の 83〜99% を占めていた側を丸ごと無視することになる。
  - probe 失敗は fail-open。 metrics 障害で動画パイプラインを止めない。
  - crit でも min_workers は下回らない。 0 台にすると throughput も slope も
    観測できず二度と発進できない (`_goodput_gear_engaged` と同じ理由)。 また
    min_workers 未満へ落とすと `_elastic_floor_guard` と綱引きになる。
  - 対象外 slug は絶対に触らない。

plugin は非公開リポジトリの入れ子 clone なので、 未配置なら skip する。
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "pipeline_supervisor" / "supervisor_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(),
                                reason="pipeline_supervisor plugin が未配置")

_GB = 1024 ** 3
_TOTAL = 90 * _GB          # tmpfs 90G


@pytest.fixture(scope="module")
def sup():
    spec = importlib.util.spec_from_file_location("supervisor_ramdisk_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _metrics(total: int, free: int, live: int) -> str:
    # MinIO は有効数字を丸めずに出す (実物: 9.663676416e+10)。 既定の {:e} は
    # 6 桁で丸まり 1MB ずれるので、 実物と同じ精度で書く。
    def _e(v: int) -> str:
        return f"{v:.10e}"

    return (
        "# HELP minio_cluster_capacity_usable_total_bytes Total usable capacity\n"
        "# TYPE minio_cluster_capacity_usable_total_bytes gauge\n"
        f'minio_cluster_capacity_usable_total_bytes{{server="127.0.0.1:9000"}} {_e(total)}\n'
        f'minio_cluster_capacity_usable_free_bytes{{server="127.0.0.1:9000"}} {_e(free)}\n'
        f'minio_cluster_usage_total_bytes{{server="127.0.0.1:9000"}} {_e(live)}\n'
        'minio_cluster_drive_total{server="127.0.0.1:9000"} 1\n'
    )


def _state(sup, *, used: int, live: int, enabled: bool = True,
           warn: float = 70.0, crit: float = 85.0, monkeypatch=None) -> dict:
    """probe を metrics テキスト固定にした state を作る。"""
    state = {
        "ramdisk_cfg": {
            "enabled": enabled,
            "metrics_url": "http://ramdisk.invalid/metrics",
            "slugs": ["video-face-extract"],
            "warn_pct": warn,
            "crit_pct": crit,
            "probe_interval_s": 0.0,     # キャッシュを効かせない
            "probe_timeout_s": 1.0,
            "default_claim_mb": 900,
        },
        "ramdisk_last": None,
    }
    payload = _metrics(_TOTAL, _TOTAL - used, live)
    monkeypatch.setattr(sup.urllib.request, "urlopen",
                        lambda *a, **k: _FakeResp(payload))
    return state


class _FakeResp:
    def __init__(self, text: str):
        self._b = text.encode("utf-8")

    def read(self) -> bytes:
        return self._b


def _wl(min_workers: int = 15, claim_mb: int | None = 900) -> dict:
    res: dict = {"vram_mb": 2082}
    if claim_mb is not None:
        res["ram_disk_mb"] = claim_mb
    return {"slug": "video-face-extract", "min_workers": min_workers, "resources": res}


# ── probe ────────────────────────────────────────────────────────────────────

def test_probe_uses_capacity_not_live_usage(sup, monkeypatch):
    """used は capacity(total-free) 由来。 live (usage_total) と混同しない。

    実測でピーク時の使用量の 83〜99% は .trash 側だった。 live を予算に使うと
    その分を丸ごと見落とす。
    """
    state = _state(sup, used=45 * _GB, live=5 * _GB, monkeypatch=monkeypatch)
    snap = sup._ramdisk_probe(state)
    assert snap is not None
    assert snap["total_mb"] == 90 * 1024
    assert snap["used_mb"] == 45 * 1024
    assert snap["live_mb"] == 5 * 1024      # live は別枠で保持するだけ
    assert snap["pct"] == pytest.approx(50.0, abs=0.1)


def test_probe_failure_returns_none(sup, monkeypatch):
    state = _state(sup, used=0, live=0, monkeypatch=monkeypatch)

    def _boom(*a, **k):
        raise OSError("connection refused")

    monkeypatch.setattr(sup.urllib.request, "urlopen", _boom)
    state["ramdisk_last"] = None
    assert sup._ramdisk_probe(state) is None
    assert state.get("ramdisk_last_error")


def test_probe_without_capacity_metrics_is_none(sup, monkeypatch):
    """403 等で本文が来ても capacity が無ければ None (= fail-open へ倒す)。"""
    state = _state(sup, used=0, live=0, monkeypatch=monkeypatch)
    monkeypatch.setattr(sup.urllib.request, "urlopen",
                        lambda *a, **k: _FakeResp("minio_cluster_drive_total 1\n"))
    state["ramdisk_last"] = None
    assert sup._ramdisk_probe(state) is None


# ── clamp ────────────────────────────────────────────────────────────────────

def test_clamp_budget_limits_growth(sup, monkeypatch):
    """通常域では crit までの残容量 / claim が増加の上限になる。

    used=45G, crit=85% → crit ライン 76.5G、 残 31.5G。 claim 900MB なので
    +35 台まで。 active=10 なので 45 が上限 → 目標 20 はそのまま通る。
    """
    state = _state(sup, used=45 * _GB, live=5 * _GB, monkeypatch=monkeypatch)
    plan = {"video-face-extract": 20}
    info = sup._ramdisk_clamp(state, plan, {"video-face-extract": 10},
                              {"video-face-extract": _wl()})
    assert plan["video-face-extract"] == 20
    assert info["clamped"] == []


def test_clamp_budget_caps_when_headroom_small(sup, monkeypatch):
    """残容量が小さければ active + 入る分 までしか許さない。

    used=60G, crit=85% → crit ライン 76.5G、 残 16.5G = 16896MB。
    claim 900MB なので +18 台。 active=2 → 上限 20。 目標 32 は 20 に落ちる。
    """
    state = _state(sup, used=60 * _GB, live=8 * _GB, monkeypatch=monkeypatch)
    plan = {"video-face-extract": 32}
    info = sup._ramdisk_clamp(state, plan, {"video-face-extract": 2},
                              {"video-face-extract": _wl()})
    assert plan["video-face-extract"] == 20
    assert info["clamped"][0]["from"] == 32
    assert info["clamped"][0]["to"] == 20


def test_clamp_warn_freezes_growth_but_allows_shrink(sup, monkeypatch):
    """warn 域では増やさない。 減らす提案はそのまま通す。"""
    state = _state(sup, used=int(0.75 * _TOTAL), live=8 * _GB, monkeypatch=monkeypatch)
    plan = {"video-face-extract": 30}
    sup._ramdisk_clamp(state, plan, {"video-face-extract": 18},
                       {"video-face-extract": _wl()})
    assert plan["video-face-extract"] == 18          # 増加は凍結

    state["ramdisk_last"] = None
    plan = {"video-face-extract": 12}
    sup._ramdisk_clamp(state, plan, {"video-face-extract": 18},
                       {"video-face-extract": _wl()})
    assert plan["video-face-extract"] == 12          # 縮小は素通し


def test_clamp_crit_falls_back_to_min_workers_not_zero(sup, monkeypatch):
    """crit では min_workers まで。 0 にはしない (片道切符 / floor guard 衝突の回避)。"""
    state = _state(sup, used=int(0.92 * _TOTAL), live=20 * _GB, monkeypatch=monkeypatch)
    plan = {"video-face-extract": 32}
    info = sup._ramdisk_clamp(state, plan, {"video-face-extract": 30},
                              {"video-face-extract": _wl(min_workers=15)})
    assert plan["video-face-extract"] == 15
    assert "CRIT" in info["clamped"][0]["reason"]


def test_clamp_crit_never_raises_above_target(sup, monkeypatch):
    """crit で「min_workers まで戻す」が **増やす** 方向に働いてはいけない。"""
    state = _state(sup, used=int(0.92 * _TOTAL), live=20 * _GB, monkeypatch=monkeypatch)
    plan = {"video-face-extract": 4}
    sup._ramdisk_clamp(state, plan, {"video-face-extract": 4},
                       {"video-face-extract": _wl(min_workers=15)})
    assert plan["video-face-extract"] == 4


def test_clamp_fail_open_when_probe_down(sup, monkeypatch):
    """probe が死んでいても絞らない。 metrics 障害でパイプラインを止めないため。"""
    state = _state(sup, used=0, live=0, monkeypatch=monkeypatch)

    def _boom(*a, **k):
        raise OSError("connection refused")

    monkeypatch.setattr(sup.urllib.request, "urlopen", _boom)
    state["ramdisk_last"] = None
    plan = {"video-face-extract": 32}
    info = sup._ramdisk_clamp(state, plan, {"video-face-extract": 2},
                              {"video-face-extract": _wl()})
    assert plan["video-face-extract"] == 32
    assert "fail-open" in info["skipped"]


def test_clamp_ignores_non_gated_slugs(sup, monkeypatch):
    """対象外 slug は crit でも触らない。"""
    state = _state(sup, used=int(0.95 * _TOTAL), live=20 * _GB, monkeypatch=monkeypatch)
    plan = {"image-hash-extract": 12, "video-face-extract": 32}
    sup._ramdisk_clamp(state, plan,
                       {"image-hash-extract": 12, "video-face-extract": 30},
                       {"image-hash-extract": {"slug": "image-hash-extract",
                                               "min_workers": 2, "resources": {}},
                        "video-face-extract": _wl()})
    assert plan["image-hash-extract"] == 12
    assert plan["video-face-extract"] == 15


def test_clamp_disabled_is_noop(sup, monkeypatch):
    state = _state(sup, used=int(0.99 * _TOTAL), live=20 * _GB,
                   enabled=False, monkeypatch=monkeypatch)
    plan = {"video-face-extract": 32}
    info = sup._ramdisk_clamp(state, plan, {"video-face-extract": 2},
                              {"video-face-extract": _wl()})
    assert plan["video-face-extract"] == 32
    assert info["skipped"] == "disabled"


def test_clamp_uses_declared_claim_mb_over_default(sup, monkeypatch):
    """resources.ram_disk_mb を宣言していればそちらを使う。

    残 16.5G = 16896MB。 claim を 4224MB と宣言すれば +4 台まで (既定 900 なら +18)。
    """
    state = _state(sup, used=60 * _GB, live=8 * _GB, monkeypatch=monkeypatch)
    plan = {"video-face-extract": 32}
    sup._ramdisk_clamp(state, plan, {"video-face-extract": 2},
                       {"video-face-extract": _wl(claim_mb=4224)})
    assert plan["video-face-extract"] == 6           # active 2 + 4


def test_clamp_falls_back_to_default_claim_when_undeclared(sup, monkeypatch):
    state = _state(sup, used=60 * _GB, live=8 * _GB, monkeypatch=monkeypatch)
    plan = {"video-face-extract": 32}
    sup._ramdisk_clamp(state, plan, {"video-face-extract": 2},
                       {"video-face-extract": _wl(claim_mb=None)})
    assert plan["video-face-extract"] == 20          # active 2 + 16896//900
