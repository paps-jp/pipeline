"""RAM ディスク投入ゲート (.47 image-ram → paprika-job-submit) の回帰テスト。

事故の背景 (2026-08-30): `.47` (CT500) の memcg が `memory.high` に貼り付き、
MinIO が accept を回せなくなった (accept キュー 4099/4096 で TCP connect が全滅)。
image-pull の GC が LIST timeout で 1 tick 800-960 秒に膨れ、 singleton なので
画像取込が全停止した。 その 3.6 時間、 **paprika-job-submit は 262/min で投入し
続けていた** —— 誰も止めていなかった。 これが死のスパイラルの燃料になり、
`.47` は消費ゼロのまま 134 GiB まで積み上がった。

このテストが守る性質:
  - crit 到達で投入を止める。 止め方は worker 台数ではなく workload の
    `init_kwargs.submit_paused` (singleton なので台数 0 は使えない)。
  - **ヒステリシス**: crit で停止 → resume (< warn) まで下がって初めて解除。
    単一閾値だと閾値上で config PUT が振動する。
  - **min_dwell_s** で連続書き換えを抑える。
  - **fail-open は片側だけ**。 probe 失敗で新規停止はしない (metrics 障害で投入を
    止める方が損害が大きい) が、 **既に停止中なら停止を維持する**。 probe 失敗で
    勝手に再開すると上記の死のスパイラルへ戻る。
  - PUT が失敗したら state を進めない (成功したことにしない)。

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
_TOTAL = 224 * _GB          # tmpfs 224G (2026-08-30 に 240G から縮小)
_URL = "http://ramdisk-47.invalid/metrics"


@pytest.fixture(scope="module")
def sup():
    spec = importlib.util.spec_from_file_location("supervisor_gate_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _FakeResp:
    def __init__(self, text: str):
        self._b = text.encode("utf-8")

    def read(self) -> bytes:
        return self._b


def _metrics(total: int, free: int) -> str:
    def _e(v: int) -> str:
        return f"{v:.10e}"

    return (
        f'minio_cluster_capacity_usable_total_bytes{{server="s"}} {_e(total)}\n'
        f'minio_cluster_capacity_usable_free_bytes{{server="s"}} {_e(free)}\n'
        f'minio_cluster_usage_total_bytes{{server="s"}} {_e(free // 2)}\n'
    )


def _state(sup, *, enabled: bool = True, dwell: float = 0.0) -> dict:
    return {
        "control_url": "http://control.invalid",
        "submit_gate_cfg": {
            "enabled": enabled,
            "probe_timeout_s": 1.0,
            "probe_interval_s": 0.0,     # キャッシュを効かせない
            "min_dwell_s": dwell,
            "targets": [{
                "name": "image-ram-47",
                "metrics_url": _URL,
                "slugs": ["paprika-job-submit"],
                "warn_pct": 70.0,
                "crit_pct": 85.0,
                "resume_pct": 55.0,
            }],
        },
        "submit_gate_state": {},
        "submit_gate_probe": {},
    }


def _set_pct(sup, monkeypatch, pct: float | None) -> None:
    """probe の応答水位を差し替える。 None で probe 失敗にする。"""
    if pct is None:
        def _boom(*a, **k):
            raise OSError("connection refused")
        monkeypatch.setattr(sup.urllib.request, "urlopen", _boom)
        return
    used = int(_TOTAL * pct / 100.0)
    payload = _metrics(_TOTAL, _TOTAL - used)
    monkeypatch.setattr(sup.urllib.request, "urlopen",
                        lambda *a, **k: _FakeResp(payload))


@pytest.fixture
def patches(sup, monkeypatch):
    """_patch_init_kwargs の呼び出しを記録する。 既定は成功。"""
    calls: list[tuple[str, dict]] = []

    def _fake(control_url, slug, updates):
        calls.append((slug, dict(updates)))
        return {"ok": True, "updates": updates}

    monkeypatch.setattr(sup, "_patch_init_kwargs", _fake)
    return calls


# ── 基本動作 ─────────────────────────────────────────────────────────────────

def test_below_warn_does_nothing(sup, monkeypatch, patches):
    """warn 未満では何も書かないし止めない。"""
    st = _state(sup)
    _set_pct(sup, monkeypatch, 40.0)
    out = sup._submit_gate_run(st)
    assert patches == []
    assert out["actions"] == []
    assert out["alerts"] == []
    assert out["targets"][0]["paused"] is False


def test_warn_alerts_but_does_not_pause(sup, monkeypatch, patches):
    """warn 帯は警告だけ。 投入は止めない (止めるのは crit)。"""
    st = _state(sup)
    _set_pct(sup, monkeypatch, 75.0)
    out = sup._submit_gate_run(st)
    assert patches == []
    assert [a["kind"] for a in out["alerts"]] == ["ramdisk_warn"]
    assert st["submit_gate_state"]["image-ram-47"]["paused"] is False


def test_crit_pauses_and_alerts(sup, monkeypatch, patches):
    """crit で submit_paused=1 を書き、 アラートを出す。"""
    st = _state(sup)
    _set_pct(sup, monkeypatch, 90.0)
    out = sup._submit_gate_run(st)
    assert patches == [("paprika-job-submit", {"submit_paused": 1})]
    assert st["submit_gate_state"]["image-ram-47"]["paused"] is True
    kinds = [a["kind"] for a in out["alerts"]]
    assert kinds == ["ramdisk_submit_paused"]
    assert out["actions"][0]["to"] == "paused"


# ── ヒステリシス ─────────────────────────────────────────────────────────────

def test_between_resume_and_crit_stays_paused(sup, monkeypatch, patches):
    """crit を割っても resume まで下がるまでは解除しない。

    ここを単一閾値にすると、 crit 直下で pause/resume が振動して config PUT を
    連打する。
    """
    st = _state(sup)
    _set_pct(sup, monkeypatch, 90.0)
    sup._submit_gate_run(st)
    patches.clear()

    for pct in (84.0, 70.0, 56.0):       # crit 未満・resume 超
        _set_pct(sup, monkeypatch, pct)
        out = sup._submit_gate_run(st)
        assert patches == [], f"pct={pct} で解除してはいけない"
        assert st["submit_gate_state"]["image-ram-47"]["paused"] is True
        # 停止中は毎 tick 鳴らす (放置されないように)
        assert [a["kind"] for a in out["alerts"]] == ["ramdisk_submit_paused"]


def test_resume_threshold_releases(sup, monkeypatch, patches):
    """resume まで下がったら submit_paused=0 を書いて再開する。"""
    st = _state(sup)
    _set_pct(sup, monkeypatch, 90.0)
    sup._submit_gate_run(st)
    patches.clear()

    _set_pct(sup, monkeypatch, 50.0)
    out = sup._submit_gate_run(st)
    assert patches == [("paprika-job-submit", {"submit_paused": 0})]
    assert st["submit_gate_state"]["image-ram-47"]["paused"] is False
    assert [a["kind"] for a in out["alerts"]] == ["ramdisk_submit_resumed"]


def test_min_dwell_blocks_rapid_flip(sup, monkeypatch, patches):
    """dwell 内の再変更は抑止する (config PUT の連打防止)。"""
    st = _state(sup, dwell=3600.0)
    _set_pct(sup, monkeypatch, 90.0)
    sup._submit_gate_run(st)                     # 初回 pause (mono=0 なので通る)
    assert patches == [("paprika-job-submit", {"submit_paused": 1})]
    patches.clear()

    _set_pct(sup, monkeypatch, 10.0)             # 一気に空になっても
    out = sup._submit_gate_run(st)
    assert patches == [], "dwell 中に解除してはいけない"
    assert st["submit_gate_state"]["image-ram-47"]["paused"] is True
    assert out["targets"][0]["dwell_block_s"] > 0


# ── fail-open は片側だけ ─────────────────────────────────────────────────────

def test_probe_failure_does_not_pause(sup, monkeypatch, patches):
    """未停止で probe が落ちても止めない (fail-open)。

    metrics 障害で投入を止める方が損害が大きい。
    """
    st = _state(sup)
    _set_pct(sup, monkeypatch, None)
    out = sup._submit_gate_run(st)
    assert patches == []
    assert st["submit_gate_state"].get("image-ram-47", {}).get("paused") is not True
    assert out["targets"][0]["fail_mode"] == "open"
    assert out["alerts"] == []


def test_probe_failure_while_paused_holds_pause(sup, monkeypatch, patches):
    """停止中に probe が落ちたら停止を維持する (fail-safe)。

    ここで勝手に再開すると 2026-08-30 の死のスパイラルへ戻る:
    .47 窒息 → image-pull 停止 → しかし投入は継続 → .47 さらに逼迫。
    """
    st = _state(sup)
    _set_pct(sup, monkeypatch, 90.0)
    sup._submit_gate_run(st)
    patches.clear()

    _set_pct(sup, monkeypatch, None)
    out = sup._submit_gate_run(st)
    assert patches == [], "probe 失敗で再開してはいけない"
    assert st["submit_gate_state"]["image-ram-47"]["paused"] is True
    assert out["targets"][0]["fail_mode"] == "hold"
    assert [a["kind"] for a in out["alerts"]] == ["ramdisk_submit_probe_failed"]


# ── 失敗時の state ───────────────────────────────────────────────────────────

def test_failed_put_does_not_advance_state(sup, monkeypatch):
    """PUT が失敗したら「止めた」ことにしない (次 tick で再試行させる)。"""
    calls: list[str] = []

    def _fail(control_url, slug, updates):
        calls.append(slug)
        return {"ok": False, "error": "PUT failed: 500"}

    monkeypatch.setattr(sup, "_patch_init_kwargs", _fail)
    st = _state(sup)
    _set_pct(sup, monkeypatch, 90.0)
    out = sup._submit_gate_run(st)
    assert calls == ["paprika-job-submit"]
    assert st["submit_gate_state"]["image-ram-47"]["paused"] is False
    assert out["actions"][0]["ok"] is False

    out2 = sup._submit_gate_run(st)              # 次 tick で再試行する
    assert calls == ["paprika-job-submit"] * 2
    assert out2["actions"][0]["ok"] is False


# ── 無効化 / 設定 ────────────────────────────────────────────────────────────

def test_disabled_is_noop(sup, monkeypatch, patches):
    st = _state(sup, enabled=False)
    _set_pct(sup, monkeypatch, 99.0)
    out = sup._submit_gate_run(st)
    assert patches == []
    assert out["skipped"] == "disabled"


def test_default_targets_point_at_47_and_job_submit(sup):
    """既定ターゲットが .47 / paprika-job-submit であること。"""
    targets = sup._submit_gate_targets({})
    assert len(targets) == 1
    t = targets[0]
    assert "10.10.50.47" in t["metrics_url"]
    assert t["slugs"] == ["paprika-job-submit"]
    # resume < warn < crit でないとヒステリシスが成立しない
    assert t["resume_pct"] < t["warn_pct"] < t["crit_pct"]


def test_broken_targets_json_falls_back_to_default(sup):
    """設定 JSON が壊れていても既定へ落ちる (ゲートが消えない)。"""
    targets = sup._submit_gate_targets({"submit_gate_targets": "{not json"})
    assert targets[0]["slugs"] == ["paprika-job-submit"]


def test_targets_json_string_is_parsed(sup):
    targets = sup._submit_gate_targets(
        {"submit_gate_targets": '[{"name":"x","metrics_url":"http://x/m",'
                                '"slugs":["s"],"crit_pct":50,"resume_pct":10}]'})
    assert targets[0]["name"] == "x"
    assert targets[0]["slugs"] == ["s"]
