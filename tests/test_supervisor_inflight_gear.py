"""in-process 並列度 autoscaler (max_inflight) の変速機。

エージェント配分側の変速機 (_goodput_gear_engaged / test_supervisor_gear_shift.py)
と同じ考え方を、 singleton workload の並列度制御にも入れたもの。

実障害 2026-08-22: LAN router の eth1 が 10G→100M に落ちて輸送が壊れ、 輸送失敗を
見た MD が image-pull の max_inflight を 96→16 まで削った。 リンク復旧後も復帰は
+8/180 秒しか無く 96 まで 30 分かかる一方、 watermark は実時間の 0.43 倍でしか進まず
遅れが 1 時間あたり 34 分ずつ広がった。 hub の asset 保持は約 2 時間なので、 遅れが
そこを越えると origin フォールバック (10 秒) に落ちて自己増幅に入る。

固定する性質は 2 つ:
  1. watermark が大きく遅れている間は「発進」= 乗算で上げる (ローギア)。 遅れが
     縮めば自動で「巡航」= +step_up (ハイギア) に戻る。
  2. 減速は **直近窓** の失敗率だけで決める。 全窓 (sample_runs=12) だと輸送復旧後も
     障害中の失敗を含み、 復旧した直後に減速するという実際に起きた誤動作になる。

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import datetime as _dt
import importlib.util
from pathlib import Path

import pytest

_SM = Path(__file__).resolve().parents[1] / "plugins" / "pipeline_supervisor" / "supervisor_main.py"
pytestmark = pytest.mark.skipif(not _SM.exists(), reason="plugins リポジトリ未チェックアウト")

SLUG = "paprika-image-pull"


@pytest.fixture(scope="module")
def sm():
    spec = importlib.util.spec_from_file_location("supervisor_main_inflight_test", _SM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


# ---------------------------------------------------------------- helpers --

def _wm(lag_s: float, *, sep: str = " ") -> str:
    """lag_s 秒だけ遅れた watermark 文字列。 image-pull は空白区切りで出す。"""
    t = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=lag_s)
    return t.isoformat(sep=sep)


def _sample(*, downloaded=200, fails=None, lag_s=0.0, is_backlog=True,
            max_inflight=16, deadline_hit=False):
    s = {
        "downloaded": downloaded,
        "dl_failed": sum((fails or {}).values()),
        "is_backlog": is_backlog,
        "max_inflight": max_inflight,
        "watermark": _wm(lag_s),
    }
    if fails:
        s["dl_fail_reasons"] = fails
    if deadline_hit:
        s["deadline_hit"] = True
    return s


def _cfg(**over):
    cfg = {
        "enabled": True, "apply_mode": True, "slugs": [SLUG],
        "consumer_slug": "image-hash-extract", "consumer_ceiling": 200000,
        "min_inflight": 8, "max_inflight": 96, "step_up": 8,
        "backoff_factor": 0.7, "fail_ratio_high": 0.20,
        "backlog_tick_ratio": 0.5, "sample_runs": 12, "min_dwell_s": 180.0,
        "ram_url": "http://hub/health", "ram_high_pct": 70.0,
        "ram_step_up": 24, "ram_dwell_s": 60.0,
        "ram_probe_interval_s": 30.0, "ram_probe_timeout_s": 5.0,
        "catchup_lag_s": 1800.0, "catchup_factor": 2.0, "catchup_dwell_s": 60.0,
        "recent_runs": 4,
        "budget_backoff": "non_backlog",
    }
    cfg.update(over)
    return cfg


def _run(sm, monkeypatch, samples, *, cfg=None, ram_pct=5.0, consumer_pending=0):
    """_inflight_autoscale_run を 1 回まわして (decision, applied_updates) を返す。"""
    applied: list[dict] = []

    def fake_get(url, timeout=None, **kw):
        if "/health" in url:
            return {"asset_spill": {"tiers": {"nonvideo": {"pct": ram_pct,
                                                           "stale": False}}}}
        if "/queue" in url:
            return {"by_state": {"pending": consumer_pending}, "total": consumer_pending}
        if "/runs" in url:
            # 本番の list_for_workload は started_at DESC (= 先頭が最新)。
            return {"runs": [{"output_json": s} for s in samples]}
        raise AssertionError("unexpected GET %s" % url)

    def fake_patch(control_url, slug, updates):
        applied.append(updates)
        return {"ok": True, "updates": updates}

    monkeypatch.setattr(sm, "_http_get_json", fake_get)
    monkeypatch.setattr(sm, "_patch_init_kwargs", fake_patch)

    state = {
        "control_url": "http://ctl",
        "inflight_cfg": cfg or _cfg(),
        "inflight_last_action": {},
        "inflight_ram_last": None,
    }
    out = sm._inflight_autoscale_run(state)
    return out["decisions"][0], applied


# --------------------------------------------------- watermark lag helper --

def test_lag_accepts_both_separators(sm):
    """image-pull は空白区切り、 links-pull は "T" 区切りで watermark を出す。"""
    for sep in (" ", "T"):
        lag = sm._inflight_watermark_lag_s([{"watermark": _wm(600.0, sep=sep)}])
        assert lag == pytest.approx(600.0, abs=5.0), sep


def test_lag_supplies_utc_for_naive_and_takes_newest(sm):
    naive = (_dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=300)
             ).replace(tzinfo=None).isoformat(sep=" ")
    samples = [{"watermark": _wm(3600.0)}, {"watermark": naive}]
    # 並び順に依らず「最も新しい watermark」= 遅れが最小のものを採る。
    assert sm._inflight_watermark_lag_s(samples) == pytest.approx(300.0, abs=10.0)


def test_lag_none_when_absent_or_garbage(sm):
    assert sm._inflight_watermark_lag_s([{"downloaded": 1}]) is None
    assert sm._inflight_watermark_lag_s([{"watermark": "not-a-date"}]) is None


# ------------------------------------------------ ローギア (発進) / ハイギア --

def test_low_gear_multiplies_when_far_behind(sm, monkeypatch):
    """95 分遅れ = 発進。 +8 ではなく乗算で開ける (実障害時の状況)。"""
    samples = [_sample(lag_s=5700.0, max_inflight=16) for _ in range(12)]
    d, applied = _run(sm, monkeypatch, samples)
    assert d["gear"] == "low_catchup"
    assert d["reason"] == "scale_up_catchup"
    assert applied == [{"max_inflight": 32}]          # 16 * 2.0, +8 ではない


def test_high_gear_adds_step_up_when_caught_up(sm, monkeypatch):
    """遅れが閾値未満なら巡航。 従来どおり +step_up。"""
    samples = [_sample(lag_s=120.0, max_inflight=16) for _ in range(12)]
    d, applied = _run(sm, monkeypatch, samples)
    assert d["gear"] == "high_cruise"
    assert d["reason"] == "scale_up_backlog"
    assert applied == [{"max_inflight": 24}]          # 16 + 8


def test_upshift_is_automatic_not_a_latch(sm, monkeypatch):
    """遅れが縮んだら自動でハイギアへ戻る (片方向のラッチにしない)。"""
    behind = [_sample(lag_s=5700.0, max_inflight=64) for _ in range(12)]
    d1, _ = _run(sm, monkeypatch, behind)
    caught = [_sample(lag_s=60.0, max_inflight=64) for _ in range(12)]
    d2, _ = _run(sm, monkeypatch, caught)
    assert (d1["gear"], d2["gear"]) == ("low_catchup", "high_cruise")


def test_low_gear_never_exceeds_max(sm, monkeypatch):
    samples = [_sample(lag_s=5700.0, max_inflight=64) for _ in range(12)]
    _, applied = _run(sm, monkeypatch, samples)
    assert applied == [{"max_inflight": 96}]          # 64*2=128 を hi で頭打ち


def test_low_gear_needs_demand(sm, monkeypatch):
    """遅れていても backlog でなければ発進しない (取る物が無い)。"""
    samples = [_sample(lag_s=5700.0, is_backlog=False, max_inflight=16)
               for _ in range(12)]
    d, applied = _run(sm, monkeypatch, samples)
    assert d["gear"] == "high_cruise"
    assert d["state"] == "no_demand"
    assert applied == []


# ------------------------------------------- 減速は直近窓だけで決める (回帰) --

_TRANSPORT = {"hub[HTTP 404]>ext:read_timeout": 120}
_CLEAN: dict = {}


def test_backoff_ignores_stale_failures_after_transport_recovery(sm, monkeypatch):
    """2026-08-22 15:18 の実際の誤動作: リンク復旧後に 22→16 と減速した。

    全窓 12 tick のうち古い 8 tick が 100Mbps 時代 (輸送失敗だらけ)、 直近 4 tick は
    復旧済みで失敗ゼロ。 全窓の fail_ratio は閾値超えだが、 減速してはいけない。
    """
    recent = [_sample(lag_s=5700.0, fails=_CLEAN, max_inflight=22) for _ in range(4)]
    stale = [_sample(lag_s=5700.0, fails=_TRANSPORT, max_inflight=22) for _ in range(8)]
    d, applied = _run(sm, monkeypatch, recent + stale)

    assert d["fail_ratio"] >= 0.20            # 全窓で見れば「失敗が多い」
    assert d["fail_ratio_recent"] == 0.0      # 直近窓は健全
    assert d["reason"] == "scale_up_catchup"  # 減速ではなく発進する
    assert applied == [{"max_inflight": 44}]


def test_backoff_still_fires_on_real_saturation(sm, monkeypatch):
    """直近窓が本当に失敗していれば、 発進局面でも減速する (減速条件は殺さない)。"""
    recent = [_sample(lag_s=5700.0, fails=_TRANSPORT, max_inflight=32) for _ in range(4)]
    old = [_sample(lag_s=5700.0, fails=_CLEAN, max_inflight=32) for _ in range(8)]
    d, applied = _run(sm, monkeypatch, recent + old)
    assert d["reason"] == "backoff_fail"
    assert applied == [{"max_inflight": 22}]  # int(32*0.7)


def test_permanent_failures_do_not_trigger_backoff(sm, monkeypatch):
    """HTTP 403/404 は中身の問題で輸送の飽和ではない (既存の性質を固定)。"""
    perm = {"hub[read_timeout]>ext:HTTP 403": 300}
    samples = [_sample(lag_s=5700.0, fails=perm, max_inflight=16) for _ in range(12)]
    d, _ = _run(sm, monkeypatch, samples)
    assert d["fail_ratio_recent"] == 0.0
    assert d["reason"] == "scale_up_catchup"


def test_consumer_ceiling_still_wins_over_low_gear(sm, monkeypatch):
    """下流が詰まっていれば発進しない (列を下流へ移すだけ)。"""
    samples = [_sample(lag_s=5700.0, max_inflight=32) for _ in range(12)]
    d, applied = _run(sm, monkeypatch, samples,
                      cfg=_cfg(consumer_ceiling=1000), consumer_pending=5000)
    assert d["reason"] == "backoff_consumer_backlog"
    assert applied == [{"max_inflight": 22}]
