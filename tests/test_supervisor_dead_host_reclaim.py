"""死んだ GPU ホストの目標値を配分計算から外す (幻のキャパシティ対策)。

goodput の census は既に max_age_s で stale agent を落としていたが、 elastic は
/api/v1/agents を無条件で全件取り込んでいた。 その結果 _elastic_agent_scale の
目標超過ガード

    tmpl_total = sum(_tmpl_on(a) for a in eligible)
    if tmpl_total >= want: return None

に死んだホストの目標値が算入され、 生きているホストへの scale-up が止まる。

実障害 2026-08-09: ai-gpu3 が 14.9 時間ダウンしたまま hash 11 / vfe 9 / embed 1 の
目標を保持し、 image-embed が backlog 176,908 件に対して実効 3 台のまま増えなかった。
operator が /hosts で enabled=0 にしても、 enabled はどこからも読まれていなかった。

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import datetime as _dt
import importlib.util
from pathlib import Path

import pytest

_SM = Path(__file__).resolve().parents[1] / "plugins" / "pipeline_supervisor" / "supervisor_main.py"
pytestmark = pytest.mark.skipif(not _SM.exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def sm():
    spec = importlib.util.spec_from_file_location("supervisor_main_deadhost_test", _SM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _agent(host: str, *, age_s: float = 0.0, tmpl: dict | None = None,
           vram_free: int = 11000) -> dict:
    seen = _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(seconds=age_s)
    return {
        "host": host,
        "last_seen_at": seen.isoformat(),
        "last_vram_free_mb": vram_free,   # need_mb = 3150*1.3+500 = 4595 を上回る値
        "template": {"workloads": tmpl or {}},
    }


# ---- 生死判定 ----

def test_fresh_agent_is_live(sm):
    assert sm._agent_not_live(_agent("ai-gpu1", age_s=5), set(), 120.0) is None


def test_stale_agent_is_excluded(sm):
    why = sm._agent_not_live(_agent("ai-gpu3", age_s=889 * 60), set(), 120.0)
    assert why is not None and why.startswith("stale_")


def test_policy_disabled_agent_is_excluded(sm):
    """operator が /hosts で無効化したホストは即座に配分から外れる。

    2026-08-09 以前は enabled がどこからも読まれず、 無効化しても何も起きなかった。
    """
    assert sm._agent_not_live(_agent("ai-gpu3", age_s=0), {"ai-gpu3"}, 120.0) == "policy_disabled"


def test_missing_heartbeat_is_excluded(sm):
    a = _agent("ai-gpu3")
    a["last_seen_at"] = None
    assert sm._agent_not_live(a, set(), 120.0) == "no_heartbeat"


def test_bad_heartbeat_is_excluded(sm):
    a = _agent("ai-gpu3")
    a["last_seen_at"] = "not-a-timestamp"
    assert sm._agent_not_live(a, set(), 120.0) == "bad_heartbeat"


def test_boundary_just_inside_max_age(sm):
    assert sm._agent_not_live(_agent("ai-gpu1", age_s=60), set(), 120.0) is None


# ---- 目標超過ガードに幽霊が混ざらないこと (本丸) ----

def _scale(sm, agents, want, current):
    return sm._elastic_agent_scale(
        {"control_url": "http://x", "elastic_agent_scale_last": {}},
        agents, "image-embed", "gpu",
        {"host_affinity": [], "max_concurrent_per_host": 4,
         "observed_vram_mb_peak": 3150},
        want=want, current=current, apply=False,
        cfg={"agent_scale_cooldown_s": 0},
    )


def test_phantom_template_no_longer_blocks_scale_up(sm):
    """死んだホストを除外したリストなら、 生きているホストに +1 できる。"""
    live_only = [_agent("ai-gpu1", tmpl={"image-embed": {"count": 1, "gpu": True}}),
                 _agent("ai-gpu8", tmpl={"image-embed": {"count": 0, "gpu": True}})]
    out = _scale(sm, live_only, want=5, current=1)
    assert out is not None
    assert out.get("reason") == "scale_up"
    assert out.get("host") == "ai-gpu8"      # 空いている方へ +1


def test_phantom_template_would_have_blocked(sm):
    """回帰の証拠: 死んだホストを混ぜると従来どおり増やせない。

    tmpl_total = 1(gpu1) + 0(gpu8) + 4(幽霊 gpu3) = 5 >= want(5) で目標超過ガードに
    掛かる。 幽霊の count は per-host cap(4) 以下にしてある — cap 超過だと cap 是正が
    先に走り、 死んだホストへの書き込みという別の無駄が表面化するため。
    このテストが落ちたら目標超過ガード自体の仕様が変わったということ。
    """
    with_ghost = [_agent("ai-gpu1", tmpl={"image-embed": {"count": 1, "gpu": True}}),
                  _agent("ai-gpu8", tmpl={"image-embed": {"count": 0, "gpu": True}}),
                  _agent("ai-gpu3", age_s=889 * 60,
                         tmpl={"image-embed": {"count": 4, "gpu": True}})]
    out = _scale(sm, with_ghost, want=5, current=1)
    assert out is None


# ---- host-policy 取得の堅牢性 ----

def test_disabled_hosts_parses_enabled_flag(sm, monkeypatch):
    monkeypatch.setattr(sm, "_http_get_json", lambda *a, **k: {
        "hosts": [{"host": "ai-gpu1", "enabled": 1},
                  {"host": "ai-gpu3", "enabled": 0},
                  {"host": "ai-gpu9"}]})          # enabled 欠落は有効扱い
    assert sm._elastic_disabled_hosts("http://x") == {"ai-gpu3"}


def test_disabled_hosts_fails_open(sm, monkeypatch):
    """control plane が一瞬重いだけで全ホストが配分から消えると致命的なので、
    取得失敗時は「無効なホストは無い」に倒す。"""
    def _boom(*a, **k):
        raise RuntimeError("timeout")
    monkeypatch.setattr(sm, "_http_get_json", _boom)
    assert sm._elastic_disabled_hosts("http://x") == set()
