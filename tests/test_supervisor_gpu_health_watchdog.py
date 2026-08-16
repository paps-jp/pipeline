"""GPU health watchdog の検知/ドレイン回帰テスト (2026-08-16)。

事故の再現: ai-gpu8 (VM152@foyer) が Xid 119 = GSP RPC timeout で
`GPU requires reset` に落ち、**19 時間無音**で放置された。 このとき

  - worker_metrics の行は流れ続けていた (6153 行) が temp_c は 100% NULL
  - mem_used_mb はハング直前の 7789 のまま凍結 → VRAM ゲートは素通り
  - onnxruntime が CPUExecutionProvider にフォールバックして**完走し続けた**
    ため errors_total にも失敗率にも一切出なかった

ので、 温度センサーの生存率だけが唯一の正直な指標になる。 併せて 2026-07-10 の
ai-gpu1 bus 脱落 (Xid 154) 型 = nvidia-smi 自体が死んで**行が 1 件も来ない**
ケースも拾えることを固定する。

plugin は非公開リポジトリの入れ子 clone なので、 未配置なら skip する。
"""

from __future__ import annotations

import datetime as dt
import importlib.util
import pathlib

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "pipeline_supervisor" / "supervisor_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(),
                                reason="pipeline_supervisor plugin が未配置")

CONTROL = "http://ctl"
GPU_HOSTS = ("ai-gpu1", "ai-gpu4", "ai-gpu8", "ai-gpu9")

# agent の effective (= agent_desired.desired_json)。 ドレインはこの slug を保ったまま
# count だけ 0 にする。
EFFECTIVE = {
    "image-embed": {"count": 2, "gpu": True, "vram_mb": 2487},
    "video-face-extract": {"count": 3, "gpu": True, "vram_mb": 736},
}


@pytest.fixture(scope="module")
def sup():
    spec = importlib.util.spec_from_file_location("supervisor_gpu_health_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _samples(n: int, *, temp: bool) -> list[dict]:
    """n 件のメトリクス。 temp=False は Xid 119 型 (温度/電力だけ NULL、 VRAM は凍結値)。"""
    return [{"temp_c": 43.0 if temp else None,
             "util_pct": 0.0 if temp else None,
             "power_w": 12.4 if temp else None,
             "mem_used_mb": 7789, "mem_total_mb": 16311} for _ in range(n)]


def _metrics(per_host: dict[str, tuple[int, bool]]) -> dict:
    """{host: (件数, 温度あり)} → /api/v1/workers/metrics のレスポンス形。"""
    workers: dict[str, dict] = {}
    for host, (n, temp) in per_host.items():
        wid = "w_" + host.replace("-", "_") + "_a4"
        workers[wid] = {"0": _samples(n, temp=temp)}
    return {"workers": workers, "since_minutes": 5}


def _hosts_in(metrics: dict) -> tuple[str, ...]:
    """metrics に出てくる host。 既定の GPU 母集合はこれ (= metrics_absent を誘発しない)。"""
    return tuple(sorted({"-".join(wid[2:].split("_")[:2])
                         for wid in (metrics.get("workers") or {})}))


def _wire(sup, monkeypatch, metrics: dict, *, policies: dict[str, dict] | None = None,
          gpu_hosts=None, agent_hosts=None, agent_age_s: float = 5.0):
    """_http_get_json を URL で振り分け、 requests.put を記録する。

    gpu_hosts / agent_hosts を省略すると metrics に居る host だけが母集合になる。
    metrics_absent 側を試すテストだけが明示的に広い母集合を渡す。
    agent_age_s は agent の heartbeat 鮮度 (判定 B の前提条件)。
    """
    policies = policies or {}
    agent_seen_at = (dt.datetime.now(dt.timezone.utc)
                     - dt.timedelta(seconds=agent_age_s)).isoformat()
    if gpu_hosts is None:
        gpu_hosts = _hosts_in(metrics)
    if agent_hosts is None:
        agent_hosts = tuple(gpu_hosts)
    puts: list[tuple[str, dict]] = []

    def fake_get(url, timeout=10.0):
        if "/workers/metrics" in url:
            return metrics
        if "/api/v1/agents" in url:
            return {"agents": [{"host": h,
                                "last_vram_total_mb": 16311 if h in gpu_hosts else 0,
                                "last_seen_at": agent_seen_at,
                                "desired": {"workloads": dict(EFFECTIVE)}}
                               for h in agent_hosts]}
        if "/host-policy" in url:
            return {"hosts": [{"host": h,
                               "vram_effective_mb": 16311 if h in gpu_hosts else None,
                               **(policies.get(h) or {})}
                              for h in sorted(set(gpu_hosts) | set(policies))]}
        raise AssertionError(f"unexpected GET {url}")

    class _Resp:
        status_code = 200

    def fake_put(url, json=None, timeout=None):
        puts.append((url, json or {}))
        return _Resp()

    monkeypatch.setattr(sup, "_http_get_json", fake_get)
    monkeypatch.setattr(sup.requests, "put", fake_put)
    return puts


def _state(**cfg):
    base = {"enabled": True, "apply_mode": True, "window_min": 5, "min_samples": 20,
            "fail_ticks": 2, "recover_ticks": 3}
    base.update(cfg)
    return {"control_url": CONTROL, "gpu_health_watchdog_cfg": base,
            "gpu_health_streaks": {}}


def _run(sup, state, n=1):
    out = None
    for _ in range(n):
        out = sup._gpu_health_watchdog(state)
    return out


# --------------------------------------------------------------------------- #
# A. sensor_dead (Xid 119 = 今回の実障害)
# --------------------------------------------------------------------------- #

def test_ai_gpu8_incident_replay_drains_only_the_dead_host(sup, monkeypatch):
    """2026-08-15 の実測値そのまま: gpu8 だけ temp_ok=0、 他 3 台は temp_ok=n。"""
    puts = _wire(sup, monkeypatch, _metrics({
        "ai-gpu1": (60, True), "ai-gpu4": (60, True),
        "ai-gpu8": (60, False), "ai-gpu9": (60, True),
    }))
    state = _state()
    out = _run(sup, state, n=2)          # fail_ticks=2

    assert out["unhealthy"] == {"ai-gpu8": {"state": "sensor_dead", "n": 60, "temp_ok": 0}}
    assert [a["host"] for a in out["actions"] if a["action"] == "drain"] == ["ai-gpu8"]
    # 健全な 3 台には一切触らない
    assert all("ai-gpu8" in url for url, _ in puts), puts


def test_drain_zeroes_effective_and_disables_policy(sup, monkeypatch):
    """enabled=0 だけでは agent が effective に向かって reconcile し続けるので、
    effective の 0 化と 2 段でないとドレインにならない。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, False)}))
    _run(sup, _state(), n=2)

    eff = [b for u, b in puts if u.endswith("/agents/ai-gpu8/effective")]
    assert eff == [{"workloads": {
        "image-embed": {"count": 0, "gpu": True, "vram_mb": 2487},
        "video-face-extract": {"count": 0, "gpu": True, "vram_mb": 736},
    }}]
    disable = [(u, b) for u, b in puts if "host-policy" in u]
    assert disable == [(f"{CONTROL}/api/v1/host-policy/ai-gpu8",
                        {"enabled": False, "updated_by": sup._GPU_HEALTH_MARK})]


def test_drain_never_sends_an_empty_workloads_dict(sup, monkeypatch):
    """**空 dict を PUT すると agent は「desired 未設定」と解釈し、 ローカルの
    bootstrap desired.json に fallback して死んだ GPU の上で子を spawn し直す**
    (fetch_desired_via_sync の `if not d.get("workloads"): return None`)。
    slug を残して count=0 にすることでのみ reconcile の DRAIN 枝に入る。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, False)}))
    _run(sup, _state(), n=2)

    for url, body in puts:
        if url.endswith("/effective"):
            assert body["workloads"], "空 workloads は bootstrap fallback を誘発する"
            assert all(spec["count"] == 0 for spec in body["workloads"].values())
            # slug 自体は消さない (消すと desired_slugs から外れて同じ罠に落ちる)
            assert set(body["workloads"]) == set(EFFECTIVE)


def test_drain_falls_back_to_template_when_effective_is_unset(sup, monkeypatch):
    """effective 未算定の host でも template の slug を 0 化してドレインする。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, False)}))
    orig = sup._http_get_json

    def no_effective(url, timeout=10.0):
        r = orig(url, timeout=timeout)
        if "/api/v1/agents" in url:
            for a in r["agents"]:
                a["template"] = {"workloads": a.pop("desired")["workloads"]}
        return r

    monkeypatch.setattr(sup, "_http_get_json", no_effective)
    _run(sup, _state(), n=2)

    eff = [b for u, b in puts if u.endswith("/effective")]
    assert eff and set(eff[0]["workloads"]) == set(EFFECTIVE)
    assert all(s["count"] == 0 for s in eff[0]["workloads"].values())


def test_non_agent_host_only_gets_policy_disable(sup, monkeypatch):
    """systemd 管理 (非 agent) の host には effective が無いので PUT しない。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, False)}), agent_hosts=())
    _run(sup, _state(), n=2)

    assert not [u for u, _ in puts if "/effective" in u]
    assert [u for u, _ in puts] == [f"{CONTROL}/api/v1/host-policy/ai-gpu8"]


def test_fail_ticks_must_accumulate_before_draining(sup, monkeypatch):
    """1 tick では撃たない (= 一過性の取りこぼしで健全ホストを外さない)。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, False)}))
    state = _state(fail_ticks=3)

    out = _run(sup, state, n=2)
    assert puts == []
    assert [a["action"] for a in out["actions"]] == ["arming"]

    out = _run(sup, state, n=1)
    assert [a["action"] for a in out["actions"]] == ["drain"]
    assert puts


def test_recovered_host_resets_the_bad_streak(sup, monkeypatch):
    """故障→回復→故障 で streak が繰り越されない (= 累積で誤爆しない)。"""
    state = _state(fail_ticks=3)
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, False)}))
    _run(sup, state, n=2)
    assert puts == []

    _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, True)}))
    _run(sup, state, n=1)
    assert state["gpu_health_streaks"]["ai-gpu8"]["bad"] == 0

    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, False)}))
    _run(sup, state, n=2)
    assert puts == []          # 3 tick 目でないのでまだ撃たない


def test_dry_mode_detects_but_never_writes(sup, monkeypatch):
    """初期投入は DRY。 検知は出すが PUT は一切しない。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, False)}))
    out = _run(sup, _state(apply_mode=False), n=5)

    assert out["unhealthy"]
    drains = [a for a in out["actions"] if a["action"] == "drain"]
    assert drains and all(a["dry_run"] for a in drains)
    assert puts == []


# --------------------------------------------------------------------------- #
# B. metrics_absent (Xid 154 / driver 死 = 行が 1 件も来ない)
# --------------------------------------------------------------------------- #

def test_gpu_host_with_no_metrics_at_all_is_drained(sup, monkeypatch):
    """nvidia-smi ごと落ちると _nvidia_smi_gpus() が [] を返して行が入らない。
    temp_ok==0 の条件だけだと n==0 で素通りするので、 別枝で拾う。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu1": (60, True)}), gpu_hosts=GPU_HOSTS)
    out = _run(sup, _state(), n=2)

    assert out["verdicts"]["ai-gpu8"]["state"] == "metrics_absent"
    assert set(out["unhealthy"]) == {"ai-gpu4", "ai-gpu8", "ai-gpu9"}
    drained = {u.rsplit("/", 1)[-1] for u, _ in puts if "host-policy" in u}
    assert drained == {"ai-gpu4", "ai-gpu8", "ai-gpu9"}


def test_down_host_is_not_treated_as_a_dead_gpu(sup, monkeypatch):
    """agent の heartbeat が切れている = ホストごと落ちている。 GPU 故障ではないので
    ドレインしない (これが無いと停止中/未起動のホストを片端から外してしまう)。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu1": (60, True)}),
                 gpu_hosts=GPU_HOSTS, agent_age_s=3600)
    out = _run(sup, _state(), n=5)

    assert out["verdicts"]["ai-gpu8"]["state"] == "unknown"
    assert out["unhealthy"] == {}
    assert puts == []


def test_cpu_only_host_is_never_flagged(sup, monkeypatch):
    """nas-c2 のような GPU 無しホストは母集合に入れない (metrics_absent の誤爆防止)。"""
    _wire(sup, monkeypatch, _metrics({"ai-gpu1": (60, True)}),
          gpu_hosts=("ai-gpu1",), agent_hosts=("ai-gpu1", "nas-c2"))
    out = _run(sup, _state(), n=3)

    assert "nas-c2" not in out["verdicts"]
    assert out["unhealthy"] == {}


def test_sample_poor_host_is_unknown_not_dead(sup, monkeypatch):
    """起動直後で数件しか無い host を「温度ゼロ件」で殺さない。"""
    puts = _wire(sup, monkeypatch, _metrics({
        "ai-gpu1": (60, True), "ai-gpu4": (60, True),
        "ai-gpu8": (3, False), "ai-gpu9": (60, True),
    }))
    out = _run(sup, _state(), n=5)

    assert out["verdicts"]["ai-gpu8"]["state"] == "unknown"
    assert out["unhealthy"] == {}
    assert puts == []


# --------------------------------------------------------------------------- #
# C. 復帰
# --------------------------------------------------------------------------- #

def test_recovery_reenables_only_after_recover_ticks(sup, monkeypatch):
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, True)}),
                 policies={"ai-gpu8": {"enabled": 0,
                                       "updated_by": "supervisor:gpu-health"}})
    state = _state(recover_ticks=3)

    _run(sup, state, n=2)
    assert puts == []

    out = _run(sup, state, n=1)
    assert [a["action"] for a in out["actions"]] == ["restore"]
    assert puts == [(f"{CONTROL}/api/v1/host-policy/ai-gpu8",
                     {"enabled": True, "updated_by": sup._GPU_HEALTH_MARK})]


def test_operator_disabled_host_is_never_auto_reenabled(sup, monkeypatch):
    """operator が手で無効化した host を watchdog が勝手に戻さない。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, True)}),
                 policies={"ai-gpu8": {"enabled": 0, "updated_by": "operator"}})
    out = _run(sup, _state(recover_ticks=3), n=6)

    assert puts == []
    assert [a for a in out["actions"] if a["action"] == "restore"] == []


def test_already_drained_host_is_not_redrained_every_tick(sup, monkeypatch):
    """enabled=0 のまま故障が続いても PUT を撃ち続けない (= ログと API の洪水防止)。"""
    puts = _wire(sup, monkeypatch, _metrics({"ai-gpu8": (60, False)}),
                 policies={"ai-gpu8": {"enabled": 0,
                                       "updated_by": "supervisor:gpu-health"}})
    _run(sup, _state(), n=10)
    assert puts == []


# --------------------------------------------------------------------------- #
# D. 縮退
# --------------------------------------------------------------------------- #

def test_metrics_fetch_failure_takes_no_action(sup, monkeypatch):
    """control plane が一瞬重いだけで全ホストをドレインしない (fail-safe)。"""
    def boom(url, timeout=10.0):
        raise RuntimeError("timeout")

    monkeypatch.setattr(sup, "_http_get_json", boom)
    out = sup._gpu_health_watchdog(_state())
    assert "error" in out and "actions" not in out


def test_disabled_by_config_is_a_noop(sup, monkeypatch):
    def boom(url, timeout=10.0):
        raise AssertionError("disabled watchdog must not fetch")

    monkeypatch.setattr(sup, "_http_get_json", boom)
    assert sup._gpu_health_watchdog(_state(enabled=False))["skipped"]
