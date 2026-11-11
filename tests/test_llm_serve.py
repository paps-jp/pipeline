"""llm-serve plugin の判断ロジックと tick の約束の回帰テスト。

守りたいこと:
  - 忙しい状態が続いたら譲る / 空きが続き VRAM が足りたら戻る (ヒステリシス)
  - 判定不能 (control plane に届かない) では何もしない
  - 空き VRAM が不明 / 不足なら起動しない
  - tick は例外を出さず短く返し、 adapt.interval_s を載せる (watchdog 誤検知防止)

plugin は非公開リポジトリの入れ子 clone なので、 未配置なら skip する。
"""

from __future__ import annotations

import importlib.util
import pathlib

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "llm_serve" / "llm_serve_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(), reason="llm_serve plugin が未配置")


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("llm_serve_main_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def cfg(m):
    return m.parse_config({"model": "Qwen/Qwen3-VL-2B-Instruct", "gpu_mem_gb": 6,
                           "yield_after_min": 10, "resume_after_min": 15,
                           "vram_margin_mb": 1024, "python_bin": "/opt/llm/venv/bin/python"})


def _st(m, phase, **kw):
    st = {"phase": phase, "busy_since": None, "idle_since": None, "backoff_until": 0.0}
    st.update(kw)
    return st


ENOUGH = 6 * 1024 + 1024


# --- 譲る ------------------------------------------------------------------ #
def test_running_yields_after_busy_lasts(m, cfg):
    st = _st(m, m.RUNNING, busy_since=0.0)
    assert m.next_action(st, cfg, True, None, 599) is None
    assert m.next_action(st, cfg, True, None, 600) == "stop"


def test_running_does_not_yield_when_disabled(m, cfg):
    cfg = dict(cfg, yield_enabled=False)
    st = _st(m, m.RUNNING, busy_since=0.0)
    assert m.next_action(st, cfg, True, None, 10_000) is None


def test_unknown_busy_never_stops(m, cfg):
    st = _st(m, m.RUNNING, busy_since=0.0)
    assert m.next_action(st, cfg, None, None, 10_000) is None


# --- 戻る ------------------------------------------------------------------ #
def test_parked_resumes_after_idle_and_vram(m, cfg):
    st = _st(m, m.PARKED, idle_since=0.0)
    assert m.next_action(st, cfg, False, ENOUGH, 899) is None
    assert m.next_action(st, cfg, False, ENOUGH, 900) == "start"


def test_parked_waits_for_vram(m, cfg):
    st = _st(m, m.PARKED, idle_since=0.0)
    assert m.next_action(st, cfg, False, ENOUGH - 1, 10_000) is None
    assert m.next_action(st, cfg, False, None, 10_000) is None


def test_parked_does_not_start_while_busy_or_unknown(m, cfg):
    st = _st(m, m.PARKED, idle_since=0.0)
    assert m.next_action(st, cfg, True, ENOUGH, 10_000) is None
    assert m.next_action(st, cfg, None, ENOUGH, 10_000) is None


def test_parked_respects_backoff(m, cfg):
    st = _st(m, m.PARKED, idle_since=0.0, backoff_until=5_000.0)
    assert m.next_action(st, cfg, False, ENOUGH, 4_999) is None
    assert m.next_action(st, cfg, False, ENOUGH, 5_000) == "start"


def test_always_resident_starts_without_idle_wait(m, cfg):
    cfg = dict(cfg, yield_enabled=False)
    st = _st(m, m.PARKED)
    assert m.next_action(st, cfg, True, ENOUGH, 0) == "start"


def test_transitions_in_flight_take_no_action(m, cfg):
    for phase in (m.STARTING, m.STOPPING):
        assert m.next_action(_st(m, phase, busy_since=0.0, idle_since=0.0),
                             cfg, True, ENOUGH, 10_000) is None


def test_update_since_hysteresis(m):
    st = {"busy_since": None, "idle_since": None}
    m.update_since(st, True, 10)
    m.update_since(st, True, 20)
    assert st == {"busy_since": 10, "idle_since": None}   # 続いている間は起点を動かさない
    m.update_since(st, None, 30)
    assert st == {"busy_since": 10, "idle_since": None}   # 判定不能は記録を変えない
    m.update_since(st, False, 40)
    assert st == {"busy_since": None, "idle_since": 40}


# --- 設定と起動コマンド ---------------------------------------------------- #
def test_is_busy_threshold(m):
    wls = [{"slug": "image-embed", "observed_depth": 999},
           {"slug": "other", "observed_depth": 10**6}]
    assert m.is_busy(wls, ["image-embed"], 1000) is False   # 見張っていない slug は無視
    wls[0]["observed_depth"] = 1000
    assert m.is_busy(wls, ["image-embed"], 1000) is True


def test_vllm_command(m, cfg):
    cmd = m.build_command(dict(cfg, extra_args="--quantization fp8"), 16311)
    assert cmd[:3] == ["/opt/llm/venv/bin/python", "-m", "vllm.entrypoints.openai.api_server"]
    assert cmd[cmd.index("--model") + 1] == "Qwen/Qwen3-VL-2B-Instruct"
    assert cmd[cmd.index("--served-model-name") + 1] == "qwen3-vl-2b-instruct"
    assert cmd[cmd.index("--gpu-memory-utilization") + 1] == "0.377"   # 6144/16311
    assert cmd[-2:] == ["--quantization", "fp8"]


def test_command_override_for_other_engines(m, cfg):
    cfg = dict(cfg, command="/opt/llama/llama-server -m {model} --port {port} -c {max_model_len}")
    assert m.build_command(cfg, 16311) == [
        "/opt/llama/llama-server", "-m", "Qwen/Qwen3-VL-2B-Instruct",
        "--port", "8000", "-c", "8192"]


def test_model_is_required(m):
    with pytest.raises(RuntimeError):
        m.parse_config({"model": ""})


def test_gpu_mem_utilization_is_clamped(m):
    assert m.gpu_mem_utilization(100_000, 16311) == 0.95
    assert m.gpu_mem_utilization(6144, 0) == 0.5


# --- tick の約束 ----------------------------------------------------------- #
def test_tick_returns_quickly_and_reports_interval(m, monkeypatch):
    """busy 判定が取れない状況でも tick は成功で返り、 遷移も起こさない。"""
    monkeypatch.setattr(m, "_advertise_host", lambda url: "10.0.0.1")
    monkeypatch.setattr(m, "_reap_orphan", lambda cfg: None)
    st = m.setup(model="x/y", log_dir=str(pathlib.Path(__file__).parent), tick_s=5)
    monkeypatch.setattr(m, "_probe_busy", lambda st: None)
    monkeypatch.setattr(m, "_gpu_mem", lambda idx: (16000, 16311))
    monkeypatch.setattr(m, "_register", lambda *a: pytest.fail("PARKED で登録してはいけない"))
    monkeypatch.setattr(m, "_deregister", lambda st: None)
    monkeypatch.setattr(m, "_wait_tick", lambda st: None)
    monkeypatch.setattr(m, "_self_enqueue_next_tick", lambda cfg, tick: None)
    spawned = []
    monkeypatch.setattr(m, "_spawn_transition", lambda st, phase, fn: spawned.append(phase))

    out = m.process(None, None, st)
    assert out["phase"] == m.PARKED and out["action"] is None and spawned == []
    assert out["adapt"] == {"interval_s": 5}


def test_first_tick_starts_when_idle(m, monkeypatch):
    """デプロイ直後に resume_after_min 待たされない。"""
    monkeypatch.setattr(m, "_advertise_host", lambda url: "10.0.0.1")
    monkeypatch.setattr(m, "_reap_orphan", lambda cfg: None)
    st = m.setup(model="x/y", log_dir=str(pathlib.Path(__file__).parent))
    monkeypatch.setattr(m, "_probe_busy", lambda st: False)
    monkeypatch.setattr(m, "_gpu_mem", lambda idx: (16000, 16311))
    monkeypatch.setattr(m, "_deregister", lambda st: None)
    monkeypatch.setattr(m, "_wait_tick", lambda st: None)
    monkeypatch.setattr(m, "_self_enqueue_next_tick", lambda cfg, tick: None)
    spawned = []
    monkeypatch.setattr(m, "_spawn_transition", lambda st, phase, fn: spawned.append(phase))

    out = m.process(None, None, st)
    assert out["action"] == "start" and spawned == [m.STARTING]


def test_dead_process_is_detected_and_backed_off(m, monkeypatch):
    monkeypatch.setattr(m, "_advertise_host", lambda url: "10.0.0.1")
    monkeypatch.setattr(m, "_reap_orphan", lambda cfg: None)
    st = m.setup(model="x/y", log_dir=str(pathlib.Path(__file__).parent))

    class Dead:
        pid, returncode = 1, 1

        def poll(self):
            return 1

    st["phase"], st["proc"] = m.RUNNING, Dead()
    monkeypatch.setattr(m, "_probe_busy", lambda st: False)
    monkeypatch.setattr(m, "_gpu_mem", lambda idx: (16000, 16311))
    monkeypatch.setattr(m, "_deregister", lambda st: None)
    monkeypatch.setattr(m, "_wait_tick", lambda st: None)
    monkeypatch.setattr(m, "_self_enqueue_next_tick", lambda cfg, tick: None)
    monkeypatch.setattr(m, "_spawn_transition", lambda st, phase, fn: None)

    out = m.process(None, None, st)
    assert out["phase"] == m.PARKED and out["action"] is None   # backoff 中は即再起動しない
    assert st["backoff_until"] > 0 and "rc=1" in out["error"]


def test_bypass_mps_option(m):
    assert m.parse_config({"model": "x"})["bypass_mps"] is False         # 既定は worker と同じ MPS に相乗り
    assert m.parse_config({"model": "x", "bypass_mps": 1})["bypass_mps"] is True


def test_enforce_eager_default_and_command(m, cfg):
    assert "--enforce-eager" in m.build_command(dict(cfg, enforce_eager=True), 16311)
    assert "--enforce-eager" not in m.build_command(dict(cfg, enforce_eager=False), 16311)
    assert m.parse_config({"model": "x"})["enforce_eager"] is True
