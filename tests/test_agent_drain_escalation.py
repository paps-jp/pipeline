"""agent の graceful kill → SIGKILL 昇格 (drain_grace_s) のテスト。

SIGTERM を無視する子が居座ると VRAM を握ったまま _active_by_workload() の台数から
消え、 VRAM ゲートが全 GPU spawn を永久拒否する状態に固着する (2026-08-01 の
ai-gpu4/5)。 昇格が効くことをここで担保する。
"""

from __future__ import annotations

import signal
import subprocess
import sys
import time

import pytest

from pipeline.agent.supervisor import AgentSupervisor, Child

pytestmark = pytest.mark.skipif(
    sys.platform == "win32", reason="SIGTERM/SIGKILL・プロセスグループは POSIX 前提"
)

# SIGTERM を握り潰して眠り続ける子 = 座礁 worker の再現。
# ハンドラ設置前に SIGTERM が届くと素で死んでしまい昇格を検証できないので、
# 設置完了を stdout で親に知らせてから眠る (= レース除去)。
_IGNORES_SIGTERM = (
    "import signal, sys, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); "
    "sys.stdout.write('ready\\n'); sys.stdout.flush(); time.sleep(60)"
)
_EXITS_ON_SIGTERM = (
    "import sys, time; sys.stdout.write('ready\\n'); sys.stdout.flush(); time.sleep(60)"
)


def _sup(**kw) -> AgentSupervisor:
    return AgentSupervisor(
        host="test-host",
        pipeline_exe=sys.executable,
        control_url="http://127.0.0.1:0",
        **kw,
    )


def _adopt(sup: AgentSupervisor, child_id: str, code: str) -> Child:
    """_spawn を経由せず、 任意の挙動の子を children に差し込む。

    子が 'ready' を書くまでブロックする。 これを待たずに SIGTERM を送ると
    ハンドラ設置前に届いて素で死に、 SIGKILL 昇格を検証したつもりで
    「SIGTERM で死んだだけ」を見る偽陽性になる。
    """
    proc = subprocess.Popen(
        [sys.executable, "-c", code], start_new_session=True,
        stdout=subprocess.PIPE, text=True,
    )
    assert proc.stdout is not None
    assert proc.stdout.readline().strip() == "ready", "子の準備完了を受け取れない"
    c = Child(child_id, "dummy-workload", False, proc, time.monotonic())
    sup.children[child_id] = c
    return c


def _wait_gone(sup: AgentSupervisor, child_id: str, timeout: float = 5.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        sup._reap()
        if child_id not in sup.children:
            return True
        time.sleep(0.05)
    return False


def test_sigterm_を無視する子は猶予超過で_sigkill_される():
    sup = _sup(drain_grace_s=0.3)
    c = _adopt(sup, "c1", _IGNORES_SIGTERM)
    try:
        c.terminating = True
        sup._signal("c1", graceful=True)
        assert c.terminating_at is not None, "SIGTERM 時刻が記録されていない"

        # 猶予内: まだ昇格しない (= 現タスク完走の猶予を奪わない)
        sup._reap()
        assert c.proc.poll() is None
        assert "c1" in sup.children

        # 猶予超過: SIGKILL 昇格 → 回収される
        time.sleep(0.4)
        assert _wait_gone(sup, "c1"), "猶予超過後も子が回収されていない"
        assert c.proc.returncode == -signal.SIGKILL, (
            f"SIGKILL 以外で終了している (rc={c.proc.returncode}) "
            "= 昇格ではなく SIGTERM で死んだだけの偽陽性"
        )
    finally:
        if c.proc.poll() is None:
            c.proc.kill()
            c.proc.wait(timeout=5)


def test_graceful_で素直に死ぬ子は_sigkill_されない():
    sup = _sup(drain_grace_s=30.0)
    c = _adopt(sup, "c2", _EXITS_ON_SIGTERM)
    try:
        c.terminating = True
        sup._signal("c2", graceful=True)
        assert _wait_gone(sup, "c2"), "SIGTERM で終了するはずの子が残っている"
        assert c.proc.returncode == -signal.SIGTERM, (
            f"SIGTERM 以外で終了している (rc={c.proc.returncode})"
        )
    finally:
        if c.proc.poll() is None:
            c.proc.kill()
            c.proc.wait(timeout=5)


def test_terminating_でない子は猶予を過ぎても殺されない():
    sup = _sup(drain_grace_s=0.1)
    c = _adopt(sup, "c3", _IGNORES_SIGTERM)
    try:
        time.sleep(0.3)
        sup._reap()
        sup._reap()
        assert c.proc.poll() is None, "稼働中の子が誤って kill された"
        assert "c3" in sup.children
    finally:
        c.proc.kill()
        c.proc.wait(timeout=5)


def test_再送抑止_sigkill_が効かなくても毎tick連打しない(monkeypatch):
    """D state 等で SIGKILL も効かない子への連打を防ぐ (terminating_at を進める)。"""
    sup = _sup(drain_grace_s=0.2)
    c = _adopt(sup, "c4", _IGNORES_SIGTERM)
    calls: list[bool] = []

    orig = sup._signal

    def _spy(child_id: str, graceful: bool) -> None:
        calls.append(graceful)
        if not graceful:
            return  # SIGKILL が効かない子を模す
        orig(child_id, graceful)

    monkeypatch.setattr(sup, "_signal", _spy)
    try:
        c.terminating = True
        sup._signal("c4", graceful=True)
        time.sleep(0.3)
        sup._reap()          # 1 回目の昇格
        sup._reap()          # 直後の tick では再送しない
        sup._reap()
        assert calls.count(False) == 1, f"SIGKILL を連打している: {calls}"
    finally:
        monkeypatch.undo()
        c.proc.kill()
        c.proc.wait(timeout=5)
