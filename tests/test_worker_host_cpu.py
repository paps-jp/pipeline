"""worker が heartbeat で送るホストの CPU 利用率 (host_cpu_pct) の回帰テスト。

守りたいこと:
  - cgroup v2 の cpu.stat (usage_usec) の増え方 ÷ 経過秒 ÷ 使ってよいコア数 で出す。
    LXC の中の load1 / lxcfs の /proc/stat は親ノードの値が混ざり、CT の ai-gpu4/5・nas が
    実際は 1〜3 割でも 100% に張り付いて、supervisor の elastic がそこへ増やさなかった (2026-11-22)
  - 直近 60 秒の平均にする (1 回ごとの差分は跳ねる)。1 回目は比べる相手が無いので None
  - カウンタが戻ったら測り直す。0〜100 に収める
  - cpu.max の quota があればコア数をそこまで下げる
  - cgroup v2 が無い環境は従来の load1 / コア数
"""

from __future__ import annotations

import pytest

from pipeline.worker import service


def test_first_sample_has_nothing_to_compare():
    assert service._cgroup_cpu_pct(100.0, 5_000_000, 10, []) is None


def test_usage_over_elapsed_and_cores():
    samples: list = []
    service._cgroup_cpu_pct(100.0, 0, 10, samples)
    # 10 秒で 25 CPU 秒 = 10 コア中 2.5 コア分 = 25%
    assert service._cgroup_cpu_pct(110.0, 25_000_000, 10, samples) == 25.0


def test_average_over_window_not_last_step():
    samples: list = []
    service._cgroup_cpu_pct(0.0, 0, 1, samples)
    service._cgroup_cpu_pct(30.0, 30_000_000, 1, samples)      # 30 秒間 100%
    # 次の 30 秒は 0% → 60 秒の平均は 50%
    assert service._cgroup_cpu_pct(60.0, 30_000_000, 1, samples) == 50.0
    # 窓を出た古い点は捨て、直近だけを見る (60〜120 秒は 0%)
    service._cgroup_cpu_pct(90.0, 30_000_000, 1, samples)
    assert service._cgroup_cpu_pct(125.0, 30_000_000, 1, samples) == 0.0


def test_counter_going_back_restarts_and_result_is_clipped():
    samples: list = [(0.0, 9_000_000)]
    assert service._cgroup_cpu_pct(5.0, 1_000, 1, samples) is None   # 戻った → 測り直し
    assert samples == [(5.0, 1_000)]
    assert service._cgroup_cpu_pct(6.0, 5_000_000, 1, samples) == 100.0  # 1 コアで 5 秒分 → 上限


def test_cpu_max_quota_lowers_cores(tmp_path, monkeypatch):
    monkeypatch.setattr(service.os, "sched_getaffinity", lambda _pid: set(range(48)), raising=False)
    p = tmp_path / "cpu.max"
    p.write_text("max 100000\n")
    assert service._cpu_cores(str(p)) == 48.0
    p.write_text("800000 100000\n")
    assert service._cpu_cores(str(p)) == 8.0
    assert service._cpu_cores(str(tmp_path / "missing")) == 48.0


def test_reads_usage_usec(tmp_path):
    p = tmp_path / "cpu.stat"
    p.write_text("usage_usec 1538332940106\nuser_usec 864474879893\nsystem_usec 673858060213\n")
    assert service._cgroup_cpu_usage_usec(str(p)) == 1538332940106
    assert service._cgroup_cpu_usage_usec(str(tmp_path / "missing")) is None


def test_falls_back_to_load_without_cgroup_v2(monkeypatch, tmp_path):
    monkeypatch.setattr(service, "_cgroup_cpu_usage_usec", lambda *a, **k: None)
    monkeypatch.setattr(service.os, "sched_getaffinity", lambda _pid: set(range(4)), raising=False)
    la = tmp_path / "loadavg"
    la.write_text("2.00 1.00 0.50 1/100 1\n")
    real_open = open

    def fake_open(path, *a, **k):
        return real_open(la, *a, **k) if path == "/proc/loadavg" else real_open(path, *a, **k)

    monkeypatch.setattr("builtins.open", fake_open)
    assert service._host_cpu_pct() == pytest.approx(50.0)
