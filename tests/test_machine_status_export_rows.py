"""machine-status-export plugin が API の応答から machine_status の行を作る部分の回帰テスト。

守りたいこと (protection_ai の画面が前提にしている決まり):
  - GPU の行に温度・VRAM (使用/総量)・メモリ帯域 (mem_util_pct) が入る
  - 消費電力は書かない (GPU だけの値で実態より小さく見えるため)
  - 45 秒より古い GPU の値は書かない
  - CPU の行は gpu_index=-1 で、GPU 系の列は空
  - Proxmox ノードの行は gpu_index=-2 で、cpu_pct = PVE の cpu × 100、mem_used_mb / mem_total_mb = RAM。
    online でないノードは書かない
  - 行の並びが UPSERT の placeholder の数と一致する (列を足したときのずれ防止)
  - ワーカーの機械の cpu_pct は、Proxmox のゲストとして見つかれば PVE の実際の使用率を使う
    (LXC の中の load は親ノード全体の値が漏れ、ai-gpu4/5・nas が 100% に張り付いていた。2026-11-22)。
    nas-c2 はゲスト名 nas で引く。見つからなければ load の値のまま。PVE の 1 回の値は跳ねるので直近 60 秒の平均
  - 「<機械>-<役割>」の host (nas-c2-pipeline-supervisor 等) は、その機械の行にまとめる

plugin は非公開リポジトリの入れ子 clone なので、未配置なら skip する。
"""

from __future__ import annotations

import datetime
import importlib.util
import pathlib

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "machine_status_export" / "export_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(), reason="machine_status_export plugin が未配置")

NOW = 1_800_000_000.0


@pytest.fixture(scope="module")
def m():
    spec = importlib.util.spec_from_file_location("machine_status_export_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


def _iso(epoch: float) -> str:
    return datetime.datetime.fromtimestamp(epoch, datetime.timezone.utc).isoformat()


def _sample(age_s: float, **kw):
    s = {"ts": _iso(NOW - age_s), "util_pct": 40.0, "temp_c": 61.0, "mem_used_mb": 9000,
         "mem_total_mb": 16311, "mem_util_pct": 55.5, "power_w": 120.0}
    s.update(kw)
    return s


def test_gpu_row_has_temp_vram_and_mem_bandwidth(m):
    metrics = {"workers": {"w_ai_gpu1_a3": {"0": [_sample(30), _sample(3)]}}}
    cpu = {"per_worker": {"w_ai_gpu1_a3": {"host": "ai-gpu1"}}, "per_host": {}}
    rows = m.build_rows(metrics, cpu, now=NOW)
    assert len(rows) == 1
    host, idx, util, temp, used, total, mem_util, cpu_pct, n_workers, sampled_at = rows[0]
    assert (host, idx) == ("ai-gpu1", 0)
    assert (util, temp, used, total, mem_util) == (40.0, 61.0, 9000, 16311, 55.5)
    assert cpu_pct is None and n_workers is None
    assert 120.0 not in rows[0]  # 消費電力は書かない


def test_newest_sample_wins_across_workers_and_stale_is_dropped(m):
    metrics = {"workers": {
        "w_ai_gpu1_a3": {"0": [_sample(8, mem_util_pct=10.0)]},
        "w_ai_gpu1_a4": {"0": [_sample(2, mem_util_pct=70.0)]},
        "w_ai_gpu9_a1": {"0": [_sample(60)]},  # 45 秒より古い
    }}
    rows = m.build_rows(metrics, {"per_worker": {}, "per_host": {}}, now=NOW)
    assert [(r[0], r[1], r[6]) for r in rows] == [("ai-gpu1", 0, 70.0)]


def test_cpu_row_leaves_gpu_columns_empty(m):
    cpu = {"per_worker": {}, "per_host": {"nas-c2": {"cpu_pct": 37.5, "n_workers": 11, "ts": _iso(NOW - 5)}}}
    rows = m.build_rows({"workers": {}}, cpu, now=NOW)
    assert rows == [("nas-c2", -1, None, None, None, None, None, 37.5, 11, m._utc(NOW - 5))]


def test_row_width_matches_upsert_placeholders(m):
    metrics = {"workers": {"w_ai_gpu1_a3": {"0": [_sample(1)]}}}
    cpu = {"per_worker": {}, "per_host": {"ai-gpu1": {"cpu_pct": 1.0, "n_workers": 1, "ts": _iso(NOW)}}}
    rows = m.build_rows(metrics, cpu, now=NOW)
    assert len(rows) == 2
    for r in rows:
        assert len(r) == m.UPSERT.count("?")
    assert "mem_util_pct" in m.DDL and "mem_util_pct" in m.ADD_COLUMNS


def test_proxmox_node_rows_have_cpu_and_ram(m):
    nodes = [
        {"node": "hall", "status": "online", "cpu": 0.362, "maxcpu": 128, "mem": 123 * 2**30, "maxmem": 188 * 2**30},
        {"node": "base", "status": "online", "cpu": 0.6421, "maxcpu": 128, "mem": 445_000 * 2**20, "maxmem": 515_000 * 2**20},
        {"node": "loft", "status": "offline"},                                     # 落ちている → 書かない
        {"node": "odd", "status": "online", "cpu": None, "maxmem": 1},            # 値が無い → 書かない
    ]
    rows = m.build_node_rows(nodes, now=NOW)
    assert rows == [
        ("base", -2, None, None, 445_000, 515_000, None, 64.2, None, m._utc(NOW)),
        ("hall", -2, None, None, 123 * 1024, 188 * 1024, None, 36.2, None, m._utc(NOW)),
    ]
    for r in rows:
        assert len(r) == m.UPSERT.count("?")


def test_proxmox_nodes_skipped_without_config(m):
    assert m._pve_nodes({"pve_urls": [], "pve_token": None}) == []
    assert m.build_node_rows([]) == []


def test_cpu_row_uses_proxmox_guest_cpu_when_known(m):
    cpu = {"per_worker": {}, "per_host": {
        "ai-gpu4": {"cpu_pct": 100.0, "n_workers": 5, "ts": _iso(NOW - 5)},   # CT: load は親ノードの値
        "nas-c2": {"cpu_pct": 100.0, "n_workers": 12, "ts": _iso(NOW - 5)},   # ゲスト名は nas
        "elsewhere": {"cpu_pct": 33.0, "n_workers": 2, "ts": _iso(NOW - 5)},  # ゲストでない → load のまま
    }}
    guests = m.build_guest_cpu([
        {"name": "ai-gpu4", "type": "lxc", "status": "running", "cpu": 0.083},
        {"name": "nas", "type": "lxc", "status": "running", "cpu": 0.237},
        {"name": "elsewhere", "type": "qemu", "status": "stopped", "cpu": 0.9},  # 止まっている → 使わない
        {"name": "no-cpu", "type": "qemu", "status": "running"},
    ])
    assert guests == {"ai-gpu4": 8.3, "nas": 23.7}
    rows = m.build_rows({"workers": {}}, cpu, now=NOW, guest_cpu=guests)
    assert {r[0]: r[7] for r in rows} == {"ai-gpu4": 8.3, "elsewhere": 33.0, "nas-c2": 23.7}


def test_role_suffixed_hosts_fold_into_their_machine(m):
    cpu = {"per_worker": {}, "per_host": {
        "nas-c2": {"cpu_pct": 40.0, "n_workers": 12, "ts": _iso(NOW - 9)},
        "nas-c2-pipeline-supervisor": {"cpu_pct": 55.0, "n_workers": 1, "ts": _iso(NOW - 3)},
        "nas-c2-proxmox-manager": {"cpu_pct": 50.0, "n_workers": 1, "ts": _iso(NOW - 4)},
        "ai-gpu1": {"cpu_pct": 10.0, "n_workers": 3, "ts": _iso(NOW - 1)},
        "ai-gpu10": {"cpu_pct": 20.0, "n_workers": 1, "ts": _iso(NOW - 1)},     # ai-gpu1 の役割ではない
    }}
    rows = m.build_rows({"workers": {}}, cpu, now=NOW)
    assert rows == [
        ("ai-gpu1", -1, None, None, None, None, None, 10.0, 3, m._utc(NOW - 1)),
        ("ai-gpu10", -1, None, None, None, None, None, 20.0, 1, m._utc(NOW - 1)),
        ("nas-c2", -1, None, None, None, None, None, 55.0, 14, m._utc(NOW - 3)),
    ]


def test_proxmox_guests_skipped_without_config(m):
    assert m._pve_guests({"pve_urls": [], "pve_token": None}) == []
    assert m.build_guest_cpu([]) == {}


def test_guest_cpu_is_averaged_over_the_window(m):
    hist: dict = {}
    assert m.smooth_guest_cpu(hist, {"ai-gpu1": 26.0, "nas": 20.0}, NOW, window_s=60) == {"ai-gpu1": 26.0, "nas": 20.0}
    assert m.smooth_guest_cpu(hist, {"ai-gpu1": 0.6, "nas": 30.0}, NOW + 10, window_s=60) == {"ai-gpu1": 13.3, "nas": 25.0}
    # 窓を出た古い値は使わない。今回出てこなかったゲスト (nas) は返さず、持ち回しからも消す
    assert m.smooth_guest_cpu(hist, {"ai-gpu1": 40.0}, NOW + 75, window_s=60) == {"ai-gpu1": 40.0}
    assert "nas" not in hist
