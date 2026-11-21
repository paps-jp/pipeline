"""machine-status-export plugin が API の応答から machine_status の行を作る部分の回帰テスト。

守りたいこと (protection_ai の画面が前提にしている決まり):
  - GPU の行に温度・VRAM (使用/総量)・メモリ帯域 (mem_util_pct) が入る
  - 消費電力は書かない (GPU だけの値で実態より小さく見えるため)
  - 45 秒より古い GPU の値は書かない
  - CPU の行は gpu_index=-1 で、GPU 系の列は空
  - Proxmox ノードの行は gpu_index=-2 で、cpu_pct = PVE の cpu × 100、mem_used_mb / mem_total_mb = RAM。
    online でないノードは書かない
  - 行の並びが UPSERT の placeholder の数と一致する (列を足したときのずれ防止)

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
