"""Flow 赤ボックスの RAM ディスク (.48 video-ram) 逼迫アラートのテスト (2026-08-14)。

なぜ専用の probe が要るか: .48 は tmpfs 90G の上に載った MinIO で、
  - node_exporter が居ない
  - PVE の storage API は thin-pool しか返さず tmpfs は出てこない
ので既存の `_pve_alerts` では原理的に拾えない。 結果、 CT501 の `memory.peak` が
89.65 GiB (= ほぼ満杯) に達していたことが誰にも見えていなかった。

守る性質:
  - 使用率は `capacity total - free` (df 相当) で判定する。 live オブジェクトのみの
    `usage_total_bytes` で見ると、 実測でピーク時の 83〜99% を占めていた
    `.minio.sys/tmp/.trash` (削除待ち) を丸ごと見落とす。
  - 閾値未満では alert を出さない (= 平常時に赤ボックスを鳴らさない)。
  - 403 等で capacity が読めない場合は「読めない」ことを warn として出す。
    黙って 0 件を返すと「監視しているつもりで何も見ていない」状態に戻る。
"""

from __future__ import annotations

import pytest

from pipeline.api import flow

_GB = 1024 ** 3
_TOTAL = 90 * _GB


def _metrics(total: int, free: int, live: int) -> str:
    def _e(v: int) -> str:
        return f"{v:.10e}"

    return (
        "# TYPE minio_cluster_capacity_usable_total_bytes gauge\n"
        f'minio_cluster_capacity_usable_total_bytes{{server="127.0.0.1:9000"}} {_e(total)}\n'
        f'minio_cluster_capacity_usable_free_bytes{{server="127.0.0.1:9000"}} {_e(free)}\n'
        f'minio_cluster_usage_total_bytes{{server="127.0.0.1:9000"}} {_e(live)}\n'
    )


class _FakeResp:
    def __init__(self, text: str):
        self._b = text.encode("utf-8")

    def read(self) -> bytes:
        return self._b

    def __enter__(self):
        return self

    def __exit__(self, *a):
        return False


@pytest.fixture
def fake_metrics(monkeypatch):
    def _install(text: str):
        monkeypatch.setattr(flow.urllib.request, "urlopen",
                            lambda *a, **k: _FakeResp(text))
    return _install


def test_below_warn_emits_no_alert(fake_metrics):
    fake_metrics(_metrics(_TOTAL, _TOTAL - 20 * _GB, 5 * _GB))     # 22%
    assert flow._ramdisk_probe_one("video-ram", "http://x:9000") is None


def test_warn_threshold_emits_warn(fake_metrics):
    fake_metrics(_metrics(_TOTAL, _TOTAL - 68 * _GB, 10 * _GB))    # 75.6%
    a = flow._ramdisk_probe_one("video-ram", "http://x:9000")
    assert a is not None
    assert a["severity"] == "warn"
    assert a["kind"] == "storage_full"
    assert a["name"] == "ramdisk:video-ram"


def test_crit_threshold_emits_crit(fake_metrics):
    fake_metrics(_metrics(_TOTAL, _TOTAL - 81 * _GB, 12 * _GB))    # 90%
    a = flow._ramdisk_probe_one("video-ram", "http://x:9000")
    assert a is not None
    assert a["severity"] == "crit"


def test_detail_separates_live_from_pending_delete(fake_metrics):
    """detail に live と削除待ちの内訳を出す。

    「使用量は多いが live はわずか」という .48 特有の状態を、 赤ボックスを見た
    人がその場で判断できるようにするため。
    """
    fake_metrics(_metrics(_TOTAL, _TOTAL - 80 * _GB, 10 * _GB))
    a = flow._ramdisk_probe_one("video-ram", "http://x:9000")
    assert a is not None
    assert "live 10.0G" in a["detail"]
    assert "削除待ち 70.0G" in a["detail"]


def test_uses_capacity_not_live_usage(fake_metrics):
    """live が小さくても df 上が逼迫していれば鳴らす (trash を見落とさない)。"""
    fake_metrics(_metrics(_TOTAL, _TOTAL - 82 * _GB, 3 * _GB))
    a = flow._ramdisk_probe_one("video-ram", "http://x:9000")
    assert a is not None and a["severity"] == "crit"


def test_missing_capacity_metrics_reports_warn(fake_metrics):
    """403 等で capacity が読めないときは黙らず warn を出す。"""
    fake_metrics("minio_cluster_drive_total 1\n")
    a = flow._ramdisk_probe_one("video-ram", "http://x:9000")
    assert a is not None
    assert a["severity"] == "warn"
    assert "MINIO_PROMETHEUS_AUTH_TYPE" in a["error"]


# ── ターゲット設定 ───────────────────────────────────────────────────────────

def test_targets_default(monkeypatch):
    monkeypatch.delenv("PIPELINE_RAMDISK_TARGETS", raising=False)
    assert flow._ramdisk_targets() == [("video-ram:.48", "http://10.10.50.48:9000")]


def test_targets_can_be_disabled(monkeypatch):
    monkeypatch.setenv("PIPELINE_RAMDISK_TARGETS", "")
    assert flow._ramdisk_targets() == []
    assert flow._ramdisk_alerts() == []


def test_targets_multiple(monkeypatch):
    monkeypatch.setenv("PIPELINE_RAMDISK_TARGETS",
                       "a=http://1.2.3.4:9000, b=http://5.6.7.8:9000/")
    assert flow._ramdisk_targets() == [("a", "http://1.2.3.4:9000"),
                                       ("b", "http://5.6.7.8:9000")]


def test_probe_failure_does_not_raise(monkeypatch):
    """probe が例外を投げても refresh は落ちない (snapshot を壊さない)。"""
    monkeypatch.setenv("PIPELINE_RAMDISK_TARGETS", "a=http://1.2.3.4:9000")

    def _boom(*a, **k):
        raise OSError("connection refused")

    monkeypatch.setattr(flow.urllib.request, "urlopen", _boom)
    flow._refresh_ramdisk_health()
    assert flow._ramdisk_health["alerts"] == []
