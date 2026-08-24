"""`.47` tank の警告ラインが hub の spill 開始点と一致していること。

2026-08-24 の実害: hub の `asset_spill.high_pct` を 80 → 90 に上げたのに
フロー画面側の係数が 0.8 のままで、 **spill の 14G 手前 (112G) で警告が鳴った**。
`.47` は 110G / 140G = 79% で spill も起きていないのに 「溢れた」 と読めてしまい、
Paprika の job 投入を止める判断を 2 度誘発した。

警告が意味を持つのは 「ここを超えると hub が .17 (ディスク) へ spill し、
image-pull の取得経路が変わって遅くなる」 という一点なので、 warn ラインは
spill 開始点そのものに置く。 つまり **同じ数字が 3 箇所にある**:

    hub の asset_spill high_pct          (別リポジトリ / 実行時設定)
    flow_layout.yaml の capacity_sql 係数 (分母 = warn ライン)
    flow.py の _TANK_WARN_PCT             (総容量の逆算と crit 判定)

hub は別リポジトリなのでここからは触れない。 残る 2 箇所が食い違っていないこと
だけは固定する —— 今回ずれたのがまさにこの対で、 かたっぽだけ直すと
「液面 100% なのに spill していない」 「総容量 157.5G」 と表示がおかしくなる。

`.48` は probe 経路 (`_RAMDISK_*_PCT`) で tmpfs の性格も違うため巻き込まない。
その分離が保たれていることも併せて見る。
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

from pipeline.api import flow

_YAML = Path(__file__).resolve().parents[1] / "pipeline" / "control" / "flow_layout.yaml"
_TMPFS_G = 140.0  # .47 の tmpfs 実容量 (CT500)


def _capacity_coefficient() -> float:
    """yaml の capacity_sql が total_bytes に掛けている係数を取り出す。"""
    src = _YAML.read_text(encoding="utf-8")
    i = src.index("- id: minio-image-ram")
    j = src.index("- id: ", i + 10)
    m = re.search(r"total_bytes\s*\*\s*([0-9.]+)\s*/\s*1073741824", src[i:j])
    assert m, "minio-image-ram の capacity_sql に係数が見つからない"
    return float(m.group(1))


def _node(used: float, warn_at: float) -> flow.FlowNode:
    return flow.FlowNode(
        id="minio-image-ram", kind="tank", x=0, y=0, label="画像RAM (.47)",
        unit="GB", pending=used, capacity_warn=warn_at,
    )


def _alert(used: float, warn_at: float | None = None) -> dict | None:
    warn_at = _TMPFS_G * (flow._TANK_WARN_PCT / 100.0) if warn_at is None else warn_at
    got = flow._tank_ramdisk_alerts([_node(used, warn_at)])
    return got[0] if got else None


# ------------------------------------------------ 3 箇所が同じ数字であること --

def test_yaml_coefficient_matches_flow_constant():
    """これがずれたのが実害の本体。 片方だけ直せないよう対で固定する。"""
    assert _capacity_coefficient() == pytest.approx(flow._TANK_WARN_PCT / 100.0), (
        "flow_layout.yaml の capacity_sql 係数と flow.py の _TANK_WARN_PCT が"
        " 食い違っている (hub の asset_spill high_pct も同じ値に揃えること)")


def test_warn_line_is_the_spill_point_not_earlier():
    """warn = spill 開始点 (140G の 90% = 126G)。 112G ではない。"""
    warn_at = _TMPFS_G * _capacity_coefficient()
    assert warn_at == pytest.approx(126.0)
    assert _alert(125.0, warn_at) is None, "spill 前に警告を出している"
    assert _alert(126.0, warn_at) is not None


# -------------------------------------------- 総容量の逆算が実容量に戻ること --

def test_reported_total_is_the_real_tmpfs_size():
    """係数だけ変えて _TANK_WARN_PCT を放置すると総容量が 157.5G と嘘になる。"""
    a = _alert(130.0)
    assert a is not None
    assert "140.0G" in a["detail"], a["detail"]


def test_reported_pct_matches_the_real_occupancy():
    """126G / 140G は 90.0%。 分母が warn ラインになっていない。"""
    a = _alert(126.0)
    assert a is not None
    assert "(90.0%)" in a["detail"], a["detail"]


# -------------------------------------------------------- severity の段付け --

def test_spill_point_is_warn_not_crit():
    """spill は設計された逃がし弁。 到達しただけで crit にはしない。"""
    assert _alert(126.0)["severity"] == "warn"


def test_crit_only_near_actual_enospc():
    """crit は tmpfs が本当に埋まる直前 (95% = 133G) から。"""
    assert _alert(132.0)["severity"] == "warn"
    assert _alert(133.0)["severity"] == "crit"
    assert flow._TANK_CRIT_PCT > flow._TANK_WARN_PCT


# --------------------------------------------- .48 (probe 経路) を巻き込まない --

def test_probe_thresholds_are_independent():
    """.48 は別 tmpfs で envelope も違う。 tank 側の変更に追随させない。"""
    assert flow._RAMDISK_WARN_PCT == 80.0
    assert flow._RAMDISK_CRIT_PCT == 90.0
    assert flow._TANK_WARN_PCT != flow._RAMDISK_WARN_PCT


def test_tank_alert_only_covers_declared_tanks():
    """宣言していない tank から赤ボックスが漏れ出さないこと。"""
    n = _node(999.0, 10.0)
    n.id = "minio-video-ram"
    assert flow._tank_ramdisk_alerts([n]) == []
