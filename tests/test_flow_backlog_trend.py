"""workload ボックスの「停滞しているか」判定 (= 上流バックログの増減) の不変条件。

背景色 (薄ピンク = 積み上がり / 薄い緑 = 消化中) の入力は
`FlowNode.backlog_trend_per_min`。 これを **edge の IN/OUT から出してはいけない** —
edge の `rate_per_min` は宣言 metric が欠けると隣の workload の throughput を
借りるため、 空のキューに 255/min が流れているような幽霊値になる
(memory: flow-edge-borrowed-throughput-artifact)。 信用できるのは tank の実 SQL
count だけなので、 判定は flow_rate_1m の `tank_level` 時系列だけで組む。

ここで固定するのは 3 点:
  1. 両端差分が 件/分 になっていること、 サンプル 1 点だけの tank は捨てること
  2. 単調増加する総数タンク (accumulator) をバックログに混ぜないこと
     —— 混ぜると paprika 投入が永久に 「停滞」 表示になる (crawl は総 URL 数)
  3. 全 workload が 1 つ以上の上流タンクに解決すること (= どれかが常に中立で
     色が付かない、 を防ぐ)
"""

from __future__ import annotations

import datetime as dt
from pathlib import Path

import yaml

from pipeline.api import flow

_YAML = Path(__file__).resolve().parents[1] / "pipeline" / "control" / "flow_layout.yaml"


def _layout() -> dict:
    return yaml.safe_load(_YAML.read_text(encoding="utf-8"))


class _FakeFRR:
    def __init__(self, rows: list[dict]) -> None:
        self.rows = rows

    def read_series(self, since: str, metric: str | None = None) -> list[dict]:
        return self.rows


def _series(now: dt.datetime, tank: str, start: float, step: float,
            minutes: int = 10) -> list[dict]:
    return [
        {"ts_min": (now - dt.timedelta(minutes=k)).isoformat(),
         "slug": tank, "value": start + step * (minutes - k)}
        for k in range(minutes, -1, -1)
    ]


def test_trend_is_per_minute_and_signed() -> None:
    now = dt.datetime(2026, 8, 31, 12, 0, tzinfo=dt.timezone.utc)
    rows = _series(now, "piling", 1000, 50) + _series(now, "draining", 5000, -20)
    out = flow._tank_level_trends(_FakeFRR(rows), now)
    assert out["piling"] == (50.0, 10.0)
    assert out["draining"] == (-20.0, 10.0)


def test_single_sample_tank_is_dropped() -> None:
    """履歴が 1 点しか無い tank は「横ばい 0」ではなく **判定不能**。

    0 を返すと再起動直後の全 workload が 「横ばい (中立)」 ではなく
    「実測した結果 動いていない」 に見えてしまう。
    """
    now = dt.datetime(2026, 8, 31, 12, 0, tzinfo=dt.timezone.utc)
    rows = [{"ts_min": now.isoformat(), "slug": "fresh", "value": 42}]
    assert flow._tank_level_trends(_FakeFRR(rows), now) == {}


def test_short_span_is_dropped() -> None:
    now = dt.datetime(2026, 8, 31, 12, 0, tzinfo=dt.timezone.utc)
    rows = _series(now, "young", 100, 10, minutes=1)   # span 1 分 < 最低 3 分
    assert flow._tank_level_trends(_FakeFRR(rows), now) == {}


def test_accumulator_tanks_are_excluded() -> None:
    """総数タンクは 「増えているのが正常」 なのでバックログ判定に入れない。"""
    nodes = _layout()["nodes"]
    sqls, _ = flow._trend_tank_sqls(nodes)
    for tid in ("crawl", "crawl_image_hash", "crawl_video_hash",
                "crawl_embedding_final", "crawl_person"):
        assert tid not in sqls, f"{tid} は accumulator なのでトレンド対象外のはず"
    # 逆に代表的なバックログタンクは必ず入っていること
    for tid in ("image_hash_extract_queue", "crawl_image", "crawl_face",
                "embed_write_queue", "video_face_extract_queue"):
        assert tid in sqls


def test_gb_tanks_are_excluded() -> None:
    """GB 単位の RAM ディスク tank は 件/分 として足せないので除外。"""
    sqls, _ = flow._trend_tank_sqls(_layout()["nodes"])
    assert "minio-image-ram" not in sqls
    assert "minio-video-ram" not in sqls


def test_every_workload_resolves_to_a_backlog_tank() -> None:
    """どの workload も上流タンクに解決できること (= 常時 中立色 の box を作らない)。

    解決規則は snapshot() と同じ: demand_from 宣言 → 無ければ tank→workload の
    実線 edge。 dashed は参照線なので使わない。
    """
    layout = _layout()
    nodes, edges = layout["nodes"], layout["edges"]
    tank_ids = {n["id"] for n in nodes if n.get("kind") == "tank"}
    trend_ids = set(flow._trend_tank_sqls(nodes)[0])
    upstream: dict[str, list[str]] = {}
    for e in edges:
        if e.get("dashed") or e["from"] not in tank_ids:
            continue
        upstream.setdefault(e["to"], []).append(e["from"])
    for n in nodes:
        if n.get("kind") != "workload":
            continue
        cand = n.get("demand_from") or upstream.get(n["id"]) or []
        used = [t for t in cand if t in trend_ids]
        assert used, f"{n['id']} に上流バックログタンクが無い"
