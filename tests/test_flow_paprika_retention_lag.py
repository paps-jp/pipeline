"""フロー画面の「paprika 退役遅れ」タンクの約束事。

2026-11-03 に Paprika の jobs 表が 5,280 万行 / 120 GB まで育った。 退役 (5 日経過後の
削除) が流入の 5 分の 1 の速度しか出ておらず、 1 か月以上負けっぱなしだったのに、
その遅れはどの画面にも出ていなかった (気づいたのは管理画面の COUNT が 60 秒かかって
Redis キャッシュが切れた時)。 このタンクはその信号を出す。

固定するのは 3 点:
- 「行数」 ではなく 「cutoff を過ぎてから消されるまでの時間」 を見る (投入量が変わっても
  閾値の意味が変わらない)。 retention 日数は paprika.settings から読み、 固定値を持たない。
- MIN(completed_at) を使わない。 (completed_at, status) index を全走査して 3 秒かかる
  (2026-11-04 実測)。 先頭 1 行を ORDER BY ... LIMIT 1 で読むと 1〜4 ms。
- `unit` 付きなので件/分のバックログ集計 (_trend_tank_sqls) から外れる。
"""

from __future__ import annotations

from pathlib import Path

import yaml

from pipeline.api import flow

_YAML = Path(__file__).resolve().parents[1] / "pipeline" / "control" / "flow_layout.yaml"
_ID = "paprika_jobs_retention_lag"


def _node() -> dict:
    data = yaml.safe_load(_YAML.read_text(encoding="utf-8"))
    return next(n for n in data["nodes"] if n["id"] == _ID)


def _sql() -> str:
    return " ".join(_node()["metric_sql"].split())


def test_measures_time_past_cutoff_not_row_count():
    sql = _sql()
    assert "TIMESTAMPDIFF(SECOND" in sql
    assert "paprika.jobs" in sql
    assert "COUNT(" not in sql


def test_retention_days_come_from_paprika_settings():
    sql = _sql()
    assert "paprika.settings" in sql
    assert "job_retention_days" in sql


def test_reads_oldest_row_with_limit_not_min():
    sql = _sql()
    assert "MIN(" not in sql
    assert "completed_at IS NOT NULL" in sql
    assert "ORDER BY completed_at LIMIT 1" in sql


def test_is_a_minutes_gauge_outside_backlog_sums():
    n = _node()
    assert n["unit"] == "min"
    assert n["capacity_warn"] == 60
    # 軽い SQL なのでリクエストパスで評価する (背景実行に回す閾値未満)
    assert float(n["metric_ttl_s"]) <= flow._TANK_SYNC_MAX_TTL_S
    data = yaml.safe_load(_YAML.read_text(encoding="utf-8"))
    sqls, _ = flow._trend_tank_sqls(data["nodes"])
    assert _ID not in sqls
