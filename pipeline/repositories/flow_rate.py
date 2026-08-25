"""flow_rate_1m: フロー各ノードの 1 分バケット時系列を集約・読取する。

- server の _flow_rate_1m_loop が 60s 毎に「直前 1 分」を sample_minute() で集約 upsert。
- flow API はこの表を read_series() で引く (= runs を再スキャンしない → 複数ブラウザの
  アクセスに対するキャッシュ。 集約はサーバで 1 分 1 回だけ)。
- **long-format (ts_min, slug, metric, value)** なのでノード増減は行追加だけで吸収
  (固定カラム = slug 毎の列は作らない)。 新 workload / 新 metric は行が増えるだけ。

metric:
- 'runs_per_min'      = その分に開始した成功 run 数 (= 全 workload 共通の rate)
- 'items_per_min'     = output_json の宣言 metric field の SUM (= 捌いた「件数/分」)
- 'raw_items_per_min' = output_json の「重複排除前」 宣言 field の SUM (= dedup 前の実件数/分。
  宣言した slug のみ。 RAW_METRIC_FIELDS 参照)
将来 tank depth 等も metric を足すだけで拡張できる。
"""
from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any

from pipeline.db.base import Database


def _floor_minute(dt: datetime) -> datetime:
    return dt.replace(second=0, microsecond=0)


class FlowRateRepository:
    def __init__(self, db: Database) -> None:
        self.db = db

    def _sum_metric_rows(
        self,
        conn: Any,
        metric_fields: dict[str, list[str]],
        metric_name: str,
        ts_min: str,
        t1: str,
    ) -> list[tuple[str, str, float]]:
        """metric_fields (slug → output_json field 名 list) を SUM して
        (slug, metric_name, value) の行 list を返す。 items_per_min /
        raw_items_per_min で SQL 組み立てが同じなので共通化。"""
        if not metric_fields:
            return []
        all_fields = sorted({f for fs in metric_fields.values() for f in fs})
        sum_cols = ", ".join(
            f'SUM(COALESCE(JSON_EXTRACT(output_json, "$.{f}"), 0)) AS "m_{f}"'
            for f in all_fields
        )
        ph = ", ".join(f":s{i}" for i in range(len(metric_fields)))
        params: dict[str, Any] = {"t0": ts_min, "t1": t1}
        for i, slug in enumerate(metric_fields.keys()):
            params[f"s{i}"] = slug
        cur = conn.execute(
            f"SELECT workload_slug AS s, {sum_cols} FROM runs "
            f"WHERE started_at >= :t0 AND started_at < :t1 AND success = 1 "
            f"AND workload_slug IN ({ph}) GROUP BY workload_slug",
            params,
        )
        out: list[tuple[str, str, float]] = []
        for r in cur.fetchall():
            slug = r["s"]
            total = 0.0
            for f in metric_fields.get(slug, []):
                v = r[f"m_{f}"]
                if v is not None:
                    total += float(v)
            out.append((slug, metric_name, total))
        return out

    def sample_minute(
        self,
        ts_start: datetime,
        metric_fields: dict[str, list[str]] | None = None,
        raw_metric_fields: dict[str, list[str]] | None = None,
    ) -> int:
        """[ts_start, ts_start+60s) の各 slug rate を集計し flow_rate_1m へ upsert。

        ts_start は分境界 (秒0) を渡す前提 (内部でも floor する)。 戻り値 = upsert 行数。
        started_at index を使うので runs が数百万行でも 1 分幅なら軽い。
        raw_metric_fields = 「重複排除前」 の宣言 field (metric="raw_items_per_min" で書く)。
        """
        ts_start = _floor_minute(ts_start)
        ts_min = ts_start.isoformat()
        t1 = (ts_start + timedelta(minutes=1)).isoformat()
        rows: list[tuple[str, str, float]] = []  # (slug, metric, value)

        with self.db.transaction() as conn:
            # 1) runs_per_min = その分に開始した成功 run 数
            cur = conn.execute(
                "SELECT workload_slug AS s, COUNT(*) AS c FROM runs "
                "WHERE started_at >= :t0 AND started_at < :t1 AND success = 1 "
                "GROUP BY workload_slug",
                {"t0": ts_min, "t1": t1},
            )
            for r in cur.fetchall():
                rows.append((r["s"], "runs_per_min", float(r["c"])))

            # 2) items_per_min = 宣言 metric field の SUM
            if metric_fields:
                rows.extend(self._sum_metric_rows(
                    conn, metric_fields, "items_per_min", ts_min, t1))

            # 3) raw_items_per_min = 「重複排除前」 宣言 field の SUM
            if raw_metric_fields:
                rows.extend(self._sum_metric_rows(
                    conn, raw_metric_fields, "raw_items_per_min", ts_min, t1))

            for slug, metric, value in rows:
                conn.execute(
                    "INSERT OR REPLACE INTO flow_rate_1m (ts_min, slug, metric, value) "
                    "VALUES (:ts, :slug, :metric, :val)",
                    {"ts": ts_min, "slug": slug, "metric": metric, "val": value},
                )
        return len(rows)

    def upsert(self, ts_min: datetime, slug: str, metric: str, value: float) -> None:
        """単一 (ts_min, slug, metric) の値を upsert。 外部ソース(hub 等)の rate を
        flow_rate_1m に載せる用 (sample_minute の runs 集約とは別経路)。"""
        ts = _floor_minute(ts_min).isoformat()
        with self.db.transaction() as conn:
            conn.execute(
                "INSERT OR REPLACE INTO flow_rate_1m (ts_min, slug, metric, value) "
                "VALUES (:ts, :slug, :metric, :val)",
                {"ts": ts, "slug": slug, "metric": metric, "val": float(value)},
            )

    def read_series(
        self,
        since_iso: str,
        until_iso: str | None = None,
        slug: str | None = None,
        metric: str | None = None,
    ) -> list[dict[str, Any]]:
        """flow_rate_1m を時系列で返す (グラフ用)。 runs を触らないので軽い。"""
        clauses = ["ts_min >= :since"]
        params: dict[str, Any] = {"since": str(since_iso)}
        if until_iso:
            clauses.append("ts_min < :until")
            params["until"] = str(until_iso)
        if slug:
            clauses.append("slug = :slug")
            params["slug"] = slug
        if metric:
            clauses.append("metric = :metric")
            params["metric"] = metric
        where = " AND ".join(clauses)
        with self.db.transaction() as conn:
            cur = conn.execute(
                f"SELECT ts_min, slug, metric, value FROM flow_rate_1m "
                f"WHERE {where} ORDER BY ts_min ASC, slug ASC",
                params,
            )
            return [dict(r) for r in cur.fetchall()]

    def latest_rates(self, metric: str = "runs_per_min") -> dict[str, float]:
        """最新 1 分バケットの slug→value (= flow snapshot の real-time rate 用)。"""
        with self.db.transaction() as conn:
            row = conn.execute(
                "SELECT MAX(ts_min) AS m FROM flow_rate_1m WHERE metric = :metric",
                {"metric": metric},
            ).fetchone()
            latest = row["m"] if row else None
            if not latest:
                return {}
            cur = conn.execute(
                "SELECT slug, value FROM flow_rate_1m WHERE ts_min = :ts AND metric = :metric",
                {"ts": latest, "metric": metric},
            )
            return {r["slug"]: float(r["value"]) for r in cur.fetchall()}

    def purge_old(self, retain_hours: int = 48) -> int:
        cutoff = (datetime.now(timezone.utc) - timedelta(hours=retain_hours)).isoformat()
        with self.db.transaction() as conn:
            cur = conn.execute(
                "DELETE FROM flow_rate_1m WHERE ts_min < :cut", {"cut": cutoff}
            )
            return cur.rowcount or 0
