"""page-keyword-compact のまとめ直し (identity personing 設計 §7.11 の 4) を固定する。

- 語の索引は終わった月を <root>/month=<月>/terms-NNNN・pages-NNNN.parquet に、
  N グラムは終わった日を <ng_root>/day=<日>/merged/grams-NNNN.parquet にまとめ直す
- 番号の範囲で分けて 1 本ずつ書く (時間切れでも続きから)。行を落とさず、本どうしの範囲は重ならない
- 行の数が入りと合わなければ書いたものを捨てて入りを残す。入りが変わったら最初からやり直す
- 日・月が終わって delay がたち、最後の 1 時間のまとめも済んでから始める
- 既定では動かない (ng_day_mode=0・kw_month_mode=0)
"""

from __future__ import annotations

import importlib.util
import json
import sys
import time
import types
from datetime import datetime, timezone
from pathlib import Path

import pytest

_PKC = Path(__file__).resolve().parents[1] / "plugins" / "page_keyword_compact" / "compact_main.py"
pytestmark = pytest.mark.skipif(not _PKC.exists(), reason="plugins リポジトリ未チェックアウト")


def _has(mod: str) -> bool:
    if mod in sys.modules:
        return True
    try:
        return importlib.util.find_spec(mod) is not None
    except (ImportError, ValueError):
        return False


needs_duck = pytest.mark.skipif(not (_has("duckdb") and _has("pyarrow")), reason="DuckDB / pyarrow が無い")


@pytest.fixture(scope="module")
def pkc():
    spec = importlib.util.spec_from_file_location("page_keyword_compact_remerge_test", _PKC)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


_NG_COLS = (("crawl_id", "int64"), ("gram", "int64"), ("field", "uint8"), ("tf", "uint16"))


def _write_ng(path, rows, row_group_size=2):
    import pyarrow as pa
    import pyarrow.parquet as pq
    path.parent.mkdir(parents=True, exist_ok=True)
    rows = sorted(rows, key=lambda r: (r[1], r[0]))       # 1 時間のファイルと同じく番号順
    t = pa.table({c: pa.array([r[k] for r in rows], getattr(pa, ty)()) for k, (c, ty) in enumerate(_NG_COLS)})
    pq.write_table(t, str(path), row_group_size=row_group_size)


def _ng_hours(ng, day, hours):
    """{時: 行} を day=<日>/hour=<時>/grams.parquet に書き、全部の行を返す"""
    allrows = []
    for h, rows in hours.items():
        _write_ng(ng / f"day={day}" / f"hour={h}" / "grams.parquet", rows)
        allrows += rows
    return allrows


def _inputs(ng):
    return sorted(str(p) for p in ng.glob("day=*/hour=*/grams.parquet"))


def test_plan_cuts_splits_by_row_group_ranges(pkc):
    stats = [(0, 9, 4), (10, 19, 4), (20, 29, 4), (30, 39, 4)]
    assert pkc.plan_cuts(stats, 8) == [20]
    assert pkc.plan_cuts(stats, 4) == [10, 20, 30]
    assert pkc.plan_cuts(stats, 100) == []
    # 重なった row group (別の時間のファイル) で、同じ区切りを 2 度作らない
    assert pkc.plan_cuts([(0, 5, 4), (0, 5, 4), (0, 9, 4), (6, 9, 4)], 4) == [6]


@needs_duck
def test_merge_ng_day_keeps_every_row_in_disjoint_sorted_parts(pkc, tmp_path):
    import duckdb
    import pyarrow.parquet as pq
    ng = tmp_path / "ng-v2"
    rows = _ng_hours(ng, "2026-11-23", {
        "00": [(1, 5, 0, 1), (2, -3, 5, 2), (3, 40, 4, 1), (4, 1 << 42, 5, 1), (5, 22, 5, 3)],
        "01": [(6, 5, 0, 1), (7, 21, 5, 1), (1, 5, 0, 1), (8, -3, 5, 1)],        # 同じページ・番号の 2 行目も残す
        "02": [(9, 39, 5, 2), (10, 6, 1, 1)],
    })
    (ng / "day=2026-11-23" / "hour=01" / ".grams.parquet.tmp").write_bytes(b"x")   # 1 時間のまとめの残り
    final = ng / "day=2026-11-23" / "merged"
    st = pkc.merge_unit(duckdb.connect(), {"grams": _inputs(ng)}, ng / "day=2026-11-23" / ".merged.tmp", final,
                        target_rows=3, row_group_size=2, deadline=time.time() + 60, part_budget_s=1)
    assert st["done"] and st["rows"] == len(rows) and st["parts"] >= 2
    parts = sorted(final.glob("grams-*.parquet"))
    assert len(parts) == st["parts"]
    got, prev_max = [], None
    for p in parts:
        t = pq.read_table(str(p))
        keys = [(r["gram"], r["crawl_id"]) for r in t.to_pylist()]
        assert keys == sorted(keys)                             # 1 本の中は番号順
        if prev_max is not None:
            assert keys[0][0] > prev_max                        # 本どうしの範囲は重ならない
        prev_max = keys[-1][0]
        assert [f.name for f in t.schema] == ["crawl_id", "gram", "field", "tf"]
        got += t.to_pylist()
    assert sorted((r["crawl_id"], r["gram"], r["field"], r["tf"]) for r in got) == sorted(rows)
    assert [f.name for f in (ng / "day=2026-11-23").iterdir()] == ["merged"]      # 1 時間のファイルは消えた
    assert (final / ".plan.json").exists() and not (ng / "day=2026-11-23" / ".merged.tmp").exists()


@needs_duck
def test_merge_unit_stops_between_parts_and_resumes(pkc, tmp_path, monkeypatch):
    import duckdb
    ng = tmp_path / "ng-v2"
    rows = _ng_hours(ng, "2026-11-23", {"00": [(i, i * 10, 5, 1) for i in range(8)]})
    files = _inputs(ng)
    tmp_dir, final = ng / "day=2026-11-23" / ".merged.tmp", ng / "day=2026-11-23" / "merged"
    clock = iter([0.0, 60.0])                                  # 1 本目は始められ、2 本目は時間が足りない
    monkeypatch.setattr(pkc.time, "time", lambda: next(clock, 1e9))
    st = pkc.merge_unit(duckdb.connect(), {"grams": files}, tmp_dir, final, target_rows=2, row_group_size=2,
                        deadline=100.0, part_budget_s=50.0)
    assert not st["done"] and st["parts"] == 1
    assert not final.exists() and all(Path(f).exists() for f in files)       # 途中では何も出さず、入りも残す
    assert [p.name for p in sorted(tmp_dir.glob("grams-*.parquet"))] == ["grams-0000.parquet"]
    monkeypatch.undo()
    st = pkc.merge_unit(duckdb.connect(), {"grams": files}, tmp_dir, final, target_rows=2, row_group_size=2,
                        deadline=time.time() + 60, part_budget_s=1)
    assert st["done"] and st["rows"] == len(rows) and st["parts"] == 3      # 続きの 3 本だけ書いた
    assert len(list(final.glob("grams-*.parquet"))) == 4 and not any(Path(f).exists() for f in files)


@needs_duck
def test_merge_unit_keeps_the_input_when_row_counts_do_not_match(pkc, tmp_path):
    import duckdb
    ng = tmp_path / "ng-v2"
    _ng_hours(ng, "2026-11-23", {"00": [(1, 1, 5, 1), (2, 2, 5, 1)]})
    files = _inputs(ng)
    tmp_dir, final = ng / "day=2026-11-23" / ".merged.tmp", ng / "day=2026-11-23" / "merged"
    tmp_dir.mkdir(parents=True)
    (tmp_dir / ".plan.json").write_text(json.dumps({"inputs": {"grams": files}, "rows_in": {"grams": 3},
                                                    "cuts": {"grams": []}}))
    with pytest.raises(RuntimeError):
        pkc.merge_unit(duckdb.connect(), {"grams": files}, tmp_dir, final, target_rows=10, row_group_size=2,
                       deadline=time.time() + 60, part_budget_s=1)
    assert not final.exists() and not tmp_dir.exists() and all(Path(f).exists() for f in files)


@needs_duck
def test_merge_unit_starts_over_when_the_input_changed(pkc, tmp_path):
    import duckdb
    ng = tmp_path / "ng-v2"
    _ng_hours(ng, "2026-11-23", {"00": [(1, 1, 5, 1)], "01": [(2, 2, 5, 1)]})
    files = _inputs(ng)
    tmp_dir, final = ng / "day=2026-11-23" / ".merged.tmp", ng / "day=2026-11-23" / "merged"
    tmp_dir.mkdir(parents=True)
    (tmp_dir / ".plan.json").write_text(json.dumps({"inputs": {"grams": files[:1]}, "rows_in": {"grams": 1},
                                                    "cuts": {"grams": []}}))
    (tmp_dir / "grams-0000.parquet").write_bytes(b"stale part")
    st = pkc.merge_unit(duckdb.connect(), {"grams": files}, tmp_dir, final, target_rows=10, row_group_size=2,
                        deadline=time.time() + 60, part_budget_s=1)
    assert st["done"] and st["rows"] == 2


def test_closed_ng_days_waits_for_the_day_end_and_the_last_hour(pkc, tmp_path):
    for day, hour in (("2026-11-23", "23"), ("2026-11-24", "00")):
        (tmp_path / f"day={day}" / f"hour={hour}").mkdir(parents=True)
        (tmp_path / f"day={day}" / f"hour={hour}" / "grams.parquet").write_bytes(b"")
    early = datetime(2026, 11, 24, 0, 50, tzinfo=timezone.utc)
    assert pkc.closed_ng_days(tmp_path, early, 3600) == []                # 日の終わり + 1 時間まで待つ
    late = datetime(2026, 11, 24, 1, 10, tzinfo=timezone.utc)
    assert [d for d, _f, _m in pkc.closed_ng_days(tmp_path, late, 3600)] == ["2026-11-23"]
    (tmp_path / "2026-11-23" / "23").mkdir(parents=True)                  # 1 時間のまとめがまだ済んでいない
    assert pkc.closed_ng_days(tmp_path, late, 3600) == []


def test_closed_kw_months_waits_for_the_month_end_and_the_last_hour(pkc, tmp_path):
    (tmp_path / "day=2026-11-30" / "hour=23").mkdir(parents=True)
    (tmp_path / "work" / "2026-11-30" / "23").mkdir(parents=True)
    now = datetime(2026, 12, 1, 2, 0, tzinfo=timezone.utc)
    assert pkc.closed_kw_months(tmp_path, now, 3600) == []
    (tmp_path / "work" / "2026-11-30" / "23").rmdir()
    (tmp_path / "work" / "2026-11-30").rmdir()
    assert [m for m, _i, _x in pkc.closed_kw_months(tmp_path, now, 3600)] == ["2026-11"]
    assert pkc.closed_kw_months(tmp_path, datetime(2026, 12, 1, 0, 30, tzinfo=timezone.utc), 3600) == []


@needs_duck
def test_remerge_pass_merges_a_finished_kw_month_and_leaves_the_current_one(pkc, tmp_path, monkeypatch):
    import duckdb
    import pyarrow as pa
    import pyarrow.parquet as pq

    def kw_hour(day, hour, terms, pages):
        d = tmp_path / f"day={day}" / f"hour={hour}"
        d.mkdir(parents=True)
        pq.write_table(pa.table({"crawl_id": pa.array([t[0] for t in terms], pa.int64()),
                                 "term_id": pa.array([t[1] for t in terms], pa.uint32()),
                                 "field": pa.array([5] * len(terms), pa.uint8()),
                                 "weight": pa.array([1.0] * len(terms), pa.float32()),
                                 "tf": pa.array([1] * len(terms), pa.uint16())}), str(d / "terms.parquet"))
        pq.write_table(pa.table({"crawl_id": pa.array(pages, pa.int64()),
                                 "site": pa.array(["s"] * len(pages)),
                                 "n_terms": pa.array([1] * len(pages), pa.int32()),
                                 "image_ids": pa.array([[c] for c in pages], pa.list_(pa.int64()))}),
                       str(d / "pages.parquet"))
    kw_hour("2026-11-29", "10", [(1, 7), (2, 3)], [1, 2])
    kw_hour("2026-11-30", "23", [(3, 7), (4, 1)], [3, 4])
    kw_hour("2026-12-01", "00", [(5, 2)], [5])                             # 次の月はまだ育っている

    class _Now(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 12, 1, 1, 30, tzinfo=timezone.utc)
    monkeypatch.setattr(pkc, "datetime", _Now)
    st = {"root": str(tmp_path), "ng_root": str(tmp_path / "ng-v2"), "duck": duckdb.connect(), "ng_mode": 0,
          "ng_day_mode": 0, "kw_month_mode": 1, "merge_part_rows": 1_000_000, "merge_delay_s": 3600,
          "part_budget_s": 1, "ng_row_group_size": 1000}
    out = pkc._remerge_pass(st, time.time() + 60)
    assert out["kw_months"] == 1 and out["kw_month_rows"] == 4 + 4 and out["remerge_errors"] == 0
    m = tmp_path / "month=2026-11"
    terms = pq.read_table(str(m / "terms-0000.parquet"))
    assert [(r["term_id"], r["crawl_id"]) for r in terms.to_pylist()] == [(1, 4), (3, 2), (7, 1), (7, 3)]
    assert terms.schema.field("term_id").type == pa.uint32()
    pages = pq.read_table(str(m / "pages-0000.parquet")).to_pylist()
    assert [r["crawl_id"] for r in pages] == [1, 2, 3, 4] and pages[0]["image_ids"] == [1]
    assert sorted(p.name for p in tmp_path.glob("day=*")) == ["day=2026-12-01"]
    assert pkc._remerge_pass(st, time.time() + 60)["kw_months"] == 0       # 2 度目は何もしない


@needs_duck
def test_remerge_pass_merges_a_finished_ng_day_only_with_both_modes(pkc, tmp_path, monkeypatch):
    import duckdb
    ng = tmp_path / "ng-v2"
    _ng_hours(ng, "2026-11-23", {"00": [(1, 1, 5, 1)], "23": [(2, 2, 5, 1)]})

    class _Now(datetime):
        @classmethod
        def now(cls, tz=None):
            return datetime(2026, 11, 24, 3, 0, tzinfo=timezone.utc)
    monkeypatch.setattr(pkc, "datetime", _Now)
    st = {"root": str(tmp_path), "ng_root": str(ng), "duck": duckdb.connect(), "ng_mode": 0, "ng_day_mode": 1,
          "kw_month_mode": 0, "merge_part_rows": 1_000_000, "merge_delay_s": 3600, "part_budget_s": 1,
          "ng_row_group_size": 1000}
    assert pkc._remerge_pass(st, time.time() + 60)["ng_days"] == 0         # ng_mode=0 なら触らない
    st["ng_mode"] = 1
    out = pkc._remerge_pass(st, time.time() + 60)
    assert out["ng_days"] == 1 and out["ng_day_rows"] == 2
    assert (ng / "day=2026-11-23" / "merged" / "grams-0000.parquet").exists()


def test_finish_leftover_removes_only_the_planned_inputs(pkc, tmp_path):
    day = tmp_path / "day=2026-11-23"
    (day / "hour=00").mkdir(parents=True)
    (day / "hour=05").mkdir(parents=True)
    a, b = day / "hour=00" / "grams.parquet", day / "hour=05" / "grams.parquet"
    a.write_bytes(b"a")
    b.write_bytes(b"b")
    (day / "merged").mkdir()
    (day / "merged" / ".plan.json").write_text(json.dumps({"inputs": {"grams": [str(a)]}}))
    assert pkc._finish_leftover(day / "merged") == 1
    assert not a.exists() and not (day / "hour=00").exists() and b.exists()


def test_remerge_is_off_by_default_and_process_does_not_wait_while_pending(pkc, tmp_path, monkeypatch):
    monkeypatch.setattr(pkc, "_self_enqueue_next_tick", lambda *a: None)
    if not _has("duckdb"):
        monkeypatch.setitem(sys.modules, "duckdb", types.ModuleType("duckdb"))
    st = pkc.setup(root=str(tmp_path))
    assert (st["ng_day_mode"], st["kw_month_mode"], st["merge_part_rows"], st["merge_delay_s"]) == \
        (0, 0, 200_000_000, 3600)
    called = []
    monkeypatch.setattr(pkc, "_remerge_pass", lambda *a: called.append(1) or {})
    monkeypatch.setattr(pkc, "closed_hours", lambda *a: [])
    pkc.run_pass(st, time.time() + 60)
    assert called == []
    monkeypatch.setattr(pkc, "run_pass", lambda state, deadline: {"hours": 0, "hours_left": 0, "remerge_pending": 1})
    slept = []
    monkeypatch.setattr(pkc.time, "sleep", lambda s: slept.append(s))
    clock = [0.0]
    monkeypatch.setattr(pkc.time, "monotonic", lambda: clock.__setitem__(0, clock[0] + 100) or clock[0])
    out = pkc.process(None, None, {"counter": 0, "loop_s": 600, "interval_s": 300, "control_url": "x",
                                   "workload_slug": "w"})
    assert slept == [] and out["remerge_pending"] == 1
