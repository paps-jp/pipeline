"""サイト収率 (Thompson Sampling) の純関数部分。

投入側 (paprika_job_submit): 壁時計割引、 窓の分割、 optimistic TS の順位付け。
観測側 (paprika_image_pull): job → site への報酬帰属と、 site / lane 別の集計 SQL の形。
DB は触らない (SQL の実行順は fake cursor で見る)。
"""

from __future__ import annotations

import random
from collections import OrderedDict

from plugins.paprika_image_pull import image_main as im
from plugins.paprika_job_submit import submit_main as sm

# ---------------- 投入側 ----------------

def test_effective_decay_follows_half_life():
    s, f = sm._yield_effective(10.0, 4.0, age_s=48 * 3600, half_life_h=48.0)
    assert abs(s - 5.0) < 1e-9 and abs(f - 2.0) < 1e-9
    # 2 半減期
    s, f = sm._yield_effective(10.0, 4.0, age_s=96 * 3600, half_life_h=48.0)
    assert abs(s - 2.5) < 1e-9 and abs(f - 1.0) < 1e-9
    # 経過不明 / 未来 / 半減期 0 は減衰なし
    assert sm._yield_effective(10.0, 4.0, None, 48.0) == (10.0, 4.0)
    assert sm._yield_effective(10.0, 4.0, -5, 48.0) == (10.0, 4.0)
    assert sm._yield_effective(10.0, 4.0, 3600, 0) == (10.0, 4.0)
    # NULL 由来の None カウントは 0 扱い
    assert sm._yield_effective(None, None, 10, 48.0) == (0.0, 0.0)


def test_split_window_keeps_both_lanes_when_mixed():
    assert sm._split_window(40, 0.5) == (20, 20)
    assert sm._split_window(40, 0.0) == (0, 40)
    assert sm._split_window(40, 1.0) == (40, 0)
    # 端の share でも A/B が成立するよう両レーン 1 枠は残す
    assert sm._split_window(40, 0.01) == (1, 39)
    assert sm._split_window(40, 0.99) == (39, 1)
    # 窓 1 では分割できない
    assert sm._split_window(1, 0.5) == (0, 1)
    assert sm._split_window(1, 1.0) == (1, 0)
    # 不正値は 0 share
    assert sm._split_window(40, "x") == (0, 40)


def test_ts_rank_is_optimistic_and_prefers_high_yield():
    rng = random.Random(1234)
    stats = {"good": (50.0, 5.0), "bad": (5.0, 50.0), "new": (0.0, 0.0)}
    top = {"good": 0, "bad": 0, "new": 0}
    new_above_bad = 0
    for _ in range(300):
        ranked = sm._ts_rank_sites(stats, rng, prior=0.5)
        assert [r[0] for r in ranked] != []
        # θ* は平均を下回らない (optimistic TS)
        for _site, theta, mean in ranked:
            assert theta >= mean - 1e-12
        top[ranked[0][0]] += 1
        order = [r[0] for r in ranked]
        if order.index("new") < order.index("bad"):
            new_above_bad += 1
    # 良いサイトが大半で先頭。 残りは観測ゼロのサイト: Jeffreys 事前 Beta(0.5,0.5) は
    # U 字なので 2 割ほど θ≈1 を引き、 未知サイトが探索される (= 意図した cold start)。
    assert top["good"] >= 200, top
    assert top["bad"] == 0, top
    # 観測ゼロのサイトは平均 0.5 の高分散 → 枯れたサイトより上に来る
    assert new_above_bad >= 250, new_above_bad


def test_ts_rank_tolerates_bad_rows():
    rng = random.Random(0)
    ranked = sm._ts_rank_sites({"a": ("x", None), "b": (1, 1)}, rng)
    assert {r[0] for r in ranked} == {"a", "b"}


# ---------------- 観測側 ----------------

def _attr(**over):
    base = dict(
        job_expected={"j1": 3, "j2": 2, "j3": 2, "j4": 1, "j7": 1, "j8": 3},
        job_consumed={"j1": 3, "j2": 2, "j3": 1, "j4": 1, "j7": 1, "j8": 1},
        job_failed={"j4", "j8"},
        no_image_job_ids=["j5", "j6", ""],
        job_inserted={"j1": 2, "j8": 1},
        job_dup={"j2": 2, "j1": 1},
        job_status={"j1": "completed", "j2": "completed", "j3": "completed",
                    "j4": "completed", "j5": "failed", "j6": "review",
                    "j7": "completed", "j8": "completed"},
        job_site={k: "site-" + k for k in ("j1", "j2", "j3", "j4", "j5", "j6", "j7", "j8")},
        attributed=OrderedDict(j7=True),
    )
    base.update(over)
    return im._yield_attribute(**base)


def test_yield_attribute_rewards_and_skips():
    ev = _attr()
    # j1: 新規あり → 成功。 j2: 全部 dup で消費完了 → 失敗 (枯渇)。
    # j5: 画像ゼロの終端 job → 失敗。 j8: 一部 transient 失敗でも新規が取れたら成功。
    assert set(ev) == {"j1", "j2", "j5", "j8"}
    assert ev["j1"] == {"site": "site-j1", "r": 1, "new": 2, "dup": 1, "noimg": 0}
    assert ev["j2"] == {"site": "site-j2", "r": 0, "new": 0, "dup": 2, "noimg": 0}
    assert ev["j5"] == {"site": "site-j5", "r": 0, "new": 0, "dup": 0, "noimg": 1}
    assert ev["j8"]["r"] == 1
    # 飛ばすもの: j3 (途中 = deadline 繰越), j4 (transient 失敗で新規ゼロ),
    # j6 (review はまだ増える), j7 (帰属済み)


def test_yield_attribute_marks_attributed_once():
    attributed: OrderedDict = OrderedDict()
    # 空の LRU から始めると j7 (全部消費・新規ゼロ) も帰属対象になる
    first = _attr(attributed=attributed)
    assert set(first) == {"j1", "j2", "j5", "j7", "j8"}
    assert set(attributed) == {"j1", "j2", "j5", "j7", "j8"}
    # 同じ tick 内容をもう一度流しても二重計上しない (watermark 巻き戻し対策)
    second = _attr(attributed=attributed)
    assert second == {}


def test_yield_attribute_ignores_jobs_without_site():
    ev = _attr(job_site={"j1": "s"})
    assert set(ev) == {"j1"}


class _Cur:
    def __init__(self, db):
        self.db = db

    def executemany(self, sql, rows):
        self.db.calls.append((" ".join(sql.split()), list(rows)))

    def execute(self, sql, args=None):
        self.db.calls.append((" ".join(sql.split()), args))

    def fetchall(self):
        return []

    def close(self):
        pass


class _Db:
    def __init__(self):
        self.calls = []

    def cursor(self):
        return _Cur(self)


def test_yield_flush_aggregates_per_site_and_lane():
    db = _Db()
    events = {
        "j1": {"site": "A", "r": 1, "new": 2, "dup": 1, "noimg": 0},
        "j2": {"site": "A", "r": 0, "new": 0, "dup": 3, "noimg": 0},
        "j3": {"site": "B", "r": 0, "new": 0, "dup": 0, "noimg": 1},
    }
    lanes = {"j1": "ts", "j2": "ts"}   # j3 は crawl_submit_job に無い → na
    summary = im._yield_flush(db, events, lanes, half_life_h=48.0)
    assert summary["jobs"] == 3 and summary["productive"] == 1 and summary["sites"] == 2
    assert summary["by_lane"] == {"ts": {"jobs": 2, "productive": 1, "new": 2},
                                  "na": {"jobs": 1, "productive": 0, "new": 0}}
    assert len(db.calls) == 2
    upd_sql, upd_rows = db.calls[0]
    assert upd_sql.startswith("UPDATE crawl_config SET")
    # yield_updated_at は SET の最後 (前の式が旧値を見るため)
    assert upd_sql.index("yield_updated_at = NOW(6)") > upd_sql.index("yield_f = ")
    assert sorted(upd_rows, key=lambda r: r[-1]) == [
        (48.0, 1.0, 48.0, 1.0, 2, 2, "A"),   # A: s+=1, f+=1, jobs+=2, new+=2
        (48.0, 0.0, 48.0, 1.0, 1, 0, "B"),
    ]
    stat_sql, stat_rows = db.calls[1]
    assert stat_sql.startswith("INSERT INTO crawl_yield_stat")
    assert sorted(stat_rows) == [("na", 1, 0, 0, 0, 1), ("ts", 2, 1, 2, 4, 0)]


def test_yield_flush_no_events_writes_nothing():
    db = _Db()
    assert im._yield_flush(db, {}, {}, 48.0) == {}
    assert db.calls == []
