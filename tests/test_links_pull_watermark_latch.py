"""links-pull の watermark 固定点 (2026-08-22 の実障害) を塞ぐ。

実障害: watermark が `2026-08-22T12:49:37.895` で **2.5 時間凍結**し、 その間
`inserted` がずっと 0 だった。 hub を直接叩いて再現した固定点はこう:

    fetch_from = watermark - SAFETY_MARGIN_S(60s) = 12:48:37.895
    hub が返す 500 件のうち 200 件目 (= LINK_FETCH_CHUNK) の completed_at が
    ちょうど 12:49:37.895 = watermark そのもの
    → `chunk_max > watermark` が False なので保存されない
    → budget_hit で最終前進もスキップ → 次 tick も全く同じクエリ・同じ結果

成立条件は「1 チャンクがマージンを跨げない」こと、 つまり hub の完了レートが
`LINK_FETCH_CHUNK / SAFETY_MARGIN_S` = 200 jobs/分 を超えること。 hub は平常
500-600 jobs/分 出るので、 **watermark をどこに置いても再発する**。 巻き戻し幅を
縮めるだけではレートが上がればまた再発するので、 時刻の比較ではなく
**処理済み job を覚えて skip する** 形にした。

ここで固定するのは:
  1. マージン内の再読み分は skip され、 チャンクには未処理 job だけが入る
  2. その結果 chunk_max は watermark を越え、 固定点が構造的に消える
  3. 1 チャンクが丸ごと遅て到着でも、 その job は skip 側に入るので次 tick は必ず進む
  4. skip キャッシュは cap で頭打ちになる (無限に太らない)

plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
from collections import OrderedDict
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest

_LM = Path(__file__).resolve().parents[1] / "plugins" / "paprika_links_pull" / "links_main.py"
pytestmark = pytest.mark.skipif(not _LM.exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def lm():
    spec = importlib.util.spec_from_file_location("links_main_latch_test", _LM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


WM = datetime(2026, 8, 22, 12, 49, 37, 895000, tzinfo=timezone.utc)


def _jobs(start: datetime, n: int, rate_per_min: float, prefix="j"):
    """completed_at 昇順の job 列 (hub の keyset pagination と同じ並び)。"""
    step = 60.0 / rate_per_min
    return [
        {"job_id": "%s%05d" % (prefix, i),
         "url": "https://site%d.example/p%d" % (i % 3, i),
         "completed_at": (start + timedelta(seconds=step * (i + 1))).isoformat()}
        for i in range(n)
    ]


def _build_targets(lm, state, jobs):
    """本番の targets_cat 構築と同じ手順を再現する (skip → site 解決 → 並べる)。"""
    seen = state.setdefault("seen_jobs", OrderedDict())
    targets, skipped, no_site = [], 0, 0
    for j in jobs:
        jid = j.get("job_id")
        if not jid or not j.get("url"):
            continue
        if jid in seen:
            skipped += 1
            continue
        dom = lm._normalize_domain(j["url"])
        site = lm._site_for_domain(dom, state["site_cache"])
        if not site:
            no_site += 1
            continue
        targets.append((jid, site, dom, lm._parse_dt(j.get("completed_at"))))
    return targets, skipped, no_site


def _mark_chunk_seen(lm, state, chunk):
    seen = state["seen_jobs"]
    for t in chunk:
        seen[t[0]] = None
    while len(seen) > lm.SEEN_JOBS_CAP:
        seen.popitem(last=False)


def _state(lm):
    return {"site_cache": {"site0.example": "s0", "site1.example": "s1",
                           "site2.example": "s2"},
            "seen_jobs": OrderedDict(),
            "watermark": WM}


# ------------------------------------------------ 固定点そのものの再現 --

def test_the_observed_fixed_point_without_skip_cache(lm):
    """skip が無いと、 200 件目がちょうど watermark に落ちて前進しない。

    実障害の算数をそのまま置く: hub が 200 jobs/分 を超えると、
    fetch_from(=wm-60s) から数えて LINK_FETCH_CHUNK 件目が watermark 以前になる。
    """
    fetch_from = WM - timedelta(seconds=lm.SAFETY_MARGIN_S)
    # ちょうど 200 件目が watermark に一致するレート
    jobs = _jobs(fetch_from, 500, rate_per_min=lm.LINK_FETCH_CHUNK)
    chunk = jobs[:lm.LINK_FETCH_CHUNK]
    chunk_max = max(lm._parse_dt(j["completed_at"]) for j in chunk)
    assert chunk_max == WM                    # 200 件目 = watermark そのもの
    assert not (chunk_max > WM)               # → 保存条件が False = 前進しない


def test_rate_above_threshold_relatches_anywhere(lm):
    """hub が 200 jobs/分 を超えている限り、 watermark をどこに置いても再発する。"""
    for rate in (300, 500, 600):
        fetch_from = WM - timedelta(seconds=lm.SAFETY_MARGIN_S)
        jobs = _jobs(fetch_from, 500, rate_per_min=rate)
        chunk_max = max(lm._parse_dt(j["completed_at"])
                        for j in jobs[:lm.LINK_FETCH_CHUNK])
        assert chunk_max < WM, rate            # 巻き戻し幅の中に収まってしまう


# ------------------------------------------------ skip キャッシュで解消 --

def test_skip_cache_advances_within_a_bounded_number_of_ticks(lm):
    """保証されるのは「1 tick で前進」ではなく「有界な tick 数で必ず前進」。

    レートが高いとマージン (60s) の中に 1 チャンクより多くの job が入る。
    500 jobs/分なら 500 件 = 2.5 チャンクなので、 マージンを抜けるまでに
    ceil(500/200)=3 tick かかる。 重要なのは **必ず抜けること**で、 その上限が
    job 数で決まっていること (時刻比較のように永久に止まらないこと)。
    """
    rate = 500
    st = _state(lm)
    fetch_from = WM - timedelta(seconds=lm.SAFETY_MARGIN_S)
    jobs = _jobs(fetch_from, 1500, rate_per_min=rate)

    margin_jobs = rate * lm.SAFETY_MARGIN_S / 60.0
    bound = int(margin_jobs // lm.LINK_FETCH_CHUNK) + 2   # 余裕を 1 tick 見る

    advanced_at = None
    for tick in range(1, bound + 1):
        targets, skipped, _ = _build_targets(lm, st, jobs)
        assert skipped == (tick - 1) * lm.LINK_FETCH_CHUNK   # 再読み分は毎回タダ
        chunk = targets[:lm.LINK_FETCH_CHUNK]
        _mark_chunk_seen(lm, st, chunk)
        if max(t[3] for t in chunk) > WM:
            advanced_at = tick
            break
    assert advanced_at is not None, "%d tick 以内に前進しなかった" % bound
    assert advanced_at <= bound


def test_repeated_ticks_make_monotonic_progress(lm):
    """同じ窓を何 tick 読み直しても、 毎回きちんと前へ進む (固定点にならない)。"""
    st = _state(lm)
    fetch_from = WM - timedelta(seconds=lm.SAFETY_MARGIN_S)
    jobs = _jobs(fetch_from, 1000, rate_per_min=600)

    marks = []
    for _ in range(4):
        targets, _, _ = _build_targets(lm, st, jobs)
        if not targets:
            break
        chunk = targets[:lm.LINK_FETCH_CHUNK]
        cmax = max(t[3] for t in chunk)
        _mark_chunk_seen(lm, st, chunk)
        if st["watermark"] is None or cmax > st["watermark"]:
            st["watermark"] = cmax
        marks.append(st["watermark"])

    assert len(marks) >= 3
    assert marks == sorted(marks)                 # 単調
    assert marks[-1] > marks[0], "前進していない"


def test_all_late_arrivals_still_progress_next_tick(lm):
    """1 チャンクが丸ごと watermark より前 (遅て到着) でも永久ラッチにならない。"""
    st = _state(lm)
    late = _jobs(WM - timedelta(seconds=120), lm.LINK_FETCH_CHUNK,
                 rate_per_min=600, prefix="late")
    fresh = _jobs(WM, 200, rate_per_min=600, prefix="new")
    page = late + fresh

    t1, _, _ = _build_targets(lm, st, page)
    chunk1 = t1[:lm.LINK_FETCH_CHUNK]
    assert max(t[3] for t in chunk1) < WM        # 全部 watermark より前 = 進まない
    _mark_chunk_seen(lm, st, chunk1)

    # だが覚えたので、 次 tick の先頭は必ず新しい側になる
    t2, skipped, _ = _build_targets(lm, st, page)
    assert skipped == lm.LINK_FETCH_CHUNK
    assert max(t[3] for t in t2[:lm.LINK_FETCH_CHUNK]) > WM


# --------------------------------------------------------- cap の頭打ち --

def test_seen_cache_is_capped(lm):
    st = _state(lm)
    seen = st["seen_jobs"]
    for i in range(lm.SEEN_JOBS_CAP + 500):
        seen["x%06d" % i] = None
        while len(seen) > lm.SEEN_JOBS_CAP:
            seen.popitem(last=False)
    assert len(seen) == lm.SEEN_JOBS_CAP
    assert "x000000" not in seen                  # 古い方から落ちる
    assert "x%06d" % (lm.SEEN_JOBS_CAP + 499) in seen


def test_cap_covers_several_minutes_of_hub_output(lm):
    """cap が hub の完了レート数分ぶんを下回ると skip が効かなくなる。"""
    assert lm.SEEN_JOBS_CAP >= 600 * 10          # 600 jobs/分 x 10 分


def _setup(lm, monkeypatch, tmp_path, **kw):
    """DB に触らずに setup() を通す。 認証情報は env ファイルで与える。"""
    import sys as _sys
    import types

    env = tmp_path / "db.env"
    env.write_text("DB_HOST=h\nDB_PORT=3306\nDB_USER=u\nDB_PASS='p'\nDB_NAME=d\n",
                   encoding="utf-8")
    monkeypatch.setitem(_sys.modules, "mariadb",
                        types.SimpleNamespace(connect=lambda **k: types.SimpleNamespace(autocommit=False)))
    # setup() は os.uname() を呼ぶ (POSIX 専用)。 ここで見たいのは kwargs の
    # 配線だけなので、 Windows でも回せるよう差し替える。
    monkeypatch.setattr(lm.os, "uname",
                        lambda: types.SimpleNamespace(nodename="test"),
                        raising=False)
    monkeypatch.setattr(lm, "_build_site_cache", lambda db: {})
    monkeypatch.setattr(lm, "_load_watermark", lambda db, slug: None)
    monkeypatch.setattr(lm, "_self_enqueue_next_tick", lambda *a, **k: None)
    return lm.setup(db_env_file=str(env), **kw)


# ------------------------------------------------ ツマミが死んでいないこと --

def test_link_fetch_knobs_are_wired_into_state(lm, monkeypatch, tmp_path):
    """本体は `state.get("link_fetch_*")` で読む。 setup() が入れ忘れると
    **常に定数へ落ちる死んだツマミ**になる (2026-08-23 に実際そうなっていた)。"""
    st = _setup(lm, monkeypatch, tmp_path,
                link_fetch_concurrency=32, link_fetch_chunk=64,
                link_fetch_timeout_s=7.5)
    assert st["link_fetch_concurrency"] == 32
    assert st["link_fetch_chunk"] == 64
    assert st["link_fetch_timeout_s"] == 7.5


def test_link_fetch_knobs_default_to_constants(lm, monkeypatch, tmp_path):
    st = _setup(lm, monkeypatch, tmp_path)
    assert st["link_fetch_concurrency"] == lm.LINK_FETCH_CONCURRENCY
    assert st["link_fetch_chunk"] == lm.LINK_FETCH_CHUNK
