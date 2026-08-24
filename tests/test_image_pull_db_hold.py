"""DB 接続を 「DB を触っている間だけ」 握ること。

2026-08-24 の実測。 サムネの同期 PUT を外して download 段が 3.8 倍になった
直後、 律速がプールへ移った:

    tick      プール容量   需要    比     実測待ち
    20:25:45   977 秒     1,116   114%   5,031 秒
    20:24:52   468 秒       672   143%   2,456 秒
    20:23:50   565 秒       700   124%   2,669 秒

需要が容量を超えているので飽和し、 96 スレッドの窓のうち常時 55 本前後が
接続待ちで止まっていた。 ところが **DB 自体は暇** (Threads_running 1-4、
Connection_errors_max_connections=0)。 つまりクエリが遅いのではなく、
`acquire()` から `release()` までの間に

    _make_thumb     29.1 ms   CPU
    _put_raw_minio  43.0 ms   .17 MinIO への送信
    db_insert        5.3 ms   ← DB が要るのはここだけ

と、 **DB を必要としない作業が挟まって接続を占有していた** (挿入経路の占有
615 秒のうち 573 秒 = 93% が非 DB 作業)。 サイズを上げると接続数だけ増えて
この保持は隠れるので、 先に借用範囲を狭める。

**順序は変えられない。** `_mark_image_downloaded` は `_put_raw_minio` が成功
した後でなければならない —— raw を保存する前に 「取得済み」 と記録すると、
実体の無い行が残る。 よって 「挿入まで握る → 返す → 非 DB 作業 → 取り直して
mark」 の 2 区間に割る。

plugins は別リポジトリでテスト基盤が無いため、 ここからソースを読んで固定する。
"""

from __future__ import annotations

import re
from pathlib import Path

import pytest

_IM = Path(__file__).resolve().parents[1] / "plugins" / "paprika_image_pull" / "image_main.py"
pytestmark = pytest.mark.skipif(not _IM.exists(), reason="plugins リポジトリ未チェックアウト")

SRC = _IM.read_text(encoding="utf-8")


def _asset_fn() -> str:
    """_process_one_asset の本体だけ切り出す。"""
    i = SRC.index("def _process_one_asset(")
    j = SRC.index("\ndef ", i + 10)
    return SRC[i:j]


def _pos(needle: str, body: str | None = None) -> int:
    body = _asset_fn() if body is None else body
    return body.index(needle)


# ------------------------------------ 非 DB 作業を握ったままにしないこと --

@pytest.mark.parametrize("call,why", [
    ("_make_thumb(tmp_path)", "サムネ生成は CPU 作業で DB を使わない"),
    ("_put_raw_minio(", ".17 MinIO への送信で DB を使わない"),
])
def test_non_db_work_runs_without_a_connection(call, why):
    """接続を返してから実行されること —— これが b1 の本体。"""
    body = _asset_fn()
    first_release = _pos("db_pool.release(db, broken=_db_broken)", body)
    assert _pos(call, body) > first_release, why


def test_insert_still_holds_the_connection():
    """挿入は当然 DB が要る。 1 区間目の中に在ること。"""
    body = _asset_fn()
    acq = _pos("db = _db_acquire()", body)
    rel = _pos("db_pool.release(db, broken=_db_broken)", body)
    assert acq < _pos("_insert_crawl_image(", body) < rel


def test_file_no_allocation_shares_the_first_hold():
    """採番も DB。 わざわざ別区間にして acquire を増やさない。"""
    body = _asset_fn()
    acq = _pos("db = _db_acquire()", body)
    rel = _pos("db_pool.release(db, broken=_db_broken)", body)
    assert acq < _pos("_next_file_no(", body) < rel


# ------------------------------------------------ 順序が保たれていること --

def test_mark_happens_after_the_raw_is_stored():
    """**順序の要件。** 保存前に 「取得済み」 と記録すると実体の無い行が残る。"""
    body = _asset_fn()
    assert _pos("_put_raw_minio(", body) < _pos("_mark_image_downloaded(", body)


def test_mark_reacquires_its_own_connection():
    """返した後なので取り直す。 借りっぱなしにしない。"""
    body = _asset_fn()
    i = _pos("_put_raw_minio(", body)
    seg = body[i:]
    assert seg.index("db = _db_acquire()") < seg.index("_mark_image_downloaded(")


def test_two_acquires_exactly():
    """区間は 2 つ。 増やすと ping (2.1ms) がそのぶん増える。"""
    assert _asset_fn().count("db = _db_acquire()") == 2


# --------------------------------------------- 返却が漏れないこと --------

def test_every_hold_releases_in_finally():
    """early return が多い関数なので finally 以外での返却は必ず漏れる。"""
    body = _asset_fn()
    rels = [m.start() for m in re.finditer(r"db_pool\.release\(", body)]
    assert len(rels) == 2, "借用区間と返却の数が合っていない"
    for r in rels:
        head = body[:r]
        assert head.rstrip().endswith("if db is not None:"), (
            "release の直前が `if db is not None:` (= finally 内) でない")


def test_released_connection_is_nulled_out():
    """返した後も db が残っていると、 後続の finally が二重返却する。"""
    body = _asset_fn()
    for m in re.finditer(r"db_pool\.release\(db, broken=_db_broken\)\n(\s*)db = None", body):
        pass
    assert body.count("db_pool.release(db, broken=_db_broken)\n                db = None") == 2


def test_broken_connection_is_not_reused_in_either_hold():
    """接続が落ちたら捨てる。 両区間で同じ扱いにする。"""
    body = _asset_fn()
    assert body.count("_db_broken = True") == 2
    for m in re.finditer(r"_db_broken = True", body):
        # 直前の非コメント行が except であること (間にコメントが入りうる)
        lines = body[:m.start()].splitlines()
        prev = [x.strip() for x in lines
                if x.strip() and not x.strip().startswith("#")][-1]
        assert prev == "except _mariadb.OperationalError:", (
            "_db_broken は OperationalError の except で立てること (直前=%r)" % prev)


# ----------------------------------------- 失敗が握ったままにならないこと --

def test_non_db_failure_still_lands_in_the_outer_handler():
    """MinIO 送信が落ちても従来どおり dl_failed になること (挙動を変えない)。

    非 DB 区間を try の外に出してしまうと例外が別経路に出る。 outer except
    より内側 = 同じインデント段に居ることで担保する。
    """
    body = _asset_fn()
    line = [x for x in body.split("\n") if "_put_raw_minio(" in x and "def " not in x][0]
    assert len(line) - len(line.lstrip()) == 8, (
        "非 DB 区間が outer try の内側 (8 スペース) に無い")
    assert "result[\"dl_failed\"] = 1" in body


# --------------------------------------------------- 計器が残ること ------

def test_reacquire_cost_is_measured():
    """acquire が 1 回増えたぶんの実費を見えるようにしておく。"""
    body = _asset_fn()
    assert '_st["db_acq2"]' in body


@pytest.mark.parametrize("stage", [
    "download", "db_insert", "thumb", "raw_put", "db_mark",
])
def test_existing_stage_timings_survive(stage):
    """前後比較ができなくなるので既存の段名は消さない。

    download だけは辞書リテラルで初期化される (`_st = {"download": ...}`)
    ので、 添字代入と両方を許す。
    """
    body = _asset_fn()
    assert (f'_st["{stage}"]' in body) or (f'{{"{stage}":' in body)
