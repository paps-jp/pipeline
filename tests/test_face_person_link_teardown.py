"""face-person-link plugin の teardown が state の資源を閉じることの回帰テスト。

守りたいこと:
  - eviction で呼ばれる teardown が shard の fd (os.open の生 int) を閉じる
    (無いと eviction 毎に 130〜170 本漏れ、 worker の soft limit 1024 を数分で使い切る。 2026-11-18)
  - http / db / shard_pool も閉じる。 1 つが例外を出しても残りは閉じる
  - 二重に呼んでも、 state でないものを渡しても例外にならない
  - executor の close() から実際に teardown が呼ばれる

plugin は非公開リポジトリの入れ子 clone なので、 未配置なら skip する。
"""

from __future__ import annotations

import importlib.util
import os
import pathlib
from concurrent.futures import ThreadPoolExecutor

import pytest

_PLUGIN = (pathlib.Path(__file__).resolve().parents[1]
           / "plugins" / "face_person_link" / "link_main.py")
pytestmark = pytest.mark.skipif(not _PLUGIN.exists(), reason="face_person_link plugin が未配置")


@pytest.fixture(scope="module")
def m():
    pytest.importorskip("httpx")
    pytest.importorskip("numpy")
    spec = importlib.util.spec_from_file_location("face_person_link_main_under_test", _PLUGIN)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


class _Closable:
    def __init__(self, fail: bool = False):
        self.closed = 0
        self.fail = fail

    def close(self):
        self.closed += 1
        if self.fail:
            raise RuntimeError("close failed")


def _is_open(fd: int) -> bool:
    try:
        os.fstat(fd)
        return True
    except OSError:
        return False


def _state_with_shards(tmp_path, n: int, *, db=None, http=None):
    cache = {}
    for i in range(n):
        p = tmp_path / f"emb_shard_{i}.bin"
        p.write_bytes(b"\0" * 2048)
        fd = os.open(str(p), os.O_RDONLY)
        cache[i] = {"fd": fd, "size": 1, "path": str(p)}
    return {
        "shard_cache": cache,
        "shard_pool": ThreadPoolExecutor(max_workers=2),
        "http": http if http is not None else _Closable(),
        "db": db if db is not None else _Closable(),
    }


def test_teardown_closes_shard_fds(m, tmp_path):
    st = _state_with_shards(tmp_path, 5)
    fds = [e["fd"] for e in st["shard_cache"].values()]
    assert all(_is_open(fd) for fd in fds)
    m.teardown(st)
    assert not any(_is_open(fd) for fd in fds)
    assert st["shard_cache"] == {}


def test_teardown_closes_http_db_and_pool(m, tmp_path):
    http, db = _Closable(), _Closable()
    st = _state_with_shards(tmp_path, 1, http=http, db=db)
    pool = st["shard_pool"]
    m.teardown(st)
    assert http.closed == 1 and db.closed == 1
    assert st["http"] is None and st["db"] is None and st["shard_pool"] is None
    with pytest.raises(RuntimeError):
        pool.submit(lambda: None)


def test_teardown_continues_after_close_failure(m, tmp_path):
    db = _Closable()
    st = _state_with_shards(tmp_path, 3, http=_Closable(fail=True), db=db)
    fds = [e["fd"] for e in st["shard_cache"].values()]
    m.teardown(st)
    assert db.closed == 1
    assert not any(_is_open(fd) for fd in fds)


def test_teardown_is_idempotent(m, tmp_path):
    st = _state_with_shards(tmp_path, 2)
    m.teardown(st)
    m.teardown(st)
    m.teardown(None)
    m.teardown({})


def test_executor_close_calls_teardown(m, tmp_path):
    """PythonModuleExecutor.close() は module の teardown を state 付きで呼ぶ。"""
    from pipeline.executors.python_module import PythonModuleExecutor

    st = _state_with_shards(tmp_path, 2)
    fds = [e["fd"] for e in st["shard_cache"].values()]
    ex = PythonModuleExecutor.__new__(PythonModuleExecutor)
    ex._module = m
    ex._module_name = "link_main"
    ex._state = st
    ex.close()
    assert not any(_is_open(fd) for fd in fds)
    assert ex._state is None
