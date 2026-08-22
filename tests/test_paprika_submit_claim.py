"""Regression: the submit queue must claim rows, not delete them at pop.

2026-08-14. Paprika's ``POST /jobs`` went from a 9.9s median to 1.2s, which
doubled how fast this plugin drains ``paprika_job_q`` -- and duplicate submits
exploded from 10% to **55% of all attempts** (per tick: 151 submitted vs 181
409s). The extra capacity was entirely absorbed by URLs paprika was already
crawling, so throughput did not move.

The cause was a gap with no owner. Popping DELETEd the queue row, but the
``crawl.downloaded_at`` mark is only written when the POST *returns*:

    pop (DELETE)  ──►  POST in flight (~1.2s)  ──►  result → mark downloaded_at
                  └── row is in NEITHER the queue NOR marked ──┘

and the filler selects ``WHERE downloaded_at IS NULL ORDER BY c.id ASC``, i.e.
the oldest unfinished rows -- exactly the ones sitting in that gap. So the
collision was structural, not chance, and it got worse the faster we drained.

Claiming instead of deleting closes the gap: the row stays, so the queue's
``UNIQUE(url)`` blocks re-insertion for the whole in-flight window, and the
queue itself becomes the in-flight registry.

These tests drive the SQL through a fake cursor -- the point is the state
machine (which statement runs when), not MariaDB behaviour.
"""

import re

import pytest

from plugins.paprika_job_submit import submit_main as sm


class _FakeCursor:
    def __init__(self, db):
        self.db = db
        self.description = None
        self._rows = []
        self.rowcount = 0

    def execute(self, sql, args=None):
        self.db.sql.append((" ".join(sql.split()), args))
        norm = " ".join(sql.split()).upper()
        if norm.startswith("SELECT"):
            self._rows = self.db.select_rows
            self.description = self.db.select_desc
            self.rowcount = len(self._rows)
        else:
            self._rows = []
            self.rowcount = self.db.write_rowcount

    def fetchall(self):
        return self._rows

    def fetchone(self):
        return self._rows[0] if self._rows else None

    def close(self):
        pass


class _FakeDB:
    def __init__(self, select_rows=None, select_desc=None, write_rowcount=1):
        self.sql = []
        self.select_rows = select_rows or []
        self.select_desc = select_desc
        self.write_rowcount = write_rowcount
        self.commits = 0

    def cursor(self):
        return _FakeCursor(self)

    def commit(self):
        self.commits += 1

    def rollback(self):
        pass

    def statements(self):
        return [s for s, _a in self.sql]


def _find(db, pattern):
    rx = re.compile(pattern, re.I)
    return [s for s in db.statements() if rx.search(s)]


# --------------------------------------------------------------------------
# pop must claim, never delete
# --------------------------------------------------------------------------

def test_fifo_pop_claims_instead_of_deleting():
    desc = [("id",), ("crawl_id",), ("url",), ("site",),
            ("download_video",), ("min_asset_size_bytes",), ("attempts",)]
    db = _FakeDB(select_rows=[(7, 100, "https://x.tld/a", "x", 1, 2048, 0)],
                 select_desc=desc)
    rows = sm._pop_from_q(db, limit=5)
    assert rows and rows[0]["id"] == 7
    assert _find(db, r"UPDATE paprika_job_q SET claimed_at"), db.statements()
    assert not _find(db, r"DELETE FROM paprika_job_q"), "pop must not delete"


def test_pop_only_considers_unclaimed_rows():
    """Otherwise a second popper would re-submit a row already in flight."""
    desc = [("id",), ("crawl_id",), ("url",), ("site",),
            ("download_video",), ("min_asset_size_bytes",), ("attempts",)]
    db = _FakeDB(select_rows=[], select_desc=desc)
    sm._pop_from_q(db, limit=5)
    sel = _find(db, r"SELECT .*FROM paprika_job_q")[0]
    assert "CLAIMED_AT IS NULL" in sel.upper()


def test_claim_ids_claims_and_filters_on_unclaimed():
    db = _FakeDB(select_rows=[(3,), (4,)])
    got = sm._claim_ids(db, [3, 4])
    assert got == {3, 4}
    sel = _find(db, r"SELECT id FROM paprika_job_q")[0]
    assert "CLAIMED_AT IS NULL" in sel.upper()
    assert _find(db, r"UPDATE paprika_job_q SET claimed_at")
    assert not _find(db, r"DELETE FROM paprika_job_q")


def test_peek_skips_claimed_rows():
    desc = [("id",), ("crawl_id",), ("url",), ("site",),
            ("download_video",), ("min_asset_size_bytes",), ("attempts",)]
    db = _FakeDB(select_rows=[], select_desc=desc)
    sm._peek_q(db, window=100)
    sel = _find(db, r"SELECT .*FROM paprika_job_q")[0]
    assert "CLAIMED_AT IS NULL" in sel.upper()


# --------------------------------------------------------------------------
# the row is released only when the work is accounted for
# --------------------------------------------------------------------------

def test_release_deletes_the_claimed_row():
    """Deleting is what frees UNIQUE(url) so the filler may queue it again."""
    db = _FakeDB()
    sm._release_q_row(db, 42)
    assert _find(db, r"DELETE FROM paprika_job_q WHERE id=")
    assert db.commits == 1


def test_release_is_a_noop_without_an_id():
    db = _FakeDB()
    sm._release_q_row(db, 0)
    assert db.statements() == []


def test_requeue_returns_the_row_to_stock_with_a_bumped_attempt():
    """Transient failures must NOT re-INSERT: the row still exists (claimed),
    so INSERT IGNORE would be silently dropped and the URL lost."""
    db = _FakeDB(write_rowcount=1)
    assert sm._requeue_q_row(db, 42, 2) is True
    upd = _find(db, r"UPDATE paprika_job_q SET claimed_at=NULL")[0]
    assert "ATTEMPTS" in upd.upper()
    assert not _find(db, r"INSERT")


def test_requeue_reports_failure_when_the_row_is_gone():
    """Then, and only then, the caller falls back to re-inserting."""
    db = _FakeDB(write_rowcount=0)
    assert sm._requeue_q_row(db, 42, 2) is False


# --------------------------------------------------------------------------
# crash safety
# --------------------------------------------------------------------------

def test_stale_claims_are_released():
    """A claim whose result never landed (process died, deadline hit) would
    otherwise hold UNIQUE(url) forever and strand that URL."""
    db = _FakeDB(write_rowcount=3)
    assert sm._release_stale_claims(db, older_than_s=120) == 3
    upd = _find(db, r"UPDATE paprika_job_q SET claimed_at=NULL")[0]
    assert "CLAIMED_AT IS NOT NULL" in upd.upper()
    assert "INTERVAL" in upd.upper()


def test_stale_window_outlasts_the_post_timeout():
    """The POST timeout is 30s and a tick can still be draining after that;
    releasing sooner would double-submit work that is merely slow."""
    assert sm._CLAIM_STALE_S >= 60


# --------------------------------------------------------------------------
# queue depth
# --------------------------------------------------------------------------

def test_depth_counts_only_unclaimed_rows():
    """Counting claimed rows as stock makes the filler think the queue is
    full and stop refilling -- starving the drainer."""
    db = _FakeDB(select_rows=[(12,)])
    assert sm._q_depth(db) == 12
    sel = _find(db, r"SELECT COUNT\(\*\) FROM paprika_job_q")[0]
    assert "CLAIMED_AT IS NULL" in sel.upper()


def test_schema_and_migration_carry_claimed_at():
    assert "claimed_at" in sm._CREATE_Q_SQL
    assert "claimed_at" in sm._ADD_CLAIMED_AT_SQL
    assert "IF NOT EXISTS" in sm._ADD_CLAIMED_AT_SQL.upper()
