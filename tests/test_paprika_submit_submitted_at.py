"""filler は crawl.submitted_at で「未投入」を判定する (2026-10-29 cutover)。

downloaded_at は名前に反して「paprika 投入マーク」を兼ねていたので、 専用列
submitted_at に移した。 404 sentinel も submitted_at に書かないと、 cutover 後に
dead 行が IS NULL へ戻って永久に再投入される。
"""

from plugins.paprika_job_submit import submit_main as sm


class _Cur:
    def __init__(self, db):
        self.db = db
        self.description = [("id",), ("url",), ("site",)]

    def execute(self, sql, args=None):
        self.db.sql.append(" ".join(sql.split()))

    def fetchall(self):
        return []

    def fetchone(self):
        return None

    def close(self):
        pass


class _DB:
    def __init__(self):
        self.sql = []

    def cursor(self):
        return _Cur(self)

    def commit(self):
        pass

    def rollback(self):
        pass


def test_filler_and_probes_read_submitted_at():
    db = _DB()
    sm._get_target_urls(db, 10, sites=["a.example"])
    sm._get_stocked_sites(db, ["a.example"])
    sm._get_unsubmitted_sites(db, ["a.example"])
    reads = [q for q in db.sql if q.startswith("SELECT")]
    assert len(reads) == 3
    assert all("submitted_at" in q and "downloaded_at" not in q for q in reads)


def test_404_sentinel_marks_both_columns():
    db = _DB()
    sm._mark_crawl_404(db, 42)
    (q,) = db.sql
    assert "submitted_at='0000-00-00 00:00:00'" in q
    assert "downloaded_at='0000-00-00 00:00:00'" in q


def test_done_mark_still_dual_writes():
    db = _DB()
    sm._mark_crawl_done(db, 42)
    (q,) = db.sql
    assert "submitted_at=NOW()" in q and "downloaded_at=NOW()" in q
