"""WorkloadRepository の単体テスト。"""

from __future__ import annotations

import pytest

from pipeline.db import get_db
from pipeline.models.workload import WorkloadCreate, WorkloadUpdate
from pipeline.repositories.workloads import (
    WorkloadAlreadyExists,
    WorkloadNotFound,
    WorkloadRepository,
)


def _repo() -> WorkloadRepository:
    db = get_db("sqlite:///:memory:")
    db.ensure_schema()
    return WorkloadRepository(db)


def _sample_create(slug: str = "image-resize") -> WorkloadCreate:
    return WorkloadCreate(
        slug=slug,
        name="Image Resize",
        description="resize jpegs to 800x600",
        executor_type="shell",
        executor_config={"command": "convert {task.pk} -resize 800x600 /tmp/{task.pk}.jpg"},
        priority=120,
        weight=2.0,
        batch_size=5,
    )


def test_create_and_get() -> None:
    repo = _repo()
    created = repo.create(_sample_create(), created_by="alice")
    assert created.slug == "image-resize"
    assert created.queue_table == "image_resize_queue"
    assert created.created_by == "alice"
    assert created.priority == 120

    got = repo.get("image-resize")
    assert got.name == "Image Resize"
    assert got.executor_config["command"].startswith("convert")


def test_duplicate_create_raises() -> None:
    repo = _repo()
    repo.create(_sample_create())
    with pytest.raises(WorkloadAlreadyExists):
        repo.create(_sample_create())


def test_get_not_found() -> None:
    repo = _repo()
    with pytest.raises(WorkloadNotFound):
        repo.get("nope")


def test_update() -> None:
    repo = _repo()
    repo.create(_sample_create())
    upd = WorkloadUpdate(
        name="Image Resize v2",
        description="updated",
        enabled=True,
        executor_type="shell",
        executor_config={"command": "echo updated"},
        priority=150,
    )
    result = repo.update("image-resize", upd)
    assert result.name == "Image Resize v2"
    assert result.enabled is True
    assert result.priority == 150


def test_set_enabled_toggle() -> None:
    repo = _repo()
    w = repo.create(_sample_create())
    assert w.enabled is False
    enabled = repo.set_enabled("image-resize", True)
    assert enabled.enabled is True
    disabled = repo.set_enabled("image-resize", False)
    assert disabled.enabled is False


def test_delete() -> None:
    repo = _repo()
    repo.create(_sample_create())
    repo.delete("image-resize")
    with pytest.raises(WorkloadNotFound):
        repo.get("image-resize")


def test_list_all_sorted() -> None:
    repo = _repo()
    repo.create(_sample_create("zulu"))
    repo.create(_sample_create("alpha"))
    items = repo.list_all()
    assert [w.slug for w in items] == ["alpha", "zulu"]


def test_queue_table_auto_created() -> None:
    repo = _repo()
    repo.create(_sample_create())
    # image_resize_queue 表が作られたか
    with repo.db.transaction() as conn:
        cur = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name='image_resize_queue'"
        )
        assert cur.fetchone() is not None
