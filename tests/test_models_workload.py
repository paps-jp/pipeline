"""Workload Pydantic モデルのテスト。"""

from __future__ import annotations

import pytest
from pydantic import ValidationError

from pipeline.models.workload import WorkloadCreate, queue_table_for


def test_minimal_create() -> None:
    w = WorkloadCreate(
        slug="image-resize",
        name="Image Resize",
        executor_type="shell",
        executor_config={"command": "echo {task.pk}"},
    )
    assert w.slug == "image-resize"
    assert w.enabled is False
    assert w.priority == 100
    assert w.weight == 1.0


def test_invalid_slug() -> None:
    with pytest.raises(ValidationError):
        WorkloadCreate(
            slug="Image Resize!",  # 大文字 + space + 記号 = NG
            name="x",
            executor_type="shell",
        )


def test_extra_field_forbidden() -> None:
    with pytest.raises(ValidationError):
        WorkloadCreate(
            slug="ok",
            name="x",
            executor_type="shell",
            random_field=1,  # extra forbid
        )


def test_queue_table_for() -> None:
    assert queue_table_for("image-resize") == "image_resize_queue"
    assert queue_table_for("my_load") == "my_load_queue"


def test_queue_table_invalid_slug() -> None:
    with pytest.raises(ValueError):
        queue_table_for("BAD SLUG")
