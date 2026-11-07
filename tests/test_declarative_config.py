"""宣言的構成管理 (export / plan / apply) のテスト。"""

from __future__ import annotations

import pytest

from pipeline.db import get_db
from pipeline.declarative.engine import (
    ConfigError,
    apply_config,
    dump_yaml,
    export_config,
    parse_yaml,
    plan_config,
)
from pipeline.models.workload import WorkloadCreate
from pipeline.repositories.deploy_paths import DeployPathsRepository
from pipeline.repositories.deploy_targets import DeployTargetsRepository
from pipeline.repositories.host_policy import HostPolicyRepository
from pipeline.repositories.workloads import WorkloadRepository


def _db():
    db = get_db("sqlite:///:memory:")
    db.ensure_schema()
    return db


def _seed(db) -> None:
    repo = WorkloadRepository(db)
    repo.create(WorkloadCreate(
        slug="image-embed", name="Image Embed", executor_type="python_module",
        executor_config={"module": "image_embed"}, requires_gpu=True,
        host_affinity=["ai-gpu1", "ai-gpu4"], priority=150, weight=2.0,
        max_concurrent_per_host=2,
    ), created_by="seed")
    repo.set_enabled("image-embed", True)
    repo.create(WorkloadCreate(
        slug="embed-write", name="Embed Write", executor_type="shell",
        executor_config={"command": "true"},
    ), created_by="seed")
    DeployTargetsRepository(db).create(label="ai-gpu1", host="10.0.0.1")
    DeployPathsRepository(db).create(
        label="plugin: image-embed", src_path="/src/image_embed", dst_path="/opt/image_embed",
        setup_command="pip install -r requirements.txt")
    HostPolicyRepository(db).upsert("ai-gpu1", {"tier": "gpu-16g-pro", "labels": ["a"],
                                                "enabled": False}, updated_by="seed")


def test_export_roundtrip_has_no_changes() -> None:
    db = _db()
    _seed(db)
    text = dump_yaml(export_config(db))
    plan = plan_config(db, parse_yaml(text))
    assert not plan.has_changes, plan.to_dict()
    assert plan.summary()["unchanged"] >= 4


def test_export_omits_defaults_and_runtime_owned() -> None:
    db = _db()
    _seed(db)
    cfg = export_config(db)
    w = cfg["workloads"]["image-embed"]
    assert "enabled" not in w and "priority" not in w and "weight" not in w
    assert "batch_size" not in w                       # 既定値は省く
    assert w["host_affinity"] == ["ai-gpu1", "ai-gpu4"]
    assert "enabled" not in cfg["hosts"]["ai-gpu1"]


def test_create_workload_is_disabled() -> None:
    db = _db()
    cfg = parse_yaml("""
workloads:
  new-one:
    name: New One
    executor_type: shell
    executor_config: {command: "true"}
""")
    plan = apply_config(db, cfg)
    assert plan.summary()["create"] == 1 and plan.ok
    w = WorkloadRepository(db).get("new-one")
    assert w.enabled is False and w.created_by == "declarative"


def test_update_changes_only_managed_fields() -> None:
    db = _db()
    _seed(db)
    cfg = parse_yaml(dump_yaml(export_config(db)))
    cfg.workloads["image-embed"].host_affinity = ["ai-gpu1"]
    plan = apply_config(db, cfg)
    assert plan.summary()["update"] == 1 and plan.ok
    ch = [c for c in plan.changes if c.op == "update"][0]
    assert ch.diff["host_affinity"]["after"] == ["ai-gpu1"]
    w = WorkloadRepository(db).get("image-embed")
    assert w.host_affinity == ["ai-gpu1"]
    # 自動制御が動かす項目は触らない
    assert w.enabled is True and w.priority == 150 and w.weight == 2.0


def test_host_affinity_order_is_not_a_change() -> None:
    db = _db()
    _seed(db)
    cfg = parse_yaml(dump_yaml(export_config(db)))
    cfg.workloads["image-embed"].host_affinity = ["ai-gpu4", "ai-gpu1"]
    assert not plan_config(db, cfg).has_changes


def test_extra_is_kept_without_prune() -> None:
    db = _db()
    _seed(db)
    cfg = parse_yaml("workloads:\n  embed-write:\n    name: Embed Write\n"
                     "    executor_type: shell\n    executor_config: {command: 'true'}\n")
    plan = apply_config(db, cfg)
    assert plan.summary()["extra"] == 1
    WorkloadRepository(db).get("image-embed")          # 消えていない


def test_prune_deletes_disabled_but_never_enabled() -> None:
    db = _db()
    _seed(db)
    cfg = parse_yaml("workloads: {}\n")
    plan = apply_config(db, cfg, prune=True)
    ops = {c.key: c.op for c in plan.changes}
    assert ops["embed-write"] == "delete"              # 無効 → 消える
    assert ops["image-embed"] == "skipped"             # 有効 → 消さない
    repo = WorkloadRepository(db)
    assert [w.slug for w in repo.list_all()] == ["image-embed"]


def test_missing_section_is_not_managed() -> None:
    db = _db()
    _seed(db)
    plan = apply_config(db, parse_yaml("version: 1\n"), prune=True)
    assert plan.changes == []
    assert len(WorkloadRepository(db).list_all()) == 2
    assert len(DeployTargetsRepository(db).list_all()) == 1


def test_deploy_and_hosts_apply() -> None:
    db = _db()
    _seed(db)
    cfg = parse_yaml("""
deploy:
  targets:
    - {label: ai-gpu1, host: 10.0.0.9}
    - {label: ai-gpu5, host: 10.0.0.5}
  paths: []
hosts:
  ai-gpu1: {tier: gpu-10g-consumer}
""")
    plan = apply_config(db, cfg, prune=True)
    assert plan.ok
    targets = {t["label"]: t for t in DeployTargetsRepository(db).list_all()}
    assert targets["ai-gpu1"]["host"] == "10.0.0.9"
    assert "ai-gpu5" in targets
    assert DeployPathsRepository(db).list_all() == []  # prune で空に
    h = HostPolicyRepository(db).get("ai-gpu1")
    assert h["tier"] == "gpu-10g-consumer"
    assert h["enabled"] == 0                           # watchdog 管轄の enabled は不変
    assert h["labels"] == []                           # 書かなかった = 既定値


def test_plan_does_not_write() -> None:
    db = _db()
    _seed(db)
    cfg = parse_yaml("workloads:\n  x:\n    name: X\n    executor_type: shell\n")
    plan_config(db, cfg, prune=True)
    assert [w.slug for w in WorkloadRepository(db).list_all()] == ["embed-write", "image-embed"]


@pytest.mark.parametrize("text, needle", [
    ("workloads: [1]\n", "workloads"),
    ("workloads:\n  Bad_Slug:\n    name: x\n    executor_type: shell\n", "slug"),
    ("workloads:\n  ok:\n    name: x\n    executor_type: shell\n    enabled: true\n", "enabled"),
    ("workloads:\n  ok:\n    name: x\n    executor_type: nope\n", "executor_type"),
    ("version: 2\n", "version"),
    ("deploy:\n  targets:\n    - {label: a, host: b}\n    - {label: a, host: c}\n", "重複"),
    ("a: [\n", "構文"),
    ("- 1\n- 2\n", "map"),
])
def test_invalid_config_is_rejected(text: str, needle: str) -> None:
    db = _db()
    with pytest.raises(ConfigError) as ei:
        plan_config(db, parse_yaml(text))
    assert needle in str(ei.value)
