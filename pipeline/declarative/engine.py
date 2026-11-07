"""宣言ファイルと DB の突き合わせ: export / plan / apply。

- export : 今の DB を YAML にする (既定値は省いて短くする)
- plan   : YAML と DB の差分を出す。 DB は 1 行も書き換えない
- apply  : plan を実行する。 既定では「ファイルに無いもの」は消さない (prune=True のときだけ)

apply はトランザクションをまたげない (各リポジトリが個別にコミットする) ので、
途中で失敗したら **そこで止めて**、 適用済み/未適用を Change.applied / error に残す。
plan を先に通して検証 (pydantic) を済ませるため、 失敗の大半は書き込み前に分かる。
"""

from __future__ import annotations

import logging
from dataclasses import dataclass, field
from typing import Any

import yaml
from pydantic import BaseModel, ValidationError

from pipeline.declarative.schema import (
    ConfigDecl,
    DeployPathDecl,
    DeployTargetDecl,
    HostDecl,
    WorkloadDecl,
)
from pipeline.models.workload import WorkloadBase, WorkloadCreate, WorkloadUpdate
from pipeline.repositories.deploy_paths import DeployPathsRepository
from pipeline.repositories.deploy_targets import DeployTargetsRepository
from pipeline.repositories.host_policy import HostPolicyRepository
from pipeline.repositories.workloads import WorkloadRepository

log = logging.getLogger("pipeline.declarative")

# 並べ替えて比較する (順序に意味が無い) 項目
_UNORDERED = {"host_affinity", "labels"}
# 空文字 / 空 dict を「未設定 (None)」と同一視する項目 (DB 側の保存形が揺れるため)
_EMPTY_IS_NONE = {
    "notes", "setup_command", "service_command", "description",
    "on_success", "on_failure", "tier",
}

SECTION_ORDER = ("workload", "deploy_target", "deploy_path", "host")


class ConfigError(Exception):
    """宣言ファイルが不正 (構文 / スキーマ / 値)。 messages に 1 件 1 行で入る。"""

    def __init__(self, messages: list[str]) -> None:
        super().__init__("; ".join(messages))
        self.messages = messages


# ---------------------------------------------------------------- 構文 ----


def parse_yaml(text: str) -> ConfigDecl:
    try:
        raw = yaml.safe_load(text) if text.strip() else {}
    except yaml.YAMLError as e:
        raise ConfigError([f"YAML の構文エラー: {e}"]) from e
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigError(["トップレベルは map (キー: 値) で書いてください"])
    try:
        return ConfigDecl.model_validate(raw)
    except ValidationError as e:
        raise ConfigError(_flatten(e)) from e


def _flatten(e: ValidationError) -> list[str]:
    out = []
    for err in e.errors():
        loc = ".".join(str(x) for x in err["loc"])
        out.append(f"{loc}: {err['msg']}")
    return out


# ------------------------------------------------------------- 差分表現 ----


@dataclass
class Change:
    section: str          # workload / deploy_target / deploy_path / host
    key: str              # slug / label / host 名
    op: str               # create / update / delete / unchanged / extra / skipped
    diff: dict[str, dict[str, Any]] = field(default_factory=dict)
    note: str | None = None
    applied: bool = False
    error: str | None = None
    # 実行用 (シリアライズしない)
    payload: Any = field(default=None, repr=False)
    row_id: int | None = field(default=None, repr=False)

    def to_dict(self) -> dict[str, Any]:
        return {
            "section": self.section,
            "key": self.key,
            "op": self.op,
            "diff": self.diff,
            "note": self.note,
            "applied": self.applied,
            "error": self.error,
        }


@dataclass
class Plan:
    changes: list[Change] = field(default_factory=list)

    def summary(self) -> dict[str, int]:
        s = {"create": 0, "update": 0, "delete": 0, "unchanged": 0, "extra": 0, "skipped": 0}
        for c in self.changes:
            s[c.op] = s.get(c.op, 0) + 1
        return s

    @property
    def has_changes(self) -> bool:
        return any(c.op in ("create", "update", "delete") for c in self.changes)

    @property
    def ok(self) -> bool:
        return all(c.error is None for c in self.changes)

    def to_dict(self) -> dict[str, Any]:
        return {
            "summary": self.summary(),
            "has_changes": self.has_changes,
            "ok": self.ok,
            "changes": [c.to_dict() for c in self.changes],
        }


def _defaults(model: type[BaseModel]) -> dict[str, Any]:
    return {
        n: f.get_default(call_default_factory=True)
        for n, f in model.model_fields.items()
        if not f.is_required()
    }


def _norm(name: str, v: Any) -> Any:
    if name in _UNORDERED and isinstance(v, list):
        return sorted(v)
    if name in _EMPTY_IS_NONE and (v == "" or v == {}):
        return None
    return v


def _diff(cur: dict[str, Any], des: dict[str, Any]) -> dict[str, dict[str, Any]]:
    out: dict[str, dict[str, Any]] = {}
    for k, after in des.items():
        before = cur.get(k)
        if _norm(k, before) != _norm(k, after):
            out[k] = {"before": before, "after": after}
    return out


def _non_default(model: type[BaseModel], d: dict[str, Any]) -> dict[str, dict[str, Any]]:
    """create の表示用: 必須項目と既定値以外だけを after として並べる。"""
    dflt = _defaults(model)
    return {
        k: {"before": None, "after": v}
        for k, v in d.items()
        if k not in dflt or _norm(k, v) != _norm(k, dflt[k])
    }


# ---------------------------------------------------------------- plan ----


def plan_config(db: Any, cfg: ConfigDecl, prune: bool = False) -> Plan:
    plan = Plan()
    if cfg.workloads is not None:
        _plan_workloads(db, cfg.workloads, prune, plan)
    if cfg.deploy is not None:
        if cfg.deploy.targets is not None:
            _plan_labelled("deploy_target", DeployTargetsRepository(db).list_all(),
                           cfg.deploy.targets, DeployTargetDecl, prune, plan)
        if cfg.deploy.paths is not None:
            _plan_labelled("deploy_path", DeployPathsRepository(db).list_all(),
                           cfg.deploy.paths, DeployPathDecl, prune, plan)
    if cfg.hosts is not None:
        _plan_hosts(db, cfg.hosts, plan)
    return plan


def _plan_workloads(db: Any, want: dict[str, WorkloadDecl], prune: bool, plan: Plan) -> None:
    repo = WorkloadRepository(db)
    current = {w.slug: w for w in repo.list_all()}
    base_fields = set(WorkloadBase.model_fields)
    errors: list[str] = []
    for slug, decl in want.items():
        des = decl.model_dump()
        try:
            if slug not in current:
                payload = WorkloadCreate(slug=slug, **des)   # enabled=False で作る
                plan.changes.append(Change(
                    "workload", slug, "create",
                    diff=_non_default(WorkloadDecl, des),
                    note="無効 (enabled=false) で作成します。 有効化は運用画面から",
                    payload=payload))
                continue
            cur_obj = current[slug]
            cur = {k: getattr(cur_obj, k) for k in WorkloadDecl.model_fields}
            diff = _diff(cur, des)
            if not diff:
                plan.changes.append(Change("workload", slug, "unchanged"))
                continue
            merged = {**cur_obj.model_dump(include=base_fields), **des}
            payload = WorkloadUpdate.model_validate(merged)
            plan.changes.append(Change("workload", slug, "update", diff=diff, payload=payload))
        except ValidationError as e:
            errors.extend(f"workloads.{slug}.{m}" for m in _flatten(e))
    if errors:
        raise ConfigError(errors)
    for slug, cur_obj in current.items():
        if slug in want:
            continue
        if not prune:
            plan.changes.append(Change("workload", slug, "extra",
                                       note="ファイルにありません (prune しないので残します)"))
        elif cur_obj.enabled:
            plan.changes.append(Change("workload", slug, "skipped",
                                       note="有効なため削除しません。 先に無効化してください"))
        else:
            plan.changes.append(Change("workload", slug, "delete",
                                       note="キュー表 (<slug>_queue) は残ります"))


def _plan_labelled(
    section: str,
    rows: list[dict[str, Any]],
    want: list[Any],
    model: type[BaseModel],
    prune: bool,
    plan: Plan,
) -> None:
    by_label: dict[str, dict[str, Any]] = {}
    for r in rows:
        by_label.setdefault(r["label"], r)
    seen: set[str] = set()
    for decl in want:
        d = decl.model_dump()
        label = d["label"]
        if label in seen:
            raise ConfigError([f"deploy: label が重複しています: {label}"])
        seen.add(label)
        cur = by_label.get(label)
        if cur is None:
            plan.changes.append(Change(section, label, "create",
                                       diff=_non_default(model, d), payload=d))
            continue
        cur_cmp = {k: (bool(cur[k]) if isinstance(d[k], bool) else cur[k]) for k in d}
        diff = _diff(cur_cmp, d)
        if diff:
            plan.changes.append(Change(section, label, "update", diff=diff,
                                       payload={k: d[k] for k in diff}, row_id=int(cur["id"])))
        else:
            plan.changes.append(Change(section, label, "unchanged"))
    for label, cur in by_label.items():
        if label in seen:
            continue
        if prune:
            plan.changes.append(Change(section, label, "delete", row_id=int(cur["id"])))
        else:
            plan.changes.append(Change(section, label, "extra",
                                       note="ファイルにありません (prune しないので残します)"))


def _plan_hosts(db: Any, want: dict[str, HostDecl], plan: Plan) -> None:
    repo = HostPolicyRepository(db)
    for host, decl in want.items():
        des = decl.model_dump()
        cur = repo.get(host)
        if cur is None:
            plan.changes.append(Change("host", host, "create",
                                       diff=_non_default(HostDecl, des), payload=des))
            continue
        diff = _diff({k: cur.get(k) for k in des}, des)
        if diff:
            plan.changes.append(Change("host", host, "update", diff=diff, payload=des))
        else:
            plan.changes.append(Change("host", host, "unchanged"))
    # ホストは「ファイルに無い」ことを理由に消さない (実機の登録を壊さないため)


# --------------------------------------------------------------- apply ----


def apply_config(db: Any, cfg: ConfigDecl, prune: bool = False,
                 by: str = "declarative") -> Plan:
    plan = plan_config(db, cfg, prune)
    wl = WorkloadRepository(db)
    targets = DeployTargetsRepository(db)
    paths = DeployPathsRepository(db)
    hosts = HostPolicyRepository(db)

    order = {s: i for i, s in enumerate(SECTION_ORDER)}
    # 作成・更新を先に、 削除は最後 (途中で止まっても「消えただけ」にならない)
    todo = sorted(
        (c for c in plan.changes if c.op in ("create", "update", "delete")),
        key=lambda c: (c.op == "delete", order[c.section]),
    )
    for c in todo:
        try:
            if c.section == "workload":
                if c.op == "create":
                    wl.create(c.payload, created_by=by)
                elif c.op == "update":
                    wl.update(c.key, c.payload)
                else:
                    wl.delete(c.key)
            elif c.section in ("deploy_target", "deploy_path"):
                repo = targets if c.section == "deploy_target" else paths
                if c.op == "create":
                    repo.create(**c.payload)
                elif c.op == "update":
                    repo.update(c.row_id, **c.payload)
                else:
                    repo.delete(c.row_id)
            elif c.section == "host":
                hosts.upsert(c.key, c.payload, updated_by=by)
            c.applied = True
            log.info("[declarative] %s %s %s (by=%s)", c.op, c.section, c.key, by)
        except Exception as e:  # noqa: BLE001 - 1 件失敗で止めて報告する
            c.error = f"{type(e).__name__}: {e}"
            log.warning("[declarative] %s %s %s 失敗: %s", c.op, c.section, c.key, c.error)
            break
    return plan


# -------------------------------------------------------------- export ----

_HEADER = """\
# pipeline.config.yaml — 宣言的構成 (config as code)
#
# 反映手順:  pipeline config plan -f <このファイル>   (差分を確認。 DB は変わらない)
#            pipeline config apply -f <このファイル>  (反映)
#
# 管理対象: workloads (定義・許可リスト) / deploy (配信先・配信パス) / hosts (静的ポリシー)
# 管理対象外 (自動制御が実行時に動かす): workload の enabled・priority・weight、
#   host の enabled、 agent の台数。 apply しても上書きされない。
# セクションごと省略すれば、 その種類には一切触れない。
# 書かなかった項目は「既定値にする」という意味。 ファイルに無い workload / 配信先は、
#   既定では消さない (apply --prune のときだけ。 有効な workload は prune でも消さない)。
"""


def _strip(model: type[BaseModel], d: dict[str, Any], keep: tuple[str, ...]) -> dict[str, Any]:
    dflt = _defaults(model)
    return {
        k: v
        for k, v in d.items()
        if k in keep or k not in dflt or _norm(k, v) != _norm(k, dflt[k])
    }


def export_config(db: Any) -> dict[str, Any]:
    workloads: dict[str, Any] = {}
    for w in WorkloadRepository(db).list_all():
        d = {k: getattr(w, k) for k in WorkloadDecl.model_fields}
        d["host_affinity"] = list(d["host_affinity"])
        workloads[w.slug] = _strip(WorkloadDecl, d, keep=("name", "executor_type"))

    def labelled(rows: list[dict[str, Any]], model: type[BaseModel]) -> list[dict[str, Any]]:
        required = tuple(k for k, f in model.model_fields.items() if f.is_required())
        out = []
        for r in rows:
            d = {}
            for k, f in model.model_fields.items():
                d[k] = bool(r[k]) if isinstance(f.default, bool) else r[k]
            out.append(_strip(model, d, keep=required))
        return out

    hosts: dict[str, Any] = {}
    for h in HostPolicyRepository(db).list():
        d = {k: h.get(k) for k in HostDecl.model_fields}
        stripped = _strip(HostDecl, d, keep=())
        if stripped:
            hosts[h["host"]] = stripped

    return {
        "version": 1,
        "workloads": workloads,
        "deploy": {
            "targets": labelled(DeployTargetsRepository(db).list_all(), DeployTargetDecl),
            "paths": labelled(DeployPathsRepository(db).list_all(), DeployPathDecl),
        },
        "hosts": hosts,
    }


def dump_yaml(cfg: dict[str, Any]) -> str:
    body = yaml.safe_dump(cfg, sort_keys=False, allow_unicode=True,
                          default_flow_style=False, width=100)
    return _HEADER + "\n" + body
