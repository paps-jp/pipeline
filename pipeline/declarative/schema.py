"""宣言ファイル (pipeline.config.yaml) のスキーマ。

トップレベルの各セクション (workloads / deploy / hosts) は **書いたときだけ管理対象**になる。
セクションごと省略すれば、 その種類の設定には一切触れない。
"""

from __future__ import annotations

from typing import Any

from pydantic import BaseModel, ConfigDict, Field, field_validator

from pipeline.models.workload import ExecutorType, _validate_slug

SCHEMA_VERSION = 1

# --- 実行時に自動制御が動かす項目 (宣言の対象外) ---------------------------------
# workload.enabled   … 運用の起動/停止スイッチ (fleet stop・UI・supervisor)
# workload.priority  … supervisor がルールで引き上げる
# workload.weight    … 同上
# host_policy.enabled … watchdog (GPU 故障/ディスク満杯) が自動で外す
# agent template     … goodput / elastic が毎 tick 書く台数
# これらを宣言に含めると、 apply のたびに自動制御と上書きし合う。
RUNTIME_OWNED_WORKLOAD_FIELDS = ("enabled", "priority", "weight")


class _Strict(BaseModel):
    model_config = ConfigDict(extra="forbid")


class WorkloadDecl(_Strict):
    """workload 1 件の宣言。 書かなかった項目は既定値 = 「既定値にする」という宣言。"""

    name: str = Field(min_length=1, max_length=128)
    executor_type: ExecutorType
    description: str | None = None
    executor_config: dict[str, Any] = Field(default_factory=dict)
    success_criteria: dict[str, Any] = Field(
        default_factory=lambda: {"type": "exit_code", "expected": 0}
    )
    batch_size: int = Field(default=10, ge=1, le=10000)
    lease_secs: int = Field(default=300, ge=10, le=86400)
    max_attempts: int = Field(default=5, ge=1, le=100)
    resources: dict[str, Any] = Field(default_factory=dict)
    # 許可リスト。 [] = 全ホスト許可 (実行時の意味と同じ)。
    host_affinity: list[str] = Field(default_factory=list)
    on_success: dict[str, Any] | None = None
    on_failure: dict[str, Any] | None = None
    supervisor_enabled: bool = True
    max_concurrent_per_host: int | None = Field(default=None, ge=1, le=100)
    max_concurrent_total: int | None = Field(default=None, ge=1, le=1000)
    requires_gpu: bool = False
    queue_backend: str = "sqlite"
    min_workers: int = Field(default=0, ge=0, le=100)
    max_workers: int | None = Field(default=None, ge=1, le=1000)

    @field_validator("host_affinity")
    @classmethod
    def _uniq_hosts(cls, v: list[str]) -> list[str]:
        if len(set(v)) != len(v):
            raise ValueError("host_affinity に重複があります")
        return v


class DeployTargetDecl(_Strict):
    label: str = Field(min_length=1, max_length=64)
    host: str = Field(min_length=1, max_length=128)
    ssh_user: str = "root"
    ssh_port: int = Field(default=22, ge=1, le=65535)
    enabled: bool = True
    notes: str | None = None


class DeployPathDecl(_Strict):
    label: str = Field(min_length=1, max_length=64)
    src_path: str = Field(min_length=1)
    dst_path: str = Field(min_length=1)
    enabled: bool = True
    delete_mode: bool = False
    setup_command: str | None = None
    service_command: str | None = None
    notes: str | None = None


class DeployDecl(_Strict):
    targets: list[DeployTargetDecl] | None = None
    paths: list[DeployPathDecl] | None = None


class HostDecl(_Strict):
    """host_policy の静的な手動設定。 enabled は watchdog が動かすので含めない。"""

    tier: str | None = None
    max_gpu_workers: int | None = Field(default=None, ge=0, le=64)
    vram_override_mb: int | None = Field(default=None, ge=0, le=200000)
    labels: list[str] = Field(default_factory=list)
    notes: str | None = None


class ConfigDecl(_Strict):
    version: int = SCHEMA_VERSION
    workloads: dict[str, WorkloadDecl] | None = None
    deploy: DeployDecl | None = None
    hosts: dict[str, HostDecl] | None = None

    @field_validator("version")
    @classmethod
    def _version(cls, v: int) -> int:
        if v != SCHEMA_VERSION:
            raise ValueError(f"未対応の version です (対応: {SCHEMA_VERSION})")
        return v

    @field_validator("workloads")
    @classmethod
    def _slugs(cls, v: dict[str, WorkloadDecl] | None) -> dict[str, WorkloadDecl] | None:
        for slug in (v or {}):
            _validate_slug(slug)
        return v
