"""Workload モデル (Pydantic v2)。

REST API の入出力 + DB row のシリアライズ両方に使う。
"""

from __future__ import annotations

import re
from datetime import datetime
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, field_validator

ExecutorType = Literal["shell", "http_post", "sql", "python_eval", "python_module", "container"]

_SLUG_RE = re.compile(r"^[a-z0-9][a-z0-9_-]{0,62}$")


def _validate_slug(v: str) -> str:
    if not _SLUG_RE.match(v):
        raise ValueError("slug は小文字英数 + `-` + `_`、先頭は英数、最大 63 文字")
    return v


class WorkloadBase(BaseModel):
    """workload の編集可能項目。"""

    model_config = ConfigDict(extra="forbid")

    name: str = Field(min_length=1, max_length=128)
    description: str | None = None
    enabled: bool = False

    executor_type: ExecutorType
    executor_config: dict[str, Any] = Field(default_factory=dict)

    success_criteria: dict[str, Any] = Field(
        default_factory=lambda: {"type": "exit_code", "expected": 0}
    )

    priority: int = Field(default=100, ge=0, le=1000)
    weight: float = Field(default=1.0, ge=0.0, le=100.0)
    batch_size: int = Field(default=10, ge=1, le=10000)
    lease_secs: int = Field(default=300, ge=10, le=86400)
    max_attempts: int = Field(default=5, ge=1, le=100)

    resources: dict[str, Any] = Field(default_factory=dict)
    host_affinity: list[str] = Field(default_factory=list)
    on_success: dict[str, Any] | None = None
    on_failure: dict[str, Any] | None = None

    # supervisor (= 自動オーケストレーター) が patch_workload / filter 変更系の
    # action をこの workload に対して実行することを許可するか。 既定 True (= 任せる)。
    # オペレータが手で値を握りたい局面(= 例: paprika-job-submit を最大化テスト中)で
    # False にすると、 supervisor は streak/cooldown は数えるが action は no-op に。
    supervisor_enabled: bool = True

    # 同一 host 上で同時に実行できる worker 数の上限。 None = 無制限 (= 既定)。
    # 用途: image-embed のように cuBLAS init で大きく VRAM を確保する plugin で、
    # 同一 GPU を多重起動して CUBLAS_STATUS_ALLOC_FAILED で setup 死亡するのを防ぐ。
    # workloads_for_worker が claim 候補から外す形で適用 (= best-effort)。
    max_concurrent_per_host: int | None = Field(default=None, ge=1, le=100)

    # GPU heavy か CPU only か (2026-06-28、 静的配置設計用)。
    # True = ONNX/CUDA を使う、 GPU host に置く。
    # False (=既定) = web/IO bound、 CPU host で十分。
    # operator が workload_filter を作るときに「この worker (GPU host) には
    # requires_gpu=True のものだけ」 と判断するために使う。 ランタイム判定はしない。
    requires_gpu: bool = False

    @field_validator("host_affinity", mode="before")
    @classmethod
    def _coerce_host_affinity(cls, v: Any) -> list[str]:
        """旧 schema (`[{"hostname": "x"}]`) からの後方互換: string list に正規化。"""
        if v is None:
            return []
        if not isinstance(v, list):
            return []
        out: list[str] = []
        for item in v:
            if isinstance(item, str):
                out.append(item)
            elif isinstance(item, dict):
                host = item.get("hostname") or item.get("host")
                if host:
                    out.append(str(host))
        return out


class WorkloadCreate(WorkloadBase):
    """POST /api/v1/workloads の body."""

    slug: str

    @field_validator("slug")
    @classmethod
    def _slug(cls, v: str) -> str:
        return _validate_slug(v)


class WorkloadUpdate(WorkloadBase):
    """PUT /api/v1/workloads/{slug} の body (slug 変更不可)."""


class Workload(WorkloadBase):
    """GET /api/v1/workloads/{slug} の response。
    DB row + observed_* メトリクスを含む完全版。
    """

    slug: str
    queue_table: str  # 内部で自動付与
    observed_depth: int = 0
    observed_age_secs: int = 0
    observed_rate: float = 0.0
    # worker self-report VRAM peak (install-multi-worker.sh が自動 N 算定に使う)
    observed_vram_mb_peak: int | None = None
    # avg/p95 は周期サンプリングの移動集計 (= 配置設計の実態値)。 peak だけだと
    # 並列度を過小設計してしまう (= 1 度の peak で 全 worker 分を予約してしまう)。
    observed_vram_mb_avg: int | None = None
    observed_vram_mb_p95: int | None = None
    observed_vram_sample_count: int = 0
    observed_vram_updated_at: datetime | None = None
    created_by: str | None = None
    created_at: datetime
    updated_at: datetime
    schema_version: int = 1


def queue_table_for(slug: str) -> str:
    """slug → queue table 名。SQL injection 防止のため slug を厳格にバリデート。

    命名規約: `<slug>_queue` (queue を suffix に置く)。
    例: slug="hash-detect-production" → "hash_detect_production_queue"
    """
    _validate_slug(slug)
    return f"{slug.replace('-', '_')}_queue"
