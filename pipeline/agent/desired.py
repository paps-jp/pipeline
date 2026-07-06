"""P1: ローカルファイル (JSON) から desired state を読む。 P2 で supervisor の sync packet に置換予定。

desired.json 例:
{
  "control_url": "http://control-plane.example:8001",
  "workloads": {
    "image-hash-extract": {"count": 2, "gpu": true, "vram_mb": 1600},
    "video-face-extract": {"count": 3, "gpu": true, "vram_mb": 1300},
    "embed-write":        {"count": 1, "gpu": false}
  }
}
count だけの略記も可: "embed-write": 1
"""
from __future__ import annotations

import json
from dataclasses import dataclass


@dataclass
class WorkloadDesired:
    slug: str
    count: int
    gpu: bool = False
    vram_mb: int = 1500  # GPU workload の 1 子あたり VRAM 見積り (spawn ゲート用)


@dataclass
class Desired:
    control_url: str
    workloads: list[WorkloadDesired]


def load_desired(path: str) -> Desired:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    control_url = str(raw["control_url"]).rstrip("/")
    wls: list[WorkloadDesired] = []
    for slug, spec in (raw.get("workloads") or {}).items():
        if isinstance(spec, int):
            spec = {"count": spec}
        wls.append(
            WorkloadDesired(
                slug=str(slug),
                count=int(spec.get("count", 0)),
                gpu=bool(spec.get("gpu", False)),
                vram_mb=int(spec.get("vram_mb", 1500)),
            )
        )
    return Desired(control_url=control_url, workloads=wls)
