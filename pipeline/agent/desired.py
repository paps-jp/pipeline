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


def _parse_workloads(raw_workloads: dict) -> list[WorkloadDesired]:
    wls: list[WorkloadDesired] = []
    for slug, spec in (raw_workloads or {}).items():
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
    return wls


def load_desired(path: str) -> Desired:
    with open(path, encoding="utf-8") as f:
        raw = json.load(f)
    control_url = str(raw["control_url"]).rstrip("/")
    return Desired(control_url=control_url, workloads=_parse_workloads(raw.get("workloads")))


def fetch_desired_via_sync(
    control_url: str,
    host: str,
    *,
    vram_total_mb: int | None,
    vram_free_mb: int | None,
    children: list,
    timeout: float = 10.0,
) -> Desired | None:
    """P2: POST /agents/{host}/sync で状態を報告し desired を受領。

    desired があれば Desired を返す (control_url は引数のものを使う)。 desired 未設定/
    到達不可なら None (呼び出し側はローカルファイルに fallback する)。
    """
    import urllib.request

    base = control_url.rstrip("/")
    body = json.dumps({
        "vram_total_mb": vram_total_mb,
        "vram_free_mb": vram_free_mb,
        "children": children,
    }).encode()
    req = urllib.request.Request(
        f"{base}/api/v1/agents/{host}/sync", data=body, method="POST",
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(req, timeout=timeout) as r:
        resp = json.load(r)
    d = resp.get("desired")
    if not d or not d.get("workloads"):
        return None
    return Desired(control_url=base, workloads=_parse_workloads(d.get("workloads")))
