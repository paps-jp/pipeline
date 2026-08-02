"""agent_desired の template(max intent) と effective(VRAM 算定結果) の分離。

elastic の AGENT-SCALE は PUT /agents/{host}/desired = set_template を毎 tick 叩く。
ここで effective をテンプレで上書きすると、 planner が算定した VRAM 予算内の
effective が数秒で消え、 物理的に入らない台数が agent へ配られ続ける
(2026-08-01: 16GB のカードに 22GB 分の plan が復活し続けた)。
"""

from __future__ import annotations

import pytest

from pipeline.db.sqlite import SqliteDatabase
from pipeline.repositories.agents import AgentRepository


@pytest.fixture()
def repo():
    db = SqliteDatabase("sqlite:///:memory:")
    db.ensure_schema()
    yield AgentRepository(db)
    db.close()


def _tmpl(**counts) -> dict:
    return {"workloads": {s: {"count": c, "gpu": True, "vram_mb": 2000}
                          for s, c in counts.items()}}


def _counts(d: dict) -> dict:
    return {s: v["count"] for s, v in (d.get("workloads") or {}).items()}


def test_初回は_effective_もテンプレで埋まる(repo):
    repo.set_template("h1", _tmpl(embed=4, hash=4), by="test")
    row = repo.get("h1")
    assert _counts(row["template"]) == {"embed": 4, "hash": 4}
    assert _counts(row["desired"]) == {"embed": 4, "hash": 4}


def test_テンプレ再書き込みでplannerのeffectiveが消えない(repo):
    repo.set_template("h1", _tmpl(embed=4, hash=4), by="operator")
    repo.set_effective("h1", _tmpl(embed=2, hash=3)["workloads"] and
                       {"workloads": _tmpl(embed=2, hash=3)["workloads"]}, by="planner")
    assert _counts(repo.get("h1")["desired"]) == {"embed": 2, "hash": 3}

    # elastic が同じテンプレを書き直す (= AGENT-SCALE の典型)
    repo.set_template("h1", _tmpl(embed=4, hash=4), by="elastic")

    row = repo.get("h1")
    assert _counts(row["template"]) == {"embed": 4, "hash": 4}, "template は更新される"
    assert _counts(row["desired"]) == {"embed": 2, "hash": 3}, \
        "planner の VRAM 算定結果がテンプレで上書きされている"


def test_テンプレが下がったら_effective_も引き下げる(repo):
    repo.set_template("h1", _tmpl(embed=4, hash=4), by="operator")
    repo.set_effective("h1", {"workloads": _tmpl(embed=3, hash=3)["workloads"]}, by="planner")

    repo.set_template("h1", _tmpl(embed=1, hash=4), by="operator")   # embed の上限を下げた

    assert _counts(repo.get("h1")["desired"]) == {"embed": 1, "hash": 3}


def test_テンプレから消えた_slug_は_effective_からも消える(repo):
    repo.set_template("h1", _tmpl(embed=4, hash=4), by="operator")
    repo.set_effective("h1", {"workloads": _tmpl(embed=2, hash=2)["workloads"]}, by="planner")

    repo.set_template("h1", _tmpl(hash=4), by="operator")

    assert _counts(repo.get("h1")["desired"]) == {"hash": 2}


def test_テンプレに増えた_slug_はテンプレ値で入る(repo):
    repo.set_template("h1", _tmpl(hash=4), by="operator")
    repo.set_effective("h1", {"workloads": _tmpl(hash=2)["workloads"]}, by="planner")

    repo.set_template("h1", _tmpl(hash=4, embed=3), by="operator")

    assert _counts(repo.get("h1")["desired"]) == {"hash": 2, "embed": 3}


def _cpu_tmpl(**counts) -> dict:
    return {"workloads": {s: {"count": c, "gpu": False, "pin": True}
                          for s, c in counts.items()}}


def test_CPU_slug_は_effective_が_0_でもテンプレで復活する(repo):
    """planner は GPU slug しか effective を戻さないので CPU slug を clamp すると
    片道切符になる (2026-08-02: nas-c2 の image-pull/job-submit が恒久停止)。"""
    repo.set_template("nas-c2", _cpu_tmpl(image_pull=1, job_submit=1), by="operator")
    repo.set_effective("nas-c2", _cpu_tmpl(image_pull=0, job_submit=0)["workloads"] and
                       {"workloads": _cpu_tmpl(image_pull=0, job_submit=0)["workloads"]},
                       by="planner")
    assert _counts(repo.get("nas-c2")["desired"]) == {"image_pull": 0, "job_submit": 0}

    repo.set_template("nas-c2", _cpu_tmpl(image_pull=1, job_submit=1), by="operator")

    assert _counts(repo.get("nas-c2")["desired"]) == {"image_pull": 1, "job_submit": 1}, \
        "CPU slug は template が権威 (誰も戻せない 0 固着を作らない)"


def test_affinity_矯正由来の_0_はテンプレで復活しない(repo):
    repo.set_template("ai-gpu5", _cpu_tmpl(image_pull=1), by="operator")
    repo.set_effective("ai-gpu5",
                       {"workloads": {"image_pull": {"count": 0, "gpu": False, "pin": True,
                                                     "affinity_blocked": True}}},
                       by="supervisor")

    repo.set_template("ai-gpu5", _cpu_tmpl(image_pull=1), by="elastic")

    row = repo.get("ai-gpu5")
    assert _counts(row["desired"]) == {"image_pull": 0}, "host_affinity は絶対制約"
    assert row["desired"]["workloads"]["image_pull"]["affinity_blocked"] is True, \
        "印が消えると次の set_template で復活してしまう"


def test_affinity_違反が解消したら_CPU_slug_は復活する(repo):
    repo.set_template("ai-gpu5", _cpu_tmpl(image_pull=1), by="operator")
    repo.set_effective("ai-gpu5",
                       {"workloads": {"image_pull": {"count": 0, "gpu": False, "pin": True,
                                                     "affinity_blocked": True}}},
                       by="supervisor")
    # planner が違反解消を検知して印を外す
    repo.set_effective("ai-gpu5",
                       {"workloads": {"image_pull": {"count": 0, "gpu": False, "pin": True}}},
                       by="supervisor")

    repo.set_template("ai-gpu5", _cpu_tmpl(image_pull=1), by="elastic")

    assert _counts(repo.get("ai-gpu5")["desired"]) == {"image_pull": 1}


def test_GPU_slug_は従来どおり_clamp_される(repo):
    """CPU 例外が GPU の VRAM 算定結果まで壊していないことの明示的な確認。"""
    mixed_t = {"workloads": {"embed": {"count": 4, "gpu": True, "vram_mb": 2000},
                             "cleanup": {"count": 2, "gpu": False}}}
    repo.set_template("ai-gpu9", mixed_t, by="operator")
    repo.set_effective("ai-gpu9",
                       {"workloads": {"embed": {"count": 1, "gpu": True, "vram_mb": 2000},
                                      "cleanup": {"count": 0, "gpu": False}}},
                       by="planner")

    repo.set_template("ai-gpu9", mixed_t, by="elastic")

    assert _counts(repo.get("ai-gpu9")["desired"]) == {"embed": 1, "cleanup": 2}


def test_effective_が壊れていてもテンプレで復旧する(repo):
    repo.set_template("h1", _tmpl(hash=4), by="operator")
    with repo.db.transaction() as conn:
        conn.execute("UPDATE agent_desired SET desired_json='{壊れた' WHERE host='h1'")

    repo.set_template("h1", _tmpl(hash=4), by="elastic")

    assert _counts(repo.get("h1")["desired"]) == {"hash": 4}
