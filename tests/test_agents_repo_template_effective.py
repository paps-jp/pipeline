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


def test_effective_が壊れていてもテンプレで復旧する(repo):
    repo.set_template("h1", _tmpl(hash=4), by="operator")
    with repo.db.transaction() as conn:
        conn.execute("UPDATE agent_desired SET desired_json='{壊れた' WHERE host='h1'")

    repo.set_template("h1", _tmpl(hash=4), by="elastic")

    assert _counts(repo.get("h1")["desired"]) == {"hash": 4}
