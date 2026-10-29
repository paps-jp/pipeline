"""image-pull が RSS 上限を超えたら tick 末に自分へ SIGTERM を送って退役する。

nas-c2 の image-pull は約 1 日で anon RSS 48.8GB に達し、 CT の memcg OOM で
pipeline-agent.service の子 10 本ごと落ちていた (2026-10-16〜29 に 5 回)。
plugins は別リポジトリでテスト基盤が無いため、 ここからパス指定で読み込む。
"""

from __future__ import annotations

import importlib.util
import signal
import sys
import types
from pathlib import Path

import pytest

_IM = Path(__file__).resolve().parents[1] / "plugins" / "paprika_image_pull" / "image_main.py"
pytestmark = pytest.mark.skipif(not _IM.exists(), reason="plugins リポジトリ未チェックアウト")


@pytest.fixture(scope="module")
def im():
    if "PIL" not in sys.modules:
        pil = types.ModuleType("PIL")
        img = types.ModuleType("PIL.Image")
        pil.Image = img
        sys.modules["PIL"] = pil
        sys.modules["PIL.Image"] = img
    spec = importlib.util.spec_from_file_location("image_main_rss_test", _IM)
    mod = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(mod)
    return mod


@pytest.fixture
def kills(im, monkeypatch):
    sent: list = []
    monkeypatch.setattr(im.os, "kill", lambda pid, sig: sent.append(sig))
    return sent


def test_over_limit_sends_sigterm_once(im, kills, monkeypatch):
    monkeypatch.setattr(im, "_rss_mb", lambda: 20000)
    state, out = {"rss_recycle_mb": 16384}, {}
    im._maybe_recycle_on_rss(state, out)
    im._maybe_recycle_on_rss(state, {})
    assert kills == [signal.SIGTERM]
    assert out["rss_recycle"] is True and out["rss_mb"] == 20000


def test_under_limit_only_reports(im, kills, monkeypatch):
    monkeypatch.setattr(im, "_rss_mb", lambda: 8000)
    out: dict = {}
    im._maybe_recycle_on_rss({"rss_recycle_mb": 16384}, out)
    assert kills == [] and out == {"rss_mb": 8000}


def test_zero_disables(im, kills, monkeypatch):
    monkeypatch.setattr(im, "_rss_mb", lambda: 60000)
    im._maybe_recycle_on_rss({"rss_recycle_mb": 0}, {})
    assert kills == []
