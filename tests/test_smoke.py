"""Pipeline scaffold の smoke test。

依存:
    pip install -e ".[dev]"
"""

from __future__ import annotations

from pipeline import __version__
from pipeline.cli import build_parser, main


def test_version_string() -> None:
    assert isinstance(__version__, str) and __version__


def test_parser_builds() -> None:
    p = build_parser()
    args = p.parse_args(["run", "--dev"])
    assert args.cmd == "run"
    assert args.dev is True
