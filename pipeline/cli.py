"""Pipeline CLI entry。`pipeline run --dev` で FastAPI + SQLite を一発起動。"""

from __future__ import annotations

import argparse
import logging
import secrets
import sys

from pipeline import __version__
from pipeline.config import Settings


def build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="pipeline",
        description="GUI-first batch fleet for non-programmers.",
    )
    p.add_argument("--version", action="version", version=f"pipeline {__version__}")

    sub = p.add_subparsers(dest="cmd", required=True)

    p_run = sub.add_parser(
        "run",
        help="Pipeline サーバを起動 (control + worker + Web UI)",
    )
    p_run.add_argument(
        "--dev",
        action="store_true",
        help="開発モード: SQLite (./pipeline.db) + 単一プロセス + worker 兼用",
    )
    p_run.add_argument(
        "--db-url",
        default=None,
        help="DB 接続 URL (例: sqlite:///./pipeline.db)",
    )
    p_run.add_argument("--host", default="0.0.0.0")
    p_run.add_argument("--port", type=int, default=8000)
    p_run.add_argument("--log-level", default="INFO")

    p_worker = sub.add_parser(
        "worker",
        help="Worker daemon (control plane に HTTP 接続して task を実行する別プロセス)",
    )
    p_worker.add_argument(
        "--control-url", required=True,
        help="Control plane の URL (例: http://10.10.50.7:8001)",
    )
    p_worker.add_argument("--hostname", default=None, help="この worker のホスト名 (default: socket.gethostname())")
    p_worker.add_argument("--worker-id", default=None, help="worker_id を強制指定 (再起動時に同 ID 利用)")
    p_worker.add_argument("--token", default=None, help="認証 token (将来用)")
    p_worker.add_argument("--idle-sleep", type=float, default=1.0)
    p_worker.add_argument("--log-level", default="INFO")
    # 後方互換: Phase C で plugin cache 経路を削除したため、 これらは no-op (= argparse error 防止)
    p_worker.add_argument("--cache-dir", default=None, help=argparse.SUPPRESS)
    p_worker.add_argument("--pip-index-url", default=None, help=argparse.SUPPRESS)
    p_worker.add_argument("--skip-pip-install", action="store_true", help=argparse.SUPPRESS)

    return p


def cmd_run(args: argparse.Namespace) -> int:
    """`pipeline run` の実装。"""
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    log = logging.getLogger("pipeline.cli")

    db_url = args.db_url or ("sqlite:///./pipeline.db" if args.dev else None)
    if not db_url:
        log.error("DB URL が指定されてません。--db-url か --dev を指定してください。")
        return 2

    # dev mode の admin password を起動時に表示 (MinIO 風)
    settings_kwargs: dict = {
        "db_url": db_url,
        "bind_host": args.host,
        "bind_port": args.port,
        "mode": "dev" if args.dev else "control",
    }
    if args.dev:
        admin_pw = secrets.token_urlsafe(16)
        settings_kwargs["admin_password"] = admin_pw
        print()
        print("=" * 60)
        print(f"Pipeline {__version__} (dev mode)")
        print(f"  Web UI : http://{args.host}:{args.port}")
        print(f"  DB     : {db_url}")
        print(f"  Admin password (this run only): {admin_pw}")
        print("=" * 60)
        print()

    settings = Settings.from_env(**settings_kwargs)

    # 遅延 import で FastAPI / uvicorn が必要な時だけロード
    import uvicorn

    from pipeline.control.server import create_app

    app = create_app(settings)
    uvicorn.run(
        app,
        host=settings.bind_host,
        port=settings.bind_port,
        log_level=args.log_level.lower(),
    )
    return 0


def cmd_worker(args: argparse.Namespace) -> int:
    import asyncio

    from pipeline.worker.service import run_worker_cli

    return asyncio.run(run_worker_cli(args))


def main(argv: list[str] | None = None) -> int:
    parser = build_parser()
    args = parser.parse_args(argv)
    if args.cmd == "run":
        return cmd_run(args)
    if args.cmd == "worker":
        return cmd_worker(args)
    parser.error(f"unknown command: {args.cmd}")
    return 2


if __name__ == "__main__":
    sys.exit(main())
