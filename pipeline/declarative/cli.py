"""`pipeline config export|plan|apply` — control plane の /api/v1/config を呼ぶ薄いクライアント。"""

from __future__ import annotations

import argparse
import os
import sys
from typing import Any

import httpx

_MARK = {"create": "+", "update": "~", "delete": "-", "extra": "?", "skipped": "!", "unchanged": "="}


def add_subparser(sub: Any) -> None:
    p = sub.add_parser("config", help="宣言的構成 (pipeline.config.yaml) の export / plan / apply")
    p.add_argument("--url", default=os.environ.get("PIPELINE_CONTROL_URL", "http://127.0.0.1:8001"),
                   help="control plane の URL (env: PIPELINE_CONTROL_URL)")
    csub = p.add_subparsers(dest="config_cmd", required=True)

    pe = csub.add_parser("export", help="今の構成を YAML で出力する")
    pe.add_argument("-o", "--output", default="-", help="出力先ファイル (default: 標準出力)")

    for name, help_ in (("plan", "差分を表示する (DB は変えない)"),
                        ("apply", "差分を反映する")):
        pp = csub.add_parser(name, help=help_)
        pp.add_argument("-f", "--file", required=True, help="pipeline.config.yaml")
        pp.add_argument("--prune", action="store_true",
                        help="ファイルに無い workload / 配信先を消す (有効な workload は消さない)")
        if name == "apply":
            pp.add_argument("-y", "--yes", action="store_true", help="確認を省く")


def _fmt(v: Any) -> str:
    s = repr(v)
    return s if len(s) <= 90 else s[:87] + "..."


def print_plan(plan: dict[str, Any], show_unchanged: bool = False) -> None:
    for c in plan["changes"]:
        if c["op"] == "unchanged" and not show_unchanged:
            continue
        line = f"{_MARK.get(c['op'], '?')} {c['section']:<13} {c['key']}  [{c['op']}]"
        if c.get("applied"):
            line += "  ✓ applied"
        if c.get("error"):
            line += f"  ✗ {c['error']}"
        print(line)
        for k, d in c["diff"].items():
            if c["op"] == "create":
                print(f"      {k}: {_fmt(d['after'])}")
            else:
                print(f"      {k}: {_fmt(d['before'])} → {_fmt(d['after'])}")
        if c.get("note"):
            print(f"      ({c['note']})")
    s = plan["summary"]
    print("\n" + ", ".join(f"{k} {v}" for k, v in s.items() if v))


def _errors(resp: httpx.Response) -> int:
    try:
        detail = resp.json().get("detail")
    except Exception:  # noqa: BLE001
        detail = resp.text
    if isinstance(detail, dict) and "errors" in detail:
        print("エラー:", file=sys.stderr)
        for m in detail["errors"]:
            print(f"  - {m}", file=sys.stderr)
    elif isinstance(detail, dict) and "changes" in detail:
        print("apply が途中で失敗しました (適用済み/未適用は下記):", file=sys.stderr)
        print_plan(detail)
    else:
        print(f"HTTP {resp.status_code}: {detail}", file=sys.stderr)
    return 1


def run(args: argparse.Namespace) -> int:
    base = args.url.rstrip("/")
    try:
        with httpx.Client(timeout=120) as cli:
            if args.config_cmd == "export":
                r = cli.get(f"{base}/api/v1/config/export")
                if r.status_code != 200:
                    return _errors(r)
                if args.output == "-":
                    sys.stdout.write(r.text)
                else:
                    with open(args.output, "w", encoding="utf-8", newline="\n") as f:
                        f.write(r.text)
                    print(f"書き出しました: {args.output}", file=sys.stderr)
                return 0

            with open(args.file, encoding="utf-8") as f:
                body = {"yaml": f.read(), "prune": args.prune}
            r = cli.post(f"{base}/api/v1/config/plan", json=body)
            if r.status_code != 200:
                return _errors(r)
            plan = r.json()
            print_plan(plan)
            if args.config_cmd == "plan":
                return 0
            if not plan["has_changes"]:
                print("差分はありません。")
                return 0
            if not args.yes:
                if input("\n上の変更を反映しますか? [y/N] ").strip().lower() != "y":
                    print("中止しました。")
                    return 1
            r = cli.post(f"{base}/api/v1/config/apply", json=body)
            if r.status_code != 200:
                return _errors(r)
            print("\n--- 反映結果 ---")
            print_plan(r.json())
            return 0
    except httpx.HTTPError as e:
        print(f"control plane に接続できません ({base}): {e}", file=sys.stderr)
        return 1
