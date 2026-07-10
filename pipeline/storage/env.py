"""env ファイル読み込み (= 全プラグインに重複していた `_load_env_file` の集約)。"""

from __future__ import annotations

from pathlib import Path


def load_env_file(path: str | None) -> dict[str, str]:
    """KEY=VALUE 形式の .env を dict に。存在しない/None は空 dict。

    各プラグインが独自に持っていた同名関数の単一実装 (video_health / bridge_main /
    cleanup_main / submit_main / links_main / image_main で重複していた)。
    """
    out: dict[str, str] = {}
    if not path:
        return out
    try:
        text = Path(path).read_text(encoding="utf-8")
    except (FileNotFoundError, OSError):
        return out
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        out[k.strip()] = v.strip().strip('"').strip("'")
    return out
