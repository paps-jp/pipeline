"""日本語と日本語/英数字の間の 不要スペースを 削除する。

例:
  "アイドル GPU を 埋め尽くす"   → "アイドルGPUを埋め尽くす"
  "同じ ワーカー fleet に N 個" → "同じワーカーfleetにN個"
  "推論 / 学習 / 解析"           → 変わらず (/ は ASCII 記号で 残す)
  "Hacker News"                  → 変わらず (両端 ASCII)
"""
from __future__ import annotations

import re
from pathlib import Path

# CJK 全般 (ひらがな / カタカナ / 漢字 / 半角カナ / 全角句読点)
JP = (
    r"[぀-ゟ"   # ひらがな
    r"゠-ヿ"   # カタカナ
    r"㐀-鿿"   # CJK 統合漢字 (拡張A 含)
    r"･-ﾟ"   # 半角カナ
    r"　-〿"   # 全角句読点 (、。「」 等)
    r"]"
)

RULES = [
    re.compile(rf"({JP}) +(?={JP})"),                # CJK-CJK
    re.compile(rf"({JP}) +(?=[A-Za-z0-9])"),         # CJK-ASCII
    re.compile(rf"([A-Za-z0-9]) +(?={JP})"),         # ASCII-CJK
]


def fix(text: str) -> str:
    # lookahead を使っているので 1 pass で 連続スペースも 拾える
    for r in RULES:
        text = r.sub(r"\1", text)
    return text


def main() -> None:
    root = Path(__file__).resolve().parent.parent
    targets = [root / "index.html", root / "setup.html", root / "manual.html"]
    for p in targets:
        if not p.exists():
            print(f"skip (not found): {p.name}")
            continue
        src = p.read_text(encoding="utf-8")
        dst = fix(src)
        if src == dst:
            print(f"{p.name}: no change")
            continue
        removed = len(src) - len(dst)
        p.write_text(dst, encoding="utf-8")
        print(f"{p.name}: removed {removed} chars  ({len(src)} → {len(dst)})")


if __name__ == "__main__":
    main()
