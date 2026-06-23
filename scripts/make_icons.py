"""Regenerate pipeline/assets/icon-*.png + favicon.ico from icon-source.png.

The source is a 3D-rendered pipe-with-red-valve PNG (white background, no alpha).
We crop to a centered square (valve is roughly centered horizontally) and emit
the size set the index.html / site.css references.
"""
from pathlib import Path

from PIL import Image

ASSETS = Path(__file__).resolve().parent.parent / "assets"
SRC = ASSETS / "icon-source.png"

SIZES = [16, 32, 64, 192, 256, 512]


def square_crop(im: Image.Image) -> Image.Image:
    w, h = im.size
    side = min(w, h)
    left = (w - side) // 2
    top = (h - side) // 2
    return im.crop((left, top, left + side, top + side))


def main() -> None:
    im = Image.open(SRC).convert("RGB")
    sq = square_crop(im)
    for s in SIZES:
        out = ASSETS / f"icon-{s}.png"
        sq.resize((s, s), Image.LANCZOS).save(out, optimize=True)
        print(f"wrote {out.name}  ({s}x{s})")

    # favicon.ico = multi-size embedded (16, 32, 48)
    ico_sizes = [(16, 16), (32, 32), (48, 48)]
    sq.resize((64, 64), Image.LANCZOS).save(ASSETS / "favicon.ico", sizes=ico_sizes)
    print("wrote favicon.ico  (16/32/48)")

    # Also keep a non-suffixed default `icon.png` (used by some templates).
    sq.resize((512, 512), Image.LANCZOS).save(ASSETS / "icon.png", optimize=True)
    print("wrote icon.png  (512x512 default)")


if __name__ == "__main__":
    main()
