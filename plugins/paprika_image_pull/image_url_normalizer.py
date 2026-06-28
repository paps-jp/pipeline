import math
import re
from collections import Counter
from urllib.parse import parse_qsl, urlencode, unquote, urlparse, urlunparse

# ----------------------------------------
# クエリキーの分類
# ----------------------------------------

# 画像の同一性に関わる可能性があるキー → 保持
KEEP_KEYS = {
    "id", "image_id", "img_id",
    "file", "filename",
    "path", "src", "url", "name",
    "key",  # 画像識別子として使うサービスがあるため保持側に分類
}

# キャッシュバスター・変換パラメータ・トラッキング等 → 削除
REMOVE_KEYS = {
    "v", "ver", "version", "r", "_",
    "t", "ts", "timestamp",
    "cache", "cachebuster", "cb",
    "rand", "random", "rnd",
    "w", "width", "h", "height",
    "q", "quality",
    "format", "fm",
    "fit", "crop", "resize",
    "auto", "dpr",
    "utm_source", "utm_medium", "utm_campaign", "utm_term", "utm_content",
    "fbclid", "gclid", "yclid",
    "token", "auth", "signature", "sig", "expires", "hash",
    # "key" は KEEP_KEYS に移動
}

# ----------------------------------------
# トークン判定用の正規表現
# ----------------------------------------

# UUIDは画像IDとして正当な値なので保持（False を返す）
UUID_RE = re.compile(
    r"^[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$",
    re.I,
)
# 16進数文字列（MD5=32桁, SHA1=40桁, SHA256=64桁など）
HEX_RE = re.compile(r"^[0-9a-f]+$", re.I)
# Base64 / Base64URL 的な文字構成
BASE64ISH_RE = re.compile(r"^[A-Za-z0-9._~+/=-]+$")
# JWT形式（ヘッダー.ペイロード.署名）
JWT_RE = re.compile(r"^[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)?$")


def shannon_entropy(value: str) -> float:
    """Shannon エントロピーを計算する（値がランダムな文字列ほど高くなる）"""
    counts = Counter(value)
    length = len(value)
    return -sum(
        (count / length) * math.log2(count / length)
        for count in counts.values()
    )


def looks_like_token(value: str) -> bool:
    """
    クエリパラメータの値がランダムトークン・ハッシュ・JWTのようなものかを判定する。

    True  → キャッシュバスターや署名など画像の同一性に無関係 → 除去
    False → 画像IDやファイル名など有意な値               → 保持

    NOTE: KEEP_KEYS に含まれるキーはこの関数を経由しないため、
    UUID が False を返すのは「キー不明のUUIDを誤保持しない」ためではなく
    「画像IDとして意味のある値を誤削除しない」ための安全側の判断。
    """
    value = value.strip()
    if not value:
        return False

    # URLエンコードを正規化（%2F → / など）
    decoded = unquote(value)

    # UUIDは画像IDとして使われるため保持
    if UUID_RE.match(decoded):
        return False

    # JWTは明らかにトークン
    if JWT_RE.match(decoded) and len(decoded) >= 24:
        return True

    # 以降はBase64/Hex的な文字構成のみを対象にする
    if not BASE64ISH_RE.match(decoded):
        return False

    # 数字のみはページ番号やIDとして有意
    if decoded.isdigit():
        return False

    # 16進数ハッシュ（32桁以上をトークンと判断）
    if HEX_RE.match(decoded):
        return len(decoded) >= 32

    # 長さ × エントロピーで段階判定
    # ファイル名のような低エントロピー文字列を除外するため閾値を設けている
    length = len(decoded)
    entropy = shannon_entropy(decoded)

    if length >= 48 and entropy >= 3.8:
        return True
    if length >= 32 and entropy >= 4.0:
        return True
    if length >= 20 and entropy >= 4.3:
        return True

    return False


def normalize_image_url(url: str) -> str:
    """
    画像URLを正規化して重複登録を防ぐ。

    - スキーム・ホストを小文字化
    - パス末尾のスラッシュを除去（ルート "/" は除く）
    - クエリパラメータから不要なものを除去・ソート
    - フラグメントを除去

    NOTE: Cloudinary 等パス内に変換パラメータを持つCDNには非対応。
    """
    parsed = urlparse(url)

    # パス末尾のスラッシュを除去（画像URLを前提とした正規化）
    path = parsed.path
    if path != "/":
        path = path.rstrip("/")

    kept_params = []
    for key, value in parse_qsl(parsed.query, keep_blank_values=True):
        k = key.lower()
        if k in KEEP_KEYS:
            kept_params.append((key, value))
            continue
        if k in REMOVE_KEYS:
            continue
        if looks_like_token(value):
            continue
        # 不明なキーはデフォルト削除（安全側に倒す）

    # キー・値の両方でソートして順序違いを同一視
    normalized_query = urlencode(
        sorted(kept_params),
        doseq=True,
    )

    return urlunparse((
        parsed.scheme.lower(),
        parsed.netloc.lower(),
        path,
        "",
        normalized_query,
        "",  # fragment 削除
    ))


