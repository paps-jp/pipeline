"""外部 URL の取得 (= SSRF ガード + サイズ上限 + 形式判定)。

外部から任意の URL を渡せる API は、 そのまま作ると **内部ネットワークの読み取り代行**
になる。 このフリートは 10.10.50.x に control plane / MinIO / MariaDB / GPU worker が
無認証で並んでいるので、 プライベート宛の取得は必ず拒否する。

残存リスク (既知・許容): DNS 検証後に応答が変わる rebinding は防げていない。 完全に
塞ぐには解決済み IP に直結して Host/SNI を差し替える必要があり、 TLS 検証と両立させる
コストが大きい。 LAN 内利用が前提の間は「検証 + 各リダイレクト hop の再検証」で止める。
"""

from __future__ import annotations

import ipaddress
import socket
from dataclasses import dataclass
from urllib.parse import urlsplit

import httpx

MAX_REDIRECTS = 3
CONNECT_TIMEOUT_S = 5.0
READ_TIMEOUT_S = 30.0

# 画像 / 動画のマジックバイト → 拡張子。 imghdr は 3.13 で削除されたので自前で持つ。
_IMAGE_MAGIC: tuple[tuple[bytes, str], ...] = (
    (b"\xff\xd8\xff", "jpg"),
    (b"\x89PNG\r\n\x1a\n", "png"),
    (b"GIF87a", "gif"),
    (b"GIF89a", "gif"),
    (b"BM", "bmp"),
)
_VIDEO_MAGIC: tuple[tuple[bytes, str], ...] = (
    (b"\x1a\x45\xdf\xa3", "webm"),      # Matroska / WebM
    (b"FLV\x01", "flv"),
)


class FetchError(Exception):
    """取得失敗。 `reason` は API がそのまま返せる短い機械可読文字列。"""

    def __init__(self, reason: str, detail: str = "") -> None:
        super().__init__(detail or reason)
        self.reason = reason
        self.detail = detail or reason


@dataclass(frozen=True)
class Fetched:
    data: bytes
    final_url: str
    content_type: str


def sniff_ext(data: bytes, *, kind: str) -> str | None:
    """先頭バイトから拡張子を推定。 判別できなければ None。"""
    if kind == "image":
        for magic, ext in _IMAGE_MAGIC:
            if data.startswith(magic):
                return ext
        # WebP: "RIFF" + 4byte size + "WEBP"
        if data[:4] == b"RIFF" and data[8:12] == b"WEBP":
            return "webp"
        return None
    for magic, ext in _VIDEO_MAGIC:
        if data.startswith(magic):
            return ext
    # ISO-BMFF (mp4 / mov / m4v): 4byte size + "ftyp"
    if data[4:8] == b"ftyp":
        brand = data[8:12]
        return "mov" if brand in (b"qt  ",) else "mp4"
    return None


def assert_public_url(url: str) -> None:
    """scheme / ホスト解決結果を検証。 プライベート宛や解決不能は FetchError。"""
    parts = urlsplit(url)
    if parts.scheme not in ("http", "https"):
        raise FetchError("bad_scheme", f"scheme={parts.scheme!r} (http/https のみ)")
    host = parts.hostname
    if not host:
        raise FetchError("bad_url", "ホスト名が空")
    try:
        infos = socket.getaddrinfo(host, parts.port or (443 if parts.scheme == "https" else 80),
                                   proto=socket.IPPROTO_TCP)
    except socket.gaierror as e:
        raise FetchError("dns_failed", f"{host}: {e}") from e
    if not infos:
        raise FetchError("dns_failed", f"{host}: 解決結果が空")
    for info in infos:
        ip = ipaddress.ip_address(info[4][0])
        # 1 つでもプライベートに解決するホストは拒否 (= round-robin の片方だけ内部を突く
        # ケースを塞ぐ)。
        if (ip.is_private or ip.is_loopback or ip.is_link_local or ip.is_reserved
                or ip.is_multicast or ip.is_unspecified):
            raise FetchError("private_address", f"{host} → {ip} (内部アドレスは拒否)")


def fetch(url: str, *, max_bytes: int) -> Fetched:
    """URL を取得。 リダイレクトは hop 毎に再検証しつつ自分で辿る。

    `max_bytes` 超過は途中で打ち切って FetchError (= 全部読んでから捨てない)。
    """
    current = url
    timeout = httpx.Timeout(READ_TIMEOUT_S, connect=CONNECT_TIMEOUT_S)
    with httpx.Client(follow_redirects=False, timeout=timeout) as client:
        for _ in range(MAX_REDIRECTS + 1):
            assert_public_url(current)
            try:
                with client.stream("GET", current) as resp:
                    if resp.is_redirect:
                        loc = resp.headers.get("location")
                        if not loc:
                            raise FetchError("bad_redirect", "Location ヘッダ無し")
                        current = str(resp.url.join(loc))
                        continue
                    if resp.status_code >= 400:
                        raise FetchError("http_error", f"HTTP {resp.status_code}")
                    declared = resp.headers.get("content-length")
                    if declared and int(declared) > max_bytes:
                        raise FetchError("too_large",
                                         f"Content-Length {declared} > {max_bytes}")
                    chunks: list[bytes] = []
                    total = 0
                    for chunk in resp.iter_bytes(65536):
                        total += len(chunk)
                        if total > max_bytes:
                            raise FetchError("too_large", f"{max_bytes} バイトを超過")
                        chunks.append(chunk)
                    return Fetched(b"".join(chunks), str(resp.url),
                                   resp.headers.get("content-type", ""))
            except httpx.HTTPError as e:
                raise FetchError("fetch_failed", f"{type(e).__name__}: {e}") from e
    raise FetchError("too_many_redirects", f"リダイレクト {MAX_REDIRECTS} 回を超過")
