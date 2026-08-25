#!/usr/bin/env python3
"""投入 API (/api/v1/create) のクライアント + CLI。 標準ライブラリのみで動く。

外部から画像 / 動画を Pipeline へ投入し、 顔検出と embedding の完了を待つ。

ライブラリとして:

    from pipeline_client import PipelineClient

    c = PipelineClient("http://localhost:8001", api_key="plk_...")
    r = c.upload_image("photo.jpg")
    print(r.items[0].id, r.items[0].dedup)
    faces = c.wait_for_embedding(r.items[0].id, timeout=1800)
    for f in faces:
        print(f["face_id"], f["embedded"], f["adaface_norm"])

CLI として:

    export PIPELINE_CREATE_URL=http://localhost:8001
    export PIPELINE_CREATE_KEY=plk_xxx_yyy

    python pipeline_client.py limits
    python pipeline_client.py image photo.jpg --wait
    python pipeline_client.py image-url https://example.com/a.jpg https://example.com/b.jpg
    python pipeline_client.py video clip.mp4
    python pipeline_client.py status 125309008
"""
from __future__ import annotations

import argparse
import json
import mimetypes
import os
import sys
import time
import urllib.error
import urllib.request
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

__all__ = ["PipelineClient", "CreateError", "CreateItem", "CreateResult"]

# 一時的な失敗として retry する HTTP status。 429 は Retry-After を尊重する。
_RETRY_STATUS = (429, 500, 502, 503, 504)


class CreateError(Exception):
    """API 呼び出しが失敗した (HTTP エラー / 応答不正 / 通信断)。"""

    def __init__(self, message: str, *, status: int | None = None,
                 detail: Any = None) -> None:
        super().__init__(message)
        self.status = status
        self.detail = detail


@dataclass
class CreateItem:
    """投入 1 件の結果。 失敗しても例外にはならず item に理由が入る。"""

    ok: bool
    kind: str
    id: int | None = None
    state: str | None = None
    dedup: bool = False
    source: str | None = None
    error: str | None = None
    detail: str | None = None

    @classmethod
    def from_dict(cls, d: dict) -> CreateItem:
        return cls(ok=bool(d.get("ok")), kind=str(d.get("kind") or ""),
                   id=d.get("id"), state=d.get("state"), dedup=bool(d.get("dedup")),
                   source=d.get("source"), error=d.get("error"), detail=d.get("detail"))


@dataclass
class CreateResult:
    accepted: int = 0
    dedup: int = 0
    failed: int = 0
    items: list[CreateItem] = field(default_factory=list)

    @classmethod
    def from_dict(cls, d: dict) -> CreateResult:
        return cls(accepted=int(d.get("accepted") or 0),
                   dedup=int(d.get("dedup") or 0),
                   failed=int(d.get("failed") or 0),
                   items=[CreateItem.from_dict(x) for x in (d.get("items") or [])])

    @property
    def ids(self) -> list[int]:
        """成功した item の id (dedup 含む)。"""
        return [i.id for i in self.items if i.ok and i.id is not None]

    def raise_if_all_failed(self) -> CreateResult:
        if self.items and self.failed == len(self.items):
            first = self.items[0]
            raise CreateError(f"全件失敗: {first.error} ({first.detail})",
                              detail=[i.error for i in self.items])
        return self


def _encode_multipart(fields: dict[str, str], file_field: str,
                      filename: str, content: bytes,
                      content_type: str) -> tuple[bytes, str]:
    """multipart/form-data を組み立てる (requests 不要にするため自前)。"""
    boundary = "----pipeline" + uuid.uuid4().hex
    out = bytearray()
    for k, v in fields.items():
        if v is None:
            continue
        out += f"--{boundary}\r\n".encode()
        out += f'Content-Disposition: form-data; name="{k}"\r\n\r\n'.encode()
        out += f"{v}\r\n".encode()
    out += f"--{boundary}\r\n".encode()
    out += (f'Content-Disposition: form-data; name="{file_field}"; '
            f'filename="{filename}"\r\n').encode()
    out += f"Content-Type: {content_type}\r\n\r\n".encode()
    out += content
    out += b"\r\n"
    out += f"--{boundary}--\r\n".encode()
    return bytes(out), f"multipart/form-data; boundary={boundary}"


class PipelineClient:
    """投入 API のクライアント。

    `base_url` は control plane のルート (例 http://localhost:8001)。
    `api_key` は `POST /api/v1/api-keys` で発行された `plk_...`。
    """

    def __init__(self, base_url: str, api_key: str, *,
                 timeout: float = 120.0, retries: int = 4) -> None:
        if not api_key:
            raise ValueError("api_key は必須です")
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.timeout = timeout
        self.retries = retries

    # ---------------- 低レベル ----------------

    def _request(self, method: str, path: str, *, data: bytes | None = None,
                 content_type: str | None = None) -> Any:
        url = self.base_url + path
        last: Exception | None = None
        for attempt in range(self.retries + 1):
            req = urllib.request.Request(url, data=data, method=method)
            req.add_header("Authorization", f"Bearer {self.api_key}")
            if content_type:
                req.add_header("Content-Type", content_type)
            try:
                with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                    body = resp.read()
                    ctype = resp.headers.get("content-type", "")
                    # 注意: control plane は未知のパスに index.html を 200 で返す
                    # (SPA catch-all)。 JSON 以外が返ったら「そのルートが無い」と判断する。
                    if "application/json" not in ctype:
                        raise CreateError(
                            f"JSON でない応答 (content-type={ctype!r})。 "
                            f"パス {path} が存在しない可能性があります",
                            status=resp.status)
                    return json.loads(body) if body else None
            except urllib.error.HTTPError as e:
                raw = e.read()
                try:
                    detail = json.loads(raw).get("detail")
                except Exception:  # noqa: BLE001
                    detail = raw.decode("utf-8", "replace")[:300]
                if e.code in _RETRY_STATUS and attempt < self.retries:
                    wait = float(e.headers.get("Retry-After") or 0) or 2.0 * (attempt + 1)
                    time.sleep(wait)
                    last = e
                    continue
                msg = {401: "API キーが無効か未指定です",
                       404: "対象が見つかりません",
                       422: "リクエストが不正です",
                       429: "日次クォータを超えました"}.get(e.code, f"HTTP {e.code}")
                raise CreateError(f"{msg}: {detail}", status=e.code, detail=detail) from e
            except urllib.error.URLError as e:
                if attempt < self.retries:
                    time.sleep(2.0 * (attempt + 1))
                    last = e
                    continue
                raise CreateError(f"接続できません: {e}") from e
        raise CreateError(f"retry 上限に達しました: {last}")

    def _post_json(self, path: str, payload: dict) -> Any:
        return self._request("POST", path,
                             data=json.dumps(payload).encode("utf-8"),
                             content_type="application/json")

    # ---------------- 情報 ----------------

    def limits(self) -> dict:
        """上限と残クォータ。 投入前の事前チェックに使う。"""
        return self._request("GET", "/api/v1/create/limits")

    # ---------------- 画像 ----------------

    def upload_image(self, path: str | Path, *, source_url: str | None = None,
                     page_url: str | None = None) -> CreateResult:
        """ローカルの画像ファイルを 1 枚投入する。

        `source_url` を渡すとその URL で dedup される (既存クローラーが取得済みの
        同一 URL と自然に重複排除される)。 `page_url` は出所ページ URL の記録用 (任意)。
        """
        p = Path(path)
        content = p.read_bytes()
        ctype = mimetypes.guess_type(p.name)[0] or "image/jpeg"
        fields = {}
        if source_url:
            fields["url"] = source_url
        if page_url:
            fields["page_url"] = page_url
        body, hdr = _encode_multipart(fields, "file", p.name, content, ctype)
        return CreateResult.from_dict(
            self._request("POST", "/api/v1/create/images", data=body, content_type=hdr))

    def create_image_urls(self, urls: list[str], *, page_url: str | None = None) -> CreateResult:
        """URL から画像を取得して投入する (1 リクエスト最大 100 件)。"""
        payload: dict[str, Any] = {"urls": list(urls)}
        if page_url:
            payload["page_url"] = page_url
        return CreateResult.from_dict(
            self._post_json("/api/v1/create/images/url", payload))

    def image_status(self, image_id: int) -> dict:
        return self._request("GET", f"/api/v1/create/images/{int(image_id)}")

    # ---------------- 動画 ----------------

    def upload_video(self, path: str | Path, *, source_url: str | None = None,
                     page_url: str | None = None) -> CreateResult:
        """ローカルの動画ファイルを 1 本投入する。

        数百 MB を超えるものは control plane にバッファを持たせないよう
        `create_video_urls()` を使うこと。
        """
        p = Path(path)
        content = p.read_bytes()
        ctype = mimetypes.guess_type(p.name)[0] or "video/mp4"
        fields = {}
        if source_url:
            fields["url"] = source_url
        if page_url:
            fields["page_url"] = page_url
        body, hdr = _encode_multipart(fields, "file", p.name, content, ctype)
        return CreateResult.from_dict(
            self._request("POST", "/api/v1/create/videos", data=body, content_type=hdr))

    def create_video_urls(self, urls: list[str], *, page_url: str | None = None) -> CreateResult:
        payload: dict[str, Any] = {"urls": list(urls)}
        if page_url:
            payload["page_url"] = page_url
        return CreateResult.from_dict(
            self._post_json("/api/v1/create/videos/url", payload))

    def video_status(self, video_id: int) -> dict:
        return self._request("GET", f"/api/v1/create/videos/{int(video_id)}")

    # ---------------- 完了待ち ----------------

    def wait_for_hash(self, image_id: int, *, timeout: float = 900.0,
                      interval: float = 10.0) -> dict:
        """顔検出 (hash) が終わるまで待って status を返す。

        `state` が `hashed` になった時点で `faces` が確定する (顔 0 件もありうる)。
        `excluded` は取り込み対象外になった状態なので待たずに返す。
        """
        deadline = time.monotonic() + timeout
        while True:
            st = self.image_status(image_id)
            if st.get("state") in ("hashed", "excluded"):
                return st
            if time.monotonic() >= deadline:
                raise CreateError(
                    f"hash 待ちがタイムアウト (image_id={image_id} state={st.get('state')})",
                    detail=st)
            time.sleep(interval)

    def wait_for_embedding(self, image_id: int, *, timeout: float = 3600.0,
                           interval: float = 15.0) -> list[dict]:
        """全ての顔の embedding が確定するまで待って faces を返す。

        `adaface_ready` は 0=未計算 / 1=計算済 / 2=QC 落選。 **2 は失敗ではなく
        「低品質なので採用しなかった」という正常な結果**なので、 0 が無くなった時点で確定とする。
        顔 0 件の画像は空リストを返す。
        """
        deadline = time.monotonic() + timeout
        while True:
            st = self.image_status(image_id)
            faces = st.get("faces") or []
            if st.get("state") == "excluded":
                return faces
            if st.get("state") == "hashed" and all(
                    int(f.get("adaface_ready") or 0) != 0 for f in faces):
                return faces
            if time.monotonic() >= deadline:
                raise CreateError(
                    f"embedding 待ちがタイムアウト (image_id={image_id})", detail=st)
            time.sleep(interval)


# ---------------------------------------------------------------- CLI


def _client_from_env(args) -> PipelineClient:
    base = args.url or os.environ.get("PIPELINE_CREATE_URL")
    key = args.key or os.environ.get("PIPELINE_CREATE_KEY")
    if not base:
        sys.exit("--url か環境変数 PIPELINE_CREATE_URL が必要です")
    if not key:
        sys.exit("--key か環境変数 PIPELINE_CREATE_KEY が必要です")
    return PipelineClient(base, key)


def _print(obj) -> None:
    print(json.dumps(obj, ensure_ascii=False, indent=2))


def _show_result(r: CreateResult) -> None:
    print(f"accepted={r.accepted} dedup={r.dedup} failed={r.failed}")
    for i in r.items:
        if i.ok:
            tag = "dedup" if i.dedup else "new"
            print(f"  [{tag}] {i.kind} id={i.id} state={i.state}"
                  + (f" src={i.source}" if i.source else ""))
        else:
            print(f"  [失敗] {i.error}: {i.detail}"
                  + (f" src={i.source}" if i.source else ""))


def main() -> int:
    ap = argparse.ArgumentParser(description="Pipeline 投入 API クライアント")
    ap.add_argument("--url", help="既定: $PIPELINE_CREATE_URL")
    ap.add_argument("--key", help="既定: $PIPELINE_CREATE_KEY")
    sub = ap.add_subparsers(dest="cmd", required=True)

    sub.add_parser("limits", help="上限と残クォータを表示")

    p = sub.add_parser("image", help="画像ファイルを投入")
    p.add_argument("paths", nargs="+")
    p.add_argument("--source-url", help="出所 URL (dedup に使われる)")
    p.add_argument("--page-url", help="出所ページ URL (記録用)")
    p.add_argument("--wait", action="store_true", help="embedding 完了まで待つ")
    p.add_argument("--timeout", type=float, default=3600.0)

    p = sub.add_parser("image-url", help="URL から画像を投入")
    p.add_argument("urls", nargs="+")
    p.add_argument("--page-url", help="出所ページ URL (記録用)")

    p = sub.add_parser("video", help="動画ファイルを投入")
    p.add_argument("paths", nargs="+")
    p.add_argument("--source-url")
    p.add_argument("--page-url")

    p = sub.add_parser("video-url", help="URL から動画を投入")
    p.add_argument("urls", nargs="+")
    p.add_argument("--page-url")

    p = sub.add_parser("status", help="画像の進行状況")
    p.add_argument("image_id", type=int)

    p = sub.add_parser("video-status", help="動画の進行状況")
    p.add_argument("video_id", type=int)

    args = ap.parse_args()
    c = _client_from_env(args)

    try:
        if args.cmd == "limits":
            _print(c.limits())
        elif args.cmd == "image":
            for path in args.paths:
                r = c.upload_image(path, source_url=args.source_url,
                                   page_url=args.page_url)
                print(f"== {path}")
                _show_result(r)
                if args.wait and r.ids:
                    faces = c.wait_for_embedding(r.ids[0], timeout=args.timeout)
                    print(f"  顔 {len(faces)} 件:")
                    for f in faces:
                        print(f"    face_id={f['face_id']} embedded={f['embedded']} "
                              f"norm={f.get('adaface_norm')} ready={f['adaface_ready']}")
        elif args.cmd == "image-url":
            _show_result(c.create_image_urls(args.urls, page_url=args.page_url))
        elif args.cmd == "video":
            for path in args.paths:
                r = c.upload_video(path, source_url=args.source_url,
                                   page_url=args.page_url)
                print(f"== {path}")
                _show_result(r)
        elif args.cmd == "video-url":
            _show_result(c.create_video_urls(args.urls, page_url=args.page_url))
        elif args.cmd == "status":
            _print(c.image_status(args.image_id))
        elif args.cmd == "video-status":
            _print(c.video_status(args.video_id))
    except CreateError as e:
        print(f"エラー: {e}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
