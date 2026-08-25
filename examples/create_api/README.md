# 投入 API 利用マニュアル

外部から画像 / 動画を Pipeline に投入し、顔検出と embedding をさせる API です。

投入したものは Paprika クローラー由来と**まったく同じ経路**を通ります。専用の推論エンドポイントではなく、既存パイプラインの入口です。

```
POST /api/v1/create/images
      │
      ▼
 crawl_image ──(30s ごとの自動 sweep)──> 顔検出 ──> crawl_face ──> embedding ──> 検索インデックス
```

エンドポイント: **`http://localhost:8001/api/v1/create`**

## 目次

- [クイックスタート](#クイックスタート)
- [重要な前提](#重要な前提)
- [認証](#認証)
- [エンドポイント](#エンドポイント)
- [進行状況の見方](#進行状況の見方)
- [エラー一覧](#エラー一覧)
- [サンプルコード](#サンプルコード)
- [Python クライアント API](#python-クライアント-api)
- [よくある落とし穴](#よくある落とし穴)

## クイックスタート

**1. キーを発行**（LAN 内から一度だけ。`api_key` はこの応答でしか取れません）

```bash
curl -sS -X POST http://localhost:8001/api/v1/api-keys \
  -H 'Content-Type: application/json' \
  -d '{"user_slug":"myapp","name":"my app"}'
```

**2. 画像を投入**

```bash
export KEY=plk_xxxxx_yyyyy
curl -sS -X POST http://localhost:8001/api/v1/create/images \
  -H "Authorization: Bearer $KEY" \
  -F 'file=@photo.jpg'
```

```json
{"accepted":1,"dedup":0,"failed":0,
 "items":[{"ok":true,"kind":"image","id":125309008,"state":"queued","dedup":false}]}
```

**3. 結果を確認**（`faces[].embedded` が true になれば完了）

```bash
curl -sS -H "Authorization: Bearer $KEY" \
  http://localhost:8001/api/v1/create/images/125309008
```

Python なら 3 行です:

```python
from pipeline_client import PipelineClient
c = PipelineClient("http://localhost:8001", api_key="plk_...")
print(c.upload_image("photo.jpg").items[0].id)
```

> **旧 `/api/v1/ingest/*` は廃止されました。** 現在は `/api/v1/create/*` です。旧パスを叩くと
> 404 ではなく **200 で HTML** が返る（後述の catch-all）ため、JSON パースエラーとして
> 現れます。移行漏れを疑ってください。

## 重要な前提

**① 非同期です。** 応答が返った時点で完了しているのは「キューに入れた」ことだけです。embedding までは、フリートの混み具合次第で数分〜数十分かかります。完了は状態照会エンドポイントで確認します。

**② 冪等です。** 同じ内容（sha256）を再投入すると新しい行は作られず、既存の id が `dedup: true` で返ります。URL 投入なら URL でも重複排除され、既存クローラーが取得済みの同じ URL とも自然に重複排除されます。安心してリトライできます。

**③ 顔が 0 件でも成功です。** 顔が写っていない画像は `faces: []` で終わります。エラーではありません。

**④ `adaface_ready: 2` は失敗ではありません。** 低品質な顔として QC で採用しなかった、という正常な結果です。失敗は `error` フィールドで判断してください。

## 認証

`Authorization: Bearer <api key>` が必須です。キーは `plk_<id>_<secret>` の形式です。

キーの発行は LAN 内の管理 API で行います（**この API は外部公開しないこと**）:

```bash
curl -sS -X POST http://localhost:8001/api/v1/api-keys \
  -H 'Content-Type: application/json' \
  -d '{"user_slug":"acme","name":"acme loader"}'
```

```json
{
  "api_key": "plk_a1b2c3d4e5f6_XXXXXXXXXXXXXXXXXXXXXXXX",
  "id": "a1b2c3d4e5f6",
  "user_slug": "acme",
  "name": "acme loader",
  "create_site": "create-acme"
}
```

`api_key` は**この応答でしか取得できません**（保存されるのは scrypt ハッシュのみ）。紛失したら失効させて再発行してください。

`create_site` は、その呼び出し元が投入した行に付く `site` 名です。あとから「この呼び出し元の分だけ数える / 止める / 消す」が SQL 1 本でできます。

失効:

```bash
curl -sS -X DELETE http://localhost:8001/api/v1/api-keys/a1b2c3d4e5f6
```

## エンドポイント

| メソッド | パス | 用途 |
|---|---|---|
| GET | `/api/v1/create/limits` | 上限と残クォータ |
| POST | `/api/v1/create/images` | 画像ファイル (multipart) |
| POST | `/api/v1/create/images/url` | 画像 URL (JSON・最大 100 件) |
| GET | `/api/v1/create/images/{image_id}` | 画像の進行状況 |
| POST | `/api/v1/create/videos` | 動画ファイル (multipart) |
| POST | `/api/v1/create/videos/url` | 動画 URL (JSON) |
| GET | `/api/v1/create/videos/{video_id}` | 動画の進行状況 |

### GET /api/v1/create/limits

大量投入の前にこれを見てください。

```json
{
  "site": "create-acme",
  "max_image_bytes": 33554432,
  "max_video_bytes": 2147483648,
  "max_urls_per_request": 100,
  "daily_limit": 20000,
  "daily_used": 152,
  "image_formats": ["jpg", "png", "gif", "bmp", "webp"],
  "video_formats": ["mp4", "mov", "webm", "flv"]
}
```

### POST /api/v1/create/images

`multipart/form-data`:

| フィールド | 必須 | 説明 |
|---|---|---|
| `file` | ○ | 画像本体 |
| `url` | | 出所 URL。指定すると URL でも重複排除される |
| `page_url` | | 出所ページ URL（記録用・dedup には使わない） |

```bash
curl -X POST http://localhost:8001/api/v1/create/images \
  -H "Authorization: Bearer $KEY" \
  -F 'file=@photo.jpg' \
  -F 'url=https://example.com/photo.jpg' \
  -F 'page_url=https://example.com/gallery/1'
```

### POST /api/v1/create/images/url

```json
{ "urls": ["https://example.com/a.jpg", "https://example.com/b.jpg"], "page_url": "https://example.com/gallery/1" }
```

単数なら `{"url": "..."}` でも可。`page_url`（出所ページ・記録用）も任意で渡せます。サーバ側が取得するので、**内部アドレス（10.x など）は拒否されます**。

### 投入系の共通応答

すべて「バッチ形」です。1 件でも複数でも同じ形で、**個別の失敗は HTTP エラーにならず item の中に入ります**。HTTP 201 でも `failed` を必ず確認してください。

```json
{
  "accepted": 1,
  "dedup": 1,
  "failed": 1,
  "items": [
    { "ok": true,  "kind": "image", "id": 125309008, "state": "queued",   "dedup": false, "source": null, "error": null, "detail": null },
    { "ok": true,  "kind": "image", "id": 98765432,  "state": "existing", "dedup": true,  "source": "https://example.com/b.jpg" },
    { "ok": false, "kind": "image", "id": null,      "error": "private_address", "detail": "internal.local → 10.0.0.5 (内部アドレスは拒否)" }
  ]
}
```

### 動画

画像と同じ形です。`page_url`（出所ページ）も任意で渡せます。

**数百 MB を超える動画は URL 投入を使ってください。** multipart は control plane のプロセスにバッファを持たせます。

## 進行状況の見方

```bash
curl -sS -H "Authorization: Bearer $KEY" \
  http://localhost:8001/api/v1/create/images/125309008
```

```json
{
  "image_id": 125309008,
  "site": "create-acme",
  "url": "create://create-acme/1d763673a771...",
  "hash_id": 75117097,
  "state": "hashed",
  "downloaded_at": "2026-07-30 09:02:56",
  "ignore_reason": null,
  "page_url": "https://example.com/gallery/1",
  "faces": [
    {
      "face_id": 87795528,
      "adaface_ready": 1,
      "embedded": true,
      "adaface_norm": 21.4,
      "det_score": 0.94,
      "bbox": [120.5, 88.0, 210.0, 190.5],
      "person_id": 553,
      "crop_minio_key": "face/877/955/87795528.jpg"
    }
  ]
}
```

`state` の意味:

| state | 意味 | 次に起こること |
|---|---|---|
| `uploading` | ストレージへの保存中 / 保存失敗 | 通常すぐ `queued` へ |
| `queued` | 保存済み・顔検出待ち | 最大 30 秒で検出キューに入る |
| `hashed` | 顔検出完了 | `faces` が確定。あとは各顔の embedding |
| `excluded` | 取り込み対象外 | 終了（`ignore_reason` を参照） |

顔ごとの `adaface_ready`:

| 値 | 意味 |
|---|---|
| 0 | embedding 未計算（待ち） |
| 1 | 計算済み・インデックス登録済み（`embedded: true`） |
| 2 | QC 落選（低品質のため不採用）**← 正常な結果** |

**完了判定は「`state` が `hashed` かつ `faces` に `adaface_ready == 0` が無い」**です。

### 動画の進行状況

```bash
curl -sS -H "Authorization: Bearer $KEY" \
  http://localhost:8001/api/v1/create/videos/745054
```

```json
{
  "video_id": 745054,
  "site": "create-acme",
  "source_url": "create://create-acme/9f8e7d...",
  "download_status": "completed",
  "state": "done",
  "face_count": 12,
  "representative_image_ids": [125309101, 125309102],
  "processed_at": "2026-07-30 10:14:02",
  "error": null,
  "faces": [ { "face_id": 87795600, "embedded": true, "adaface_norm": 23.1 } ]
}
```

| state | 意味 |
|---|---|
| `processing` | 取得・顔抽出のいずれかが進行中 |
| `done` | 顔抽出完了（`processed_at` が入る） |
| `failed` | 失敗（`error` を参照） |

`download_status` は内部の生値で `pending` → `queued` → `downloading` → `completed` / `failed` と遷移します。判定には `state` を使ってください。

抽出された顔は `crawl_image` にも登録されるため、`representative_image_ids` から画像側の状態照会もできます。

## エラー一覧

| HTTP | 意味 |
|---|---|
| 401 | キーが未指定 / 無効 / 失効 / 期限切れ |
| 404 | 指定した id が存在しない |
| 422 | リクエストが不正（`url` も `urls` も無い、URL が 100 件超 など） |
| 429 | 日次クォータ超過 |

item の `error`:

| error | 意味 | 対処 |
|---|---|---|
| `empty` | 0 バイト | 入力を確認 |
| `too_large` | サイズ上限超過 | `limits` を確認 |
| `unsupported_format` | 先頭バイトから形式を判別できない | **拡張子ではなく中身で判定**しています。実体を確認 |
| `private_address` | 内部アドレスに解決される URL | 外部から到達できる URL を使う |
| `dns_failed` / `http_error` / `fetch_failed` | URL 取得に失敗 | リトライ可 |
| `too_many_redirects` | リダイレクト 3 回超 | 最終 URL を直接指定 |
| `storage_failed` | ストレージ保存に失敗 | **リトライ可**（行は巻き戻されます） |
| `video_unavailable` | 動画用ストレージが未設定 | 管理者に連絡 |
| `internal` | 想定外 | 管理者に連絡 |

## サンプルコード

このディレクトリに入っています。

| ファイル | 内容 |
|---|---|
| [`pipeline_client.py`](pipeline_client.py) | Python クライアント + CLI（標準ライブラリのみ・依存なし） |
| [`curl_examples.sh`](curl_examples.sh) | curl レシピ集 |

### Python

```python
from pipeline_client import PipelineClient

c = PipelineClient("http://localhost:8001", api_key="plk_...")

print(c.limits())

r = c.upload_image("photo.jpg")
r.raise_if_all_failed()
image_id = r.items[0].id
print(image_id, "dedup:", r.items[0].dedup)

# embedding が確定するまで待つ (adaface_ready が 0 でなくなるまで)
for f in c.wait_for_embedding(image_id, timeout=1800):
    print(f["face_id"], f["embedded"], f["adaface_norm"])
```

URL をまとめて:

```python
r = c.create_image_urls([f"https://example.com/{i}.jpg" for i in range(100)])
print(f"新規 {r.accepted} / 重複 {r.dedup} / 失敗 {r.failed}")
for item in r.items:
    if not item.ok:
        print("失敗:", item.source, item.error, item.detail)
```

### CLI

```bash
export PIPELINE_CREATE_URL=http://localhost:8001
export PIPELINE_CREATE_KEY=plk_xxx_yyy

python pipeline_client.py limits
python pipeline_client.py image photo.jpg --wait
python pipeline_client.py image-url https://example.com/a.jpg
python pipeline_client.py status 125309008
```

## Python クライアント API

`pipeline_client.PipelineClient`（標準ライブラリのみ・依存なし）

| メソッド | 戻り値 | 説明 |
|---|---|---|
| `limits()` | `dict` | 上限と残クォータ |
| `upload_image(path, *, source_url=None, page_url=None)` | `CreateResult` | 画像ファイルを投入 |
| `create_image_urls(urls, *, page_url=None)` | `CreateResult` | 画像 URL をまとめて投入（最大 100 件） |
| `image_status(image_id)` | `dict` | 画像の進行状況 |
| `upload_video(path, *, source_url=None, page_url=None)` | `CreateResult` | 動画ファイルを投入 |
| `create_video_urls(urls, *, page_url=None)` | `CreateResult` | 動画 URL を投入 |
| `video_status(video_id)` | `dict` | 動画の進行状況 |
| `wait_for_hash(image_id, *, timeout=900, interval=10)` | `dict` | 顔検出完了まで待つ |
| `wait_for_embedding(image_id, *, timeout=3600, interval=15)` | `list[dict]` | 全顔の embedding 確定まで待って `faces` を返す |

`CreateResult` は `accepted` / `dedup` / `failed` / `items` を持ち、便利メソッドが 2 つあります:

| | 説明 |
|---|---|
| `.ids` | 成功した item の id リスト（dedup 含む） |
| `.raise_if_all_failed()` | 全件失敗なら `CreateError` を投げる（部分失敗では投げない） |

各 `item` は `ok` / `kind` / `id` / `state` / `dedup` / `source` / `error` / `detail`。

例外は `CreateError` のみで、`.status`（HTTP コード）と `.detail` を持ちます。**個別の投入失敗は例外にならず `item.error` に入る**ので、`failed` を必ず確認してください。

コンストラクタ引数:

```python
PipelineClient(base_url, api_key, *, timeout=120.0, retries=4)
```

`retries` は 429 / 5xx / 通信断に対する自動リトライ回数です（429 は `Retry-After` を尊重）。

## よくある落とし穴

**存在しないパスが 200 で HTML を返します。** control plane はフロントエンド用の catch-all を持つため、パスを打ち間違えると 404 ではなく `text/html` の 200 が返ります。JSON パースエラーで気付くことになるので、**応答の `content-type` を確認してください**。`pipeline_client.py` はこのチェックを内蔵しています。

**拡張子は信用されません。** 形式は先頭バイトで判定します。`.jpg` という名前の PNG は PNG として扱われ（問題なく通ります）、画像でないファイルは `unsupported_format` になります。

**クォータはプロセス内カウンタです。** control plane を再起動するとリセットされます。厳密な課金には使えません。

**大量投入時は `limits` の `daily_used` を見てください。** 上限に当たると 429 が返ります。`pipeline_client.py` は 429 を `Retry-After` に従って自動リトライします。

**embedding の待ち時間はフリートの状況次第です。** 大規模な再計算バックログが走っている間は、新規投入分がその後ろに並びます。急ぐ場合は管理者に確認してください。
