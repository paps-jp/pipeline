#!/usr/bin/env bash
# 投入 API (/api/v1/create) の curl レシピ集。
# 実行するのではなく、 必要な行をコピーして使うことを想定している。
set -euo pipefail

BASE="${PIPELINE_CREATE_URL:-http://10.10.50.7:8001}"
KEY="${PIPELINE_CREATE_KEY:?PIPELINE_CREATE_KEY を設定してください}"
AUTH=(-H "Authorization: Bearer ${KEY}")

# ---------------------------------------------------------------- 管理面
# キー発行は LAN 内の管理 API (無認証)。 外部へ公開しないこと。
# 応答の api_key は **この時だけ** 取得できる。
new_key() {
  curl -sS -X POST "${BASE}/api/v1/api-keys" \
    -H 'Content-Type: application/json' \
    -d '{"user_slug":"acme","name":"acme loader"}'
}

list_keys() { curl -sS "${BASE}/api/v1/api-keys"; }

revoke_key() { curl -sS -X DELETE "${BASE}/api/v1/api-keys/$1" -o /dev/null -w '%{http_code}\n'; }

# ---------------------------------------------------------------- 事前確認
# 上限と残クォータ。 大量投入の前にこれを見る。
limits() { curl -sS "${AUTH[@]}" "${BASE}/api/v1/create/limits"; }

# ---------------------------------------------------------------- 画像
# ファイルを 1 枚。 -F file=@... が本体、 url= は任意 (dedup 用の出所 URL)。
put_image() {
  curl -sS -X POST "${BASE}/api/v1/create/images" "${AUTH[@]}" \
    -F "file=@$1" \
    ${2:+-F "url=$2"}
}

# URL から (単数)
put_image_url() {
  curl -sS -X POST "${BASE}/api/v1/create/images/url" "${AUTH[@]}" \
    -H 'Content-Type: application/json' \
    -d "$(printf '{"url":"%s"}' "$1")"
}

# URL から (複数・最大 100 件)
put_image_urls() {
  # 使い方: put_image_urls https://a/1.jpg https://a/2.jpg
  local json
  json=$(printf '%s\n' "$@" | python3 -c 'import sys,json; print(json.dumps({"urls":[l.strip() for l in sys.stdin if l.strip()]}))')
  curl -sS -X POST "${BASE}/api/v1/create/images/url" "${AUTH[@]}" \
    -H 'Content-Type: application/json' -d "${json}"
}

# 進行状況。 faces[].embedded が true になれば embedding 完了。
image_status() { curl -sS "${AUTH[@]}" "${BASE}/api/v1/create/images/$1"; }

# embedding 完了までポーリングする例
wait_image() {
  local id="$1" i
  for i in $(seq 1 120); do
    local st
    st=$(image_status "$id")
    local state
    state=$(printf '%s' "$st" | python3 -c 'import sys,json; print(json.load(sys.stdin)["state"])')
    echo "  [$i] state=${state}"
    if [ "${state}" = "hashed" ] || [ "${state}" = "excluded" ]; then
      printf '%s' "$st" | python3 -m json.tool
      return 0
    fi
    sleep 10
  done
  echo "タイムアウト" >&2
  return 1
}

# ---------------------------------------------------------------- 動画
put_video() {
  curl -sS -X POST "${BASE}/api/v1/create/videos" "${AUTH[@]}" \
    -F "file=@$1" \
    ${2:+-F "page_url=$2"}
}

# 大きい動画は URL 投入を使う (control plane にバッファを持たせない)
put_video_url() {
  curl -sS -X POST "${BASE}/api/v1/create/videos/url" "${AUTH[@]}" \
    -H 'Content-Type: application/json' \
    -d "$(printf '{"url":"%s"}' "$1")"
}

video_status() { curl -sS "${AUTH[@]}" "${BASE}/api/v1/create/videos/$1"; }

# ---------------------------------------------------------------- 使用例
if [ "${1:-}" = "demo" ]; then
  echo "== limits =="
  limits | python3 -m json.tool

  echo "== 画像投入 =="
  put_image "${2:?画像パスを指定}" | python3 -m json.tool
fi
