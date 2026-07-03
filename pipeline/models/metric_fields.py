"""flow snapshot 用の「捌いた件数/min」 metric 宣言 (2026-06-30)。

workload は declarative yaml がない API 作成型のため、 各 plugin の
output_json[field] のうち「捌いた件数」 として SUM すべき field 名を
ここに一元宣言する。 scheduler の 30s aggregate tick が runs.output_json
から SUM して workloads.observed_rate に書き込み、 flow.py が読み出す。

ない slug = 1 run = 1 件 想定 (= image-hash-extract / image-embed /
face-person-link / video-face-extract 等)。 これらは fallback で
runs/min (= 既存 throughput_counts) を flow snapshot に表示する。

新 plugin 追加時はここに entry を追加。 entry の値 = field 名 list で、
複数指定された場合は SUM (= dispatcher のように複数 enqueue 系を合算)。
"""

from __future__ import annotations

# slug → output_json で SUM すべき field 名のリスト
# (= 直近 1min での合計 = 捌いた件数/min)
#
# 複数 field 指定時は SUM。 UI で「投入数/min」として表示される。
# 実 output_json のキー名と一致する必要がある — 変更時は該当 plugin の
# process() 戻り値を確認すること。
WORKLOAD_METRIC_FIELDS: dict[str, list[str]] = {
    # paprika -> pipeline 入口
    "paprika-links-pull": ["inserted"],           # = crawl 新規 INSERT 件数
    "paprika-video-pull": ["inserted"],           # = crawl_video 新規 INSERT 件数
    "paprika-image-pull": ["inserted"],           # = crawl_image 新規 INSERT (downloaded は 内訳)
    # hub 側で本当に「新 Job 発生」する数だけ計上する。 adopted は既に hub に居た
    # URL を pipeline が再認識しただけで、 hub 側の新規生産はない。
    # 実測 (2026-07-03): submitted / (submitted + adopted) = 1 : 10 で 9 割が
    # 重複再送 (crawl → q の filler が dedup していない)。 UI にはこの重複部分
    # を混ぜず、 純粋な「hub 発生数」を出す。
    "paprika-job-submit": ["submitted"],
    # dispatcher (= 自己 tick で N 件を後段 queue に enqueue)
    "image-dispatcher": [
        "hash_detect_enqueued",
        "embed_image_enqueued",
        "embed_movie_enqueued",
    ],
    "video-dispatcher": ["enqueued"],
    # embed-write (= 1 tick で N 行 shard に書込み)。 written = 実 shard 書込み成功数。
    # claimed は tempo 内で pending → claimed に UPDATE した件数で成功指標ではない。
    "embed-write": ["written"],
}


def metric_fields_for(slug: str) -> list[str]:
    """slug の宣言 fields を返す (= 未宣言なら空 list)。"""
    return WORKLOAD_METRIC_FIELDS.get(slug, [])
