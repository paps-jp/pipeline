"""/api/v1/flow — プラント風 flow dashboard 用集約 endpoint。

`pipeline/control/flow_layout.yaml` を読み込み、 各 workload の最新 run +
各 tank の SQL count を 1 リクエストで返す。 UI は 3-5s ごとに poll。

設計方針:
- N+1 防止: tank の SQL は 1 接続で順次評価、 結果は 3s in-mem cache。
- yaml の path は env `PAPRIKA_FLOW_LAYOUT_PATH` で上書き可。
- tank metric_sql は SELECT のみ、 複数文 (`;`) を拒否。
- MariaDB 接続情報は env `PAPRIKA_FLOW_DB_ENV` (デフォルト `PIPELINE_ENV_FILE` = `/etc/pipeline/.env`)
  から DB_HOST/DB_PORT/DB_USER/DB_PASS/DB_NAME を読み込み。 接続失敗時は
  各 tank に error を返すだけで snapshot 全体は壊さない。
"""

from __future__ import annotations

import datetime as _dt
import json
import logging
import os
import ssl
import threading
import time
import urllib.request
from pathlib import Path
from typing import Any

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel

from pipeline.repositories.runs import RunsRepository

log = logging.getLogger(__name__)
router = APIRouter(prefix="/api/v1/flow", tags=["flow"])

_LAYOUT_PATH_DEFAULT = Path(__file__).resolve().parents[1] / "control" / "flow_layout.yaml"
_DEFAULT_DB_ENV = os.environ.get("PIPELINE_ENV_FILE", "/etc/pipeline/.env")

# tank metric cache (= 30 秒)
# COUNT(*) on large tables can take 10-20s; poll every 30s to avoid query pile-up
_TANK_CACHE_TTL_S = 30.0
# これを超える TTL を宣言した tank は「重い」とみなし、 **リクエストパスで実行しない**
# (2026-07-23)。 1.8億行の COUNT(*) は 30-40s かかるため、 同期実行すると snapshot API が
# その間ブロックし、 過去に起きた「重いクエリで event loop が固まりフリート全体の
# heartbeat が滞留」を再現してしまう。 重い tank は stale 値を返しつつ裏で更新する。
_TANK_SYNC_MAX_TTL_S = 120.0
_TANK_BG_STMT_TIMEOUT_MS = 120000   # 2min — 1.8億行 COUNT(*) 用 (背景実行のみ)
# 上流バックログ (= tank 水位) の増減トレンド用 (2026-08-31)。 flow_rate_1m の
# metric='tank_level' 系列 (server の _flow_rate_1m_loop が 60s 毎に記録) を窓の
# 両端で差分して 件/分 を出す。 窓が短いと 30s cache の階段でブレるので 10 分。
# span が最低 3 分に満たない (= 起動直後・サンプル欠け) 間は None = 中立表示。
_TANK_TREND_WINDOW_MIN = 10
_TANK_TREND_MIN_SPAN_MIN = 3.0
_tank_cache: dict[str, tuple[float, int | None, str | None]] = {}
_tank_cache_lock = threading.Lock()
_tank_refresh_inflight: set[str] = set()
_db_cfg_cache: tuple[float, dict[str, Any] | None] = (0.0, None)


class FlowNode(BaseModel):
    id: str
    kind: str  # workload / tank / external
    x: float
    y: float
    label: str
    icon: str | None = None
    workload_slug: str | None = None
    url: str | None = None
    state: str | None = None
    throughput_per_min: float | None = None
    # 重複排除前の実件数/分 (2026-08-25)。 RAW_METRIC_FIELDS 宣言 slug のみ付く
    # (= 現状 paprika-image-pull のみ)。 未宣言 slug は常に None (= UI 非表示)。
    raw_throughput_per_min: float | None = None
    last_run_at: str | None = None
    last_output: dict[str, Any] | None = None
    adapt: dict[str, Any] | None = None
    pending: float | None = None
    capacity_warn: float | None = None
    fill_ratio: float | None = None
    # 件数以外の tank (= RAM ディスクの GB 等) の単位表記。 UI が値の後ろに付けるだけ。
    unit: str | None = None
    error: str | None = None
    # workload の最新 run が失敗した時のエラー文言 + 実行 worker (= GPU故障等の緊急検知用)。
    # tank と違い workload node ではこれまで未設定だった (state=failed だけ)。
    error_worker: str | None = None
    # 需要ベース配分の宣言 (2026-07-28): この workload が「食う」上流 tank id 集合。
    # supervisor が /api/v1/flow/snapshot 経由でここ宣言のタンク残量を集計し、
    # elastic scaler の pending (=want算定入力) に注入する。 dispatcher-elimination 後、
    # 自 queue が push-drain 済で 0 の workload の真の需要はここでしか見えない。
    demand_from: list[str] | None = None
    # 「停滞しているか」判定 (2026-08-31)。 workload node のみ。 この workload が食う
    # 上流 tank (demand_from 宣言、 無ければ tank→workload の実線 edge) の水位が
    # 直近 backlog_trend_span_min 分で何件/分 増減したか。
    #   > 0 = 積み上がっている (= 捌けていない)  / < 0 = 減らせている
    # edge の rate_per_min は宣言 metric が欠けると隣の workload の throughput を
    # 借りる「借り物」なので判定には使わない (= 実 SQL count の時系列だけを使う)。
    backlog_trend_per_min: float | None = None
    backlog_trend_span_min: float | None = None
    backlog_tanks: list[str] | None = None


class FlowEdge(BaseModel):
    id: str
    source: str
    target: str
    label: str | None = None
    metric_field: str | None = None
    # tank と同じ SELECT を edge にも許す。 送信元が workload でない pipe
    # (例: RAM ディスク MinIO の流入出) は last_output も throughput_per_min も
    # 持たないため、 metric_field / throughput 借用のどちらでも rate が出せない。
    metric_sql: str | None = None
    dashed: bool = False
    rate_per_min: float | None = None


class InfraAlert(BaseModel):
    """ストレージ等インフラの異常通知 (= フロー赤ボックスに表示)。"""
    name: str            # "minio:crawl" / "db:main" / "garage/local-lvm" / "faiss:search"
    kind: str            # "minio" | "db" | "thinpool" | "storage_full" | "faiss"
    endpoint: str | None = None
    error: str | None = None
    severity: str = "crit"       # "crit" | "warn"
    detail: str | None = None    # 容量系の内訳 ("768.8G / 794.3G (96.8%)")


class FlowSnapshot(BaseModel):
    canvas: dict[str, Any]
    nodes: list[FlowNode]
    edges: list[FlowEdge]
    # ストレージ死活 down (= reg.health() の ok=False) と PVE ストレージ逼迫。
    # GPU/silent-hang と同じ赤ボックスへ。
    infra_alerts: list[InfraAlert] = []


# ── ストレージ死活キャッシュ (background refresh = snapshot リクエストを絶対にブロックしない) ──
# MinIO は HTTP /minio/health/live、DB は SELECT 1 を pipeline.storage.StorageRegistry でプローブ。
# down が一件でもあれば snapshot.infra_alerts に載る → UI 左上の赤ボックスに集約表示。
_STORAGE_HEALTH_TTL_S = 30.0
_storage_health: dict[str, Any] = {"ts": 0.0, "alerts": [], "refreshing": False}
_storage_health_lock = threading.Lock()


def _refresh_storage_health() -> None:
    alerts: list[dict[str, Any]] = []
    try:
        from pipeline.storage import StorageRegistry
        env_path = os.environ.get("PAPRIKA_FLOW_DB_ENV") or str(_DEFAULT_DB_ENV)
        reg = StorageRegistry.from_env_file(env_path)
        for h in reg.health():
            if not h.ok:
                alerts.append({"name": h.name, "kind": h.kind,
                               "endpoint": h.endpoint, "error": h.error})
    except Exception as e:  # noqa: BLE001
        log.warning("flow: storage health probe failed: %s", e)
    with _storage_health_lock:
        _storage_health["ts"] = time.monotonic()
        _storage_health["alerts"] = alerts
        _storage_health["refreshing"] = False


def _storage_alerts() -> list[dict[str, Any]]:
    """cache を即返す。stale ならバックグラウンド refresh を1本だけ起動 (request 非ブロック)。"""
    now = time.monotonic()
    with _storage_health_lock:
        stale = (now - _storage_health["ts"]) >= _STORAGE_HEALTH_TTL_S
        if stale and not _storage_health["refreshing"]:
            _storage_health["refreshing"] = True
            threading.Thread(target=_refresh_storage_health, daemon=True).start()
        return list(_storage_health["alerts"])


# ── PVE ストレージ逼迫 (thin-pool 割当率) ───────────────────────────────────
# LVM-thin の割当は「一度でも書かれたブロック」の高水位で、 trim しない限り下がらない。
# 2026-08-02 に garage が 96.8% まで張り付き満杯まで残り 10h の状態が誰にも見えて
# いなかった (PVE GUI には出ていたが Flow を見ている運用では気付けない) ため、
# ここで PVE API を叩いて赤ボックスに合流させる。 死活 probe と同じく background
# refresh + cache で snapshot リクエストは絶対にブロックしない。
_PVE_HEALTH_TTL_S = 120.0
_pve_health: dict[str, Any] = {"ts": 0.0, "alerts": [], "refreshing": False}
_pve_health_lock = threading.Lock()
_pve_cfg_cache: tuple[float, dict[str, Any] | None] = (0.0, None)


def _parse_env_file(path: Path) -> dict[str, str]:
    """`KEY=value` 形式の env ファイルを読む。 存在しなければ空 dict。"""
    if not path.exists():
        return {}
    env: dict[str, str] = {}
    for line in path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
    return env


def _read_pve_cfg() -> dict[str, Any] | None:
    """PVE API 接続設定を env から読み込み (60s cache)。 未設定なら None = 機能 off。"""
    global _pve_cfg_cache
    now = time.monotonic()
    ts, cfg = _pve_cfg_cache
    if (now - ts) < 60.0 and ts > 0.0:
        return cfg
    env = _parse_env_file(Path(os.environ.get("PAPRIKA_FLOW_DB_ENV") or _DEFAULT_DB_ENV))

    def _get(key: str, default: str = "") -> str:
        return (os.environ.get(key) or env.get(key) or default).strip()

    urls = [u.strip().rstrip("/") for u in _get("PVE_API_URL").split(",") if u.strip()]
    token_id, secret = _get("PVE_TOKEN_ID"), _get("PVE_TOKEN_SECRET")
    if not urls or not token_id or not secret:
        _pve_cfg_cache = (now, None)
        return None
    cfg = {
        "urls": urls,
        "token": f"{token_id}={secret}",
        "storages": [s.strip() for s in _get("PVE_STORAGES", "local-lvm").split(",") if s.strip()],
        "warn_pct": float(_get("PVE_WARN_PCT", "85")),
        "crit_pct": float(_get("PVE_CRIT_PCT", "93")),
    }
    _pve_cfg_cache = (now, cfg)
    return cfg


def _pve_get(base: str, path: str, token: str, timeout: float = 4.0) -> Any:
    """PVE API を 1 本叩く。 証明書は自己署名なので検証しない (クラスタ内 LAN)。"""
    req = urllib.request.Request(
        f"{base}/api2/json{path}", headers={"Authorization": f"PVEAPIToken={token}"},
    )
    ctx = ssl._create_unverified_context()  # noqa: S323 — PVE 既定の自己署名証明書
    with urllib.request.urlopen(req, timeout=timeout, context=ctx) as resp:  # noqa: S310
        return json.loads(resp.read().decode("utf-8")).get("data")


def _pve_fill_eta_h(base: str, token: str, node: str, storage: str,
                    avail_b: float) -> float | None:
    """PVE 自身の RRD から直近の増加率を出し、 満杯までの残り時間 (h) を返す。

    ローカルに履歴を持たずに済むので、 probe は完全に stateless。 減少中/横ばいなら None。
    """
    try:
        rows = _pve_get(base, f"/nodes/{node}/storage/{storage}/rrddata?timeframe=hour",
                        token) or []
    except Exception:  # noqa: BLE001 — ETA は付加情報。 取れなくても alert 自体は出す
        return None
    pts = [(float(r["time"]), float(r["used"])) for r in rows
           if r.get("time") is not None and r.get("used") is not None]
    if len(pts) < 2:
        return None
    (t0, u0), (t1, u1) = pts[0], pts[-1]
    if t1 <= t0 or u1 <= u0:
        return None
    rate_b_per_s = (u1 - u0) / (t1 - t0)
    if rate_b_per_s <= 0:
        return None
    return avail_b / rate_b_per_s / 3600.0


def _refresh_pve_health() -> None:
    alerts: list[dict[str, Any]] = []
    try:
        cfg = _read_pve_cfg()
        if cfg:
            alerts = _probe_pve_storages(cfg)
    except Exception as e:  # noqa: BLE001
        log.warning("flow: pve storage probe failed: %s", e)
    with _pve_health_lock:
        _pve_health["ts"] = time.monotonic()
        _pve_health["alerts"] = alerts
        _pve_health["refreshing"] = False


def _probe_pve_storages(cfg: dict[str, Any]) -> list[dict[str, Any]]:
    token = cfg["token"]
    base = ""
    nodes: list[dict[str, Any]] = []
    for url in cfg["urls"]:  # 先に応答した 1 台をクラスタ全体の入口として使う
        try:
            nodes = _pve_get(url, "/nodes", token) or []
            base = url
            break
        except Exception as e:  # noqa: BLE001
            log.debug("flow: pve entry %s unreachable: %s", url, e)
    if not base:
        log.warning("flow: no PVE API entrypoint reachable (%s)", ",".join(cfg["urls"]))
        return []

    alerts: list[dict[str, Any]] = []
    for n in nodes:
        node = str(n.get("node") or "")
        if not node or n.get("status") != "online":
            continue
        for storage in cfg["storages"]:
            try:
                st = _pve_get(base, f"/nodes/{node}/storage/{storage}/status", token)
            except Exception as e:  # noqa: BLE001 — 未定義 storage / ノード応答なしは黙って skip
                log.debug("flow: pve %s/%s status failed: %s", node, storage, e)
                continue
            total, used = float(st.get("total") or 0), float(st.get("used") or 0)
            if total <= 0:
                continue
            pct = used / total * 100.0
            severity = ("crit" if pct >= cfg["crit_pct"]
                        else "warn" if pct >= cfg["warn_pct"] else None)
            if severity is None:
                continue
            gib = 1024 ** 3
            detail = f"{used / gib:.1f}G / {total / gib:.1f}G ({pct:.1f}%)"
            eta_h = _pve_fill_eta_h(base, token, node, storage, total - used)
            if eta_h is not None and eta_h < 240:
                detail += f" · 満杯まで約 {eta_h:.1f}h"
            alerts.append({
                "name": f"{node}/{storage}",
                "kind": "thinpool" if st.get("type") == "lvmthin" else "storage_full",
                "endpoint": base,
                "severity": severity,
                "detail": detail,
                "error": detail,
            })
    return alerts


def _pve_alerts() -> list[dict[str, Any]]:
    """cache を即返す。 stale ならバックグラウンド refresh を1本だけ起動 (request 非ブロック)。"""
    if _read_pve_cfg() is None:
        return []
    now = time.monotonic()
    with _pve_health_lock:
        stale = (now - _pve_health["ts"]) >= _PVE_HEALTH_TTL_S
        if stale and not _pve_health["refreshing"]:
            _pve_health["refreshing"] = True
            threading.Thread(target=_refresh_pve_health, daemon=True).start()
        return list(_pve_health["alerts"])


# ── RAM ディスク MinIO (.48 video-ram) の逼迫 ─────────────────────────────────
# .48 は Paprika (producer) と video-face-extract (consumer) の間に挟まった
# tmpfs 90G のステージング。 実体が RAM なので満杯にすると MinIO が ENOSPC で
# 書けなくなり、 動画パイプラインが両側から止まる。 CT501 は swap:0 で逃げ場も無い。
#
# 2026-08-14 の調査で `memory.peak` が 89.65 GiB (= tmpfs ほぼ満杯) を記録して
# いたことが判明したが、 **どの監視にも出ていなかった**。 .48 には node_exporter
# が居らず、 PVE の storage API にも tmpfs は出ない (thin-pool しか見ない) ため、
# 既存の _pve_alerts では原理的に拾えない。 MinIO 自身の Prometheus メトリクスが
# 唯一のソースなので、 ここで直接叩いて同じ赤ボックスへ合流させる。
#
# 使用率は `capacity total - free` (= df 相当) で見る。 MinIO の削除は
# `.minio.sys/tmp/.trash` 経由の非同期 purge なので、 live オブジェクトのみを表す
# `usage_total_bytes` で見ると tmpfs を実際に食っている分を見落とす
# (実測でピーク時の 83〜99% が trash 側だった)。
_RAMDISK_HEALTH_TTL_S = 60.0
# 実値は env `PIPELINE_RAMDISK_TARGETS` で与える (公開リポジトリに内部 IP を
# 置かないため)。 未設定は「監視対象無し」= 赤ボックスが出なくなるので、
# _ramdisk_targets() で 1 度だけ警告を残す (黙って無効化させない)。
_RAMDISK_DEFAULT = ""
# 閾値は .48 の実測 envelope 由来。 2026-08-14 の 6 分連続実測で通常ピークは
# 80G tmpfs の 71.7% (58.8G) まで届き、 約 30 秒で自然に落ちる (Paprika が
# バケットを一括削除 → MinIO の trash purge 待ち、 の重なり)。 70/85 だと
# 平常運転で赤ボックスが点いてしまい、 alert として意味を失う。
_RAMDISK_WARN_PCT = 80.0
_RAMDISK_CRIT_PCT = 90.0
_ramdisk_health: dict[str, Any] = {"ts": 0.0, "alerts": [], "refreshing": False}
_ramdisk_health_lock = threading.Lock()


_warned: set[str] = set()


def _warn_once(key: str, msg: str) -> None:
    """同じ設定漏れを毎 tick 出さない (60 秒 TTL で回るので煩い)。"""
    if key in _warned:
        return
    _warned.add(key)
    log.warning("%s", msg)


def _ramdisk_targets() -> list[tuple[str, str]]:
    """監視対象の (表示名, base URL) 一覧。 env で上書き可、 空文字で無効化。

    書式: ``name=url,name2=url2``。 supervisor 側の ramdisk_metrics_url とは
    独立に設定できる (別プロセスなので設定を共有していない)。
    """
    raw = os.environ.get("PIPELINE_RAMDISK_TARGETS")
    if raw is None:
        raw = _RAMDISK_DEFAULT
        # 既定が空 = 未設定なら監視対象ゼロ。 それ自体は正しい挙動だが、 設定漏れと
        # 区別が付かないまま赤ボックスが出なくなるのが一番まずい (2026-08-22 に
        # links-pull が 2.5 時間黙って止まっていたのと同じ形) ので 1 度だけ残す。
        _warn_once("ramdisk",
                   "flow: PIPELINE_RAMDISK_TARGETS 未設定 — RAM ディスクの"
                   " 赤ボックスは出ません (systemd の drop-in で与えてください)")
    out: list[tuple[str, str]] = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        name, _, url = item.partition("=")
        if not url:
            continue
        out.append((name.strip(), url.strip().rstrip("/")))
    return out


def _ramdisk_probe_one(name: str, base: str) -> dict[str, Any] | None:
    """1 台分の使用率を読む。 逼迫していなければ None (= alert 無し)。"""
    url = f"{base}/minio/v2/metrics/cluster"
    req = urllib.request.Request(url, headers={"Accept": "text/plain"})
    with urllib.request.urlopen(req, timeout=4.0) as r:
        raw = r.read().decode("utf-8", "replace")

    vals: dict[str, float] = {}
    for ln in raw.splitlines():
        if not ln or ln.startswith("#"):
            continue
        metric, _, rest = ln.partition("{")
        if rest:
            _, _, v = rest.partition("} ")
        else:
            metric, _, v = ln.partition(" ")
        metric = metric.strip()
        if metric in ("minio_cluster_capacity_usable_total_bytes",
                      "minio_cluster_capacity_usable_free_bytes",
                      "minio_cluster_usage_total_bytes"):
            try:
                vals[metric] = float(v.strip())
            except ValueError:
                continue

    total = vals.get("minio_cluster_capacity_usable_total_bytes") or 0.0
    free = vals.get("minio_cluster_capacity_usable_free_bytes")
    if total <= 0 or free is None:
        # 403 (= MINIO_PROMETHEUS_AUTH_TYPE 未設定) もここに来る。 死活は
        # _storage_alerts が別途見ているので、 ここでは「読めない」だけ warn。
        return {"name": f"ramdisk:{name}", "kind": "storage_full",
                "endpoint": url, "severity": "warn",
                "error": "capacity metrics を読めない "
                         "(MINIO_PROMETHEUS_AUTH_TYPE=public が要る)"}

    used = max(0.0, total - free)
    pct = used / total * 100.0
    if pct < _RAMDISK_WARN_PCT:
        return None
    g = 1024 ** 3
    # 判定と表示は capacity(total-free) だけで行う。 これは df と一致する実測値。
    #
    # `minio_cluster_usage_total_bytes` (= live オブジェクトの実体) を引いて
    # 「live / 削除待ち」 の内訳を出したくなるが、 **やってはいけない**。 この値は
    # データスキャナが定期的に集計したもので数分〜数十分遅れる。 2026-08-14 の実測で
    # 実 du 8152MB に対しメトリクスは 11812MB (45% 過大) を返し、 引き算した
    # 「削除待ち」 は 8390MB の実測に対し 5000MB を示した。 used < live になる
    # 瞬間すらあり、 内訳が 0 に潰れる。 誤った内訳は内訳が無いより有害なので出さない。
    # 内訳が要るときは CT で直接 du を取る (`du -sm /data/paprika
    # /data/.minio.sys/tmp/.trash`)。
    detail = f"{used / g:.1f}G / {total / g:.1f}G ({pct:.1f}%) — tmpfs 逼迫"
    return {"name": f"ramdisk:{name}", "kind": "storage_full", "endpoint": base,
            "severity": "crit" if pct >= _RAMDISK_CRIT_PCT else "warn",
            "detail": detail}


def _refresh_ramdisk_health() -> None:
    alerts: list[dict[str, Any]] = []
    for name, base in _ramdisk_targets():
        try:
            a = _ramdisk_probe_one(name, base)
        except Exception as e:  # noqa: BLE001 — probe 失敗自体は死活側の担当
            log.warning("flow: ramdisk probe failed %s (%s): %s", name, base, e)
            continue
        if a:
            alerts.append(a)
    with _ramdisk_health_lock:
        _ramdisk_health["ts"] = time.monotonic()
        _ramdisk_health["alerts"] = alerts
        _ramdisk_health["refreshing"] = False


# ── storage_capacity 由来の RAM ディスク逼迫 (.47 image-ram) ──────────────────
# 元々 .47 の MinIO は Prometheus が 403 を返していた (CT500 に
# MINIO_PROMETHEUS_AUTH_TYPE=public の drop-in が無かった) ため、 上の
# _ramdisk_probe_one 経路が使えず、 storage_capacity テーブル (認証付き MinIO
# クライアントで収集、 repositories/storage_capacity.py の image-ram-47) を
# ソースにしてある。
#
# **2026-08-30 に .47 にも drop-in を入れたので probe 経路も使えるようになった**が、
# ここは storage_capacity のまま据え置く。 閾値 (_TANK_WARN_PCT) が hub の
# asset_spill high_pct と対になっていて、 probe 経路の _RAMDISK_*_PCT とは
# 意味が違うため。 移すなら下記「3 箇所」を揃えてから。
#
# 対象 tank の capacity_sql は 「warn ライン」 (= tmpfs 実容量 x _TANK_WARN_PCT)
# を返す約束にしてある。 よって pending >= capacity_warn がそのまま warn 到達で、
# crit はその CRIT/WARN 倍。 tank の fill_ratio は 1.0 で頭打ちになる (液面表示の
# ため) ので、 crit 判定には使えない。 pending と capacity_warn から直接出すこと。
#
# .47 の warn は **hub の asset_spill high_pct と同じ線**に置く。 ここを超えると
# hub は .17 (ディスク) へ spill し、 image-pull の取得が目に見えて遅くなる ——
# つまり「まだ壊れていないが経路が変わった」= 警告に値する状態そのもの。
# 2026-08-24 に high_pct を 80 → 90 へ上げた際、 ここが 80 のままだったため
# **spill (126G) より 14G 手前で警告が鳴り**、 「溢れた」と誤読させていた。
# high_pct を動かすときは flow_layout.yaml の capacity_sql の係数と
# _TANK_WARN_PCT を必ず一緒に動かすこと (3 箇所が同じ数字を持っている)。
# probe 経路 (.48) は tmpfs の性格が違うので _RAMDISK_*_PCT のまま据え置く。
_TANK_RAMDISK_ALERTS = {"minio-image-ram": "image-ram:.47"}
_TANK_WARN_PCT = 90.0
_TANK_CRIT_PCT = 95.0
_TANK_CRIT_OVER_WARN = _TANK_CRIT_PCT / _TANK_WARN_PCT


def _tank_ramdisk_alerts(nodes: list["FlowNode"]) -> list[dict[str, Any]]:
    """tank の実測値から RAM ディスク逼迫を出す。 probe が使えない .47 用。"""
    out: list[dict[str, Any]] = []
    warn_frac = _TANK_WARN_PCT / 100.0
    for node in nodes:
        label = _TANK_RAMDISK_ALERTS.get(node.id)
        if not label or node.pending is None or not node.capacity_warn:
            continue
        warn_at = float(node.capacity_warn)
        used = float(node.pending)
        if used < warn_at:
            continue
        total = warn_at / warn_frac if warn_frac else 0.0
        pct = (used / total * 100.0) if total else 0.0
        out.append({
            "name": f"ramdisk:{node.id}",
            "kind": "storage_full",
            "endpoint": label,
            "severity": ("crit" if used >= warn_at * _TANK_CRIT_OVER_WARN
                         else "warn"),
            "detail": f"{used:.1f}G / {total:.1f}G ({pct:.1f}%) — tmpfs 逼迫",
        })
    return out


# ── RAM ディスク逼迫による投入停止 (supervisor の submit-gate) ────────────────
# supervisor (_submit_gate_run) は .47 が crit を越えると workload の
# executor_config.init_kwargs に submit_paused=1 を書いて Paprika 投入を止める。
# 「止まっていること」が赤ボックスに出ないと、 スループットが落ちた理由が
# 分からないまま放置される (2026-08-30 の障害では、 逆に **誰も止めていない**
# ことが 3.6 時間気付かれなかった)。
#
# フラグそのものが状態なので、 supervisor の内部状態ではなく workloads テーブルを
# 直接読む。 supervisor が死んでいても実際に止まっていれば必ず出る。
def _submit_paused_alerts(db: Any) -> list[dict[str, Any]]:
    """init_kwargs.submit_paused が立っている workload を赤ボックスに出す。"""
    try:
        with db.transaction() as conn:
            rows = conn.execute(
                "SELECT slug, executor_config FROM workloads WHERE enabled = 1 "
                "OR executor_config LIKE '%submit_paused%'"
            ).fetchall()
    except Exception as e:  # noqa: BLE001 — 表示用なので落とさない
        log.warning("flow: submit-gate 状態の読み取りに失敗: %s", e)
        return []

    out: list[dict[str, Any]] = []
    for r in rows:
        raw = r["executor_config"]
        if not raw or "submit_paused" not in str(raw):
            continue
        try:
            cfg = json.loads(raw) if isinstance(raw, str) else (raw or {})
            paused = int((cfg.get("init_kwargs") or {}).get("submit_paused") or 0)
        except Exception:  # noqa: BLE001
            continue
        if not paused:
            continue
        out.append({
            "name": f"submit-paused:{r['slug']}",
            "kind": "storage_full",
            "endpoint": r["slug"],
            "severity": "warn",
            "detail": ("RAM ディスク逼迫のため投入を一時停止中 — "
                       "水位が resume 閾値まで下がれば supervisor が自動再開する"),
        })
    return out


def _ramdisk_alerts() -> list[dict[str, Any]]:
    """cache を即返す。 stale ならバックグラウンド refresh を1本だけ起動。"""
    if not _ramdisk_targets():
        return []
    now = time.monotonic()
    with _ramdisk_health_lock:
        stale = (now - _ramdisk_health["ts"]) >= _RAMDISK_HEALTH_TTL_S
        if stale and not _ramdisk_health["refreshing"]:
            _ramdisk_health["refreshing"] = True
            threading.Thread(target=_refresh_ramdisk_health, daemon=True).start()
        return list(_ramdisk_health["alerts"])


# ── faiss_api 死活 + index 鮮度 ───────────────────────────────────────────────
# faiss_api (.27:9000) は pipeline 管理外のプロセスで、 落ちても誰も検知できなかった
# (2026-08-04 に半日以上落ちたまま face-person-link が走り、 knn 不能を「候補ゼロ」と
# 誤読して全 face を新規 person 化していた)。 SSH 鍵が要る systemd 監視と違い
# GET /version は無認証で叩けるので、 検知だけ先にここへ合流させる。
#
# index 鮮度も同じ probe で見る。 faiss-index-build の max_age_hours (既定 168h) を
# 超えても再構築されていなければ、 検索に出ない embedding が積み上がっている。
_FAISS_HEALTH_TTL_S = 60.0
# 実値は env `PAPRIKA_FLOW_FAISS_URL`。 未設定なら FAISS の生存確認を
# 行わない (= 赤ボックスが出ない) ので、 こちらも警告を残す。
_FAISS_URL_DEFAULT = ""
_FAISS_STALE_H_DEFAULT = 168.0
_faiss_health: dict[str, Any] = {"ts": 0.0, "alerts": [], "refreshing": False}
_faiss_health_lock = threading.Lock()


def _faiss_index_age_h(version: str | None) -> float | None:
    """/version の "2026-07-31 04:03:40" を現在との時間差 (h) にする。"""
    if not version:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
        try:
            built = _dt.datetime.strptime(str(version)[:19], fmt)
        except ValueError:
            continue
        return (_dt.datetime.now() - built).total_seconds() / 3600.0
    return None


def _probe_faiss(url: str, stale_h: float) -> list[dict[str, Any]]:
    try:
        with urllib.request.urlopen(f"{url}/version", timeout=5) as resp:  # noqa: S310
            data = json.loads(resp.read().decode("utf-8", "replace"))
    except Exception as e:  # noqa: BLE001 — 不通そのものが検知したい事象
        return [{"name": "faiss:search", "kind": "faiss", "endpoint": url,
                 "severity": "crit", "error": str(e)[:200],
                 "detail": "knn 検索不能 — face-person-link は止めること"}]

    if not data.get("ready") or data.get("error"):
        return [{"name": "faiss:search", "kind": "faiss", "endpoint": url,
                 "severity": "crit",
                 "error": str(data.get("error") or "ready=false")[:200],
                 "detail": "index 未ロード — face-person-link は止めること"}]

    age_h = _faiss_index_age_h(data.get("version"))
    if age_h is not None and age_h >= stale_h:
        return [{"name": "faiss:index", "kind": "faiss", "endpoint": url,
                 "severity": "warn",
                 "error": f"index が {age_h / 24:.1f} 日前のまま",
                 "detail": f"build={data.get('version')} — 以降の embedding は検索に出ない"}]
    return []


def _refresh_faiss_health() -> None:
    alerts: list[dict[str, Any]] = []
    try:
        url = (os.environ.get("PAPRIKA_FLOW_FAISS_URL") or _FAISS_URL_DEFAULT).rstrip("/")
        if not url:
            _warn_once("faiss",
                       "flow: PAPRIKA_FLOW_FAISS_URL 未設定 — FAISS の生存確認は"
                       " 行いません (systemd の drop-in で与えてください)")
            return
        stale_h = float(os.environ.get("PAPRIKA_FLOW_FAISS_STALE_H")
                        or _FAISS_STALE_H_DEFAULT)
        alerts = _probe_faiss(url, stale_h)
    except Exception as e:  # noqa: BLE001
        log.warning("flow: faiss health probe failed: %s", e)
    with _faiss_health_lock:
        _faiss_health["ts"] = time.monotonic()
        _faiss_health["alerts"] = alerts
        _faiss_health["refreshing"] = False


def _faiss_alerts() -> list[dict[str, Any]]:
    """cache を即返す。 stale ならバックグラウンド refresh を1本だけ起動 (request 非ブロック)。"""
    now = time.monotonic()
    with _faiss_health_lock:
        stale = (now - _faiss_health["ts"]) >= _FAISS_HEALTH_TTL_S
        if stale and not _faiss_health["refreshing"]:
            _faiss_health["refreshing"] = True
            threading.Thread(target=_refresh_faiss_health, daemon=True).start()
        return list(_faiss_health["alerts"])


# ── GPU 死活 (= センサーが黙った GPU を赤ボックスに出す) ────────────────────
# 2026-08-15: ai-gpu8 が Xid 119 (GSP timeout) で `GPU requires reset` に落ち、
# **19 時間気付かれなかった**。 このとき UI の GPU カードは VRAM と Mem 帯域を
# 表示し続けていた (= mem_used/mem_total の凍結値から計算した見かけの数字) ので、
# 「カードに数字が出ている = 生きている」と読めてしまう。
#
# nvidia-smi は rc=0 で応答し、 温度/電力/クロックだけを `[N/A]` で返す。 つまり
# **temp_c が NULL であることだけが唯一の正直な信号**。 util_pct は MPS 下で
# service.py が power から逆算して埋めるため、 mem_used_mb はハング中も凍結値が
# 入り続けるため、 どちらも判定に使えない。
#
# supervisor の _gpu_health_watchdog が同じ条件でドレインするが、 こちらは表示専用
# (= watchdog が DRY でも、 apply 前でも人間には見える)。
_GPU_HEALTH_TTL_S = 60.0
_GPU_HEALTH_WINDOW_MIN_DEFAULT = 5
_GPU_HEALTH_MIN_SAMPLES_DEFAULT = 20
_gpu_health: dict[str, Any] = {"ts": 0.0, "alerts": [], "refreshing": False}
_gpu_health_lock = threading.Lock()


def _probe_gpu_health(db: Any, window_min: int, min_samples: int) -> list[dict[str, Any]]:
    """worker_metrics のセンサー生存率から故障 GPU を拾う。

    worker_metrics は reaper が 24h で刈るうえ ts に index があるので、 数分窓の
    集約は軽い。 host は worker_id 由来で正規化する (`w_ai_gpu8_a4` → `ai-gpu8`)。
    """
    since = (_dt.datetime.now(_dt.timezone.utc)
             - _dt.timedelta(minutes=window_min)).isoformat()
    with db.transaction() as conn:
        rows = conn.execute(
            "SELECT m.worker_id AS wid, COUNT(*) AS n, "
            "       SUM(CASE WHEN m.temp_c IS NOT NULL THEN 1 ELSE 0 END) AS temp_ok "
            "FROM worker_metrics m JOIN workers w ON w.id = m.worker_id "
            "WHERE m.ts >= :since AND w.state = 'active' "
            "GROUP BY m.worker_id",
            {"since": since},
        ).fetchall()

    per_host: dict[str, dict[str, int]] = {}
    for r in rows:
        wid = r["wid"] or ""
        parts = wid[2:].split("_") if wid.startswith("w_") else []
        host = (parts[0] + "-" + parts[1]) if len(parts) >= 2 else wid
        st = per_host.setdefault(host, {"n": 0, "temp_ok": 0})
        st["n"] += int(r["n"] or 0)
        st["temp_ok"] += int(r["temp_ok"] or 0)

    alerts: list[dict[str, Any]] = []
    for host, st in sorted(per_host.items()):
        if st["temp_ok"] or st["n"] < min_samples:
            continue
        # endpoint は付けない (name が既に host を持っており、 UI が二重表示する)。
        # detail は UI 側で 160 文字に切られるので、 効く情報から順に置く。
        alerts.append({
            "name": host, "kind": "gpu", "severity": "crit",
            "error": "GPU センサー無応答 — `GPU requires reset` の可能性",
            "detail": (f"直近 {window_min} 分の {st['n']} サンプル全てで温度/電力が NULL "
                       "(VRAM 表示は凍結値)。 workload は CPU フォールバックで完走し続ける。 "
                       "復旧は PVE で qm stop→qm start"),
        })
    return alerts


def _refresh_gpu_health(db: Any) -> None:
    alerts: list[dict[str, Any]] = []
    try:
        window_min = int(os.environ.get("PAPRIKA_FLOW_GPU_HEALTH_WINDOW_MIN")
                         or _GPU_HEALTH_WINDOW_MIN_DEFAULT)
        min_samples = int(os.environ.get("PAPRIKA_FLOW_GPU_HEALTH_MIN_SAMPLES")
                          or _GPU_HEALTH_MIN_SAMPLES_DEFAULT)
        alerts = _probe_gpu_health(db, window_min, min_samples)
    except Exception as e:  # noqa: BLE001 — 表示用なので落とさない
        log.warning("flow: gpu health probe failed: %s", e)
    with _gpu_health_lock:
        _gpu_health["ts"] = time.monotonic()
        _gpu_health["alerts"] = alerts
        _gpu_health["refreshing"] = False


def _gpu_alerts(db: Any) -> list[dict[str, Any]]:
    """cache を即返す。 stale ならバックグラウンド refresh を1本だけ起動 (request 非ブロック)。"""
    now = time.monotonic()
    with _gpu_health_lock:
        stale = (now - _gpu_health["ts"]) >= _GPU_HEALTH_TTL_S
        if stale and not _gpu_health["refreshing"]:
            _gpu_health["refreshing"] = True
            threading.Thread(target=_refresh_gpu_health, args=(db,), daemon=True).start()
        return list(_gpu_health["alerts"])


def _load_layout() -> dict[str, Any]:
    path = Path(os.environ.get("PAPRIKA_FLOW_LAYOUT_PATH") or _LAYOUT_PATH_DEFAULT)
    if not path.exists():
        raise HTTPException(404, f"flow layout not found: {path}")
    try:
        import yaml  # type: ignore
    except ImportError as e:
        raise HTTPException(500, "PyYAML not installed on server") from e
    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as e:
        raise HTTPException(500, f"yaml parse failed: {e}") from e
    return data


def _safe_sql(sql: str) -> bool:
    s = (sql or "").strip().rstrip(";").lower()
    if not s.startswith("select"):
        return False
    if ";" in sql.strip().rstrip(";"):
        return False
    return True


def _read_db_cfg() -> dict[str, Any] | None:
    """env file から MariaDB 接続情報を読み込み (60s cache)."""
    global _db_cfg_cache
    now = time.monotonic()
    ts, cfg = _db_cfg_cache
    if cfg is not None and (now - ts) < 60.0:
        return cfg
    env_path = Path(os.environ.get("PAPRIKA_FLOW_DB_ENV") or _DEFAULT_DB_ENV)
    env = _parse_env_file(env_path)
    if not env:
        log.debug("flow: db env not found at %s", env_path)
        _db_cfg_cache = (now, None)
        return None
    if not all(env.get(k) for k in ("DB_HOST", "DB_USER", "DB_PASS", "DB_NAME")):
        log.warning("flow: db env missing required keys")
        _db_cfg_cache = (now, None)
        return None
    cfg = {
        "host": env["DB_HOST"],
        "port": int(env.get("DB_PORT", "3306")),
        "user": env["DB_USER"],
        "password": env["DB_PASS"],
        "database": env["DB_NAME"],
        "connect_timeout": 3,
    }
    _db_cfg_cache = (now, cfg)
    return cfg


def _coerce_num(raw: Any) -> float | int:
    """SQL の 1 セルを数値化する。 整数値は int、 小数は float (Decimal も通す)。"""
    f = float(raw)
    return int(f) if f.is_integer() else f


def _exec_tanks(tank_sqls: dict[str, str],
                ttls: dict[str, float] | None = None,
                ) -> dict[str, tuple[float | None, str | None]]:
    """tank_id → (value, error) を 1 接続でまとめて返す。 cache + DB 接続失敗時は全 tank に error。

    ttls は tank_id → TTL 秒 (未指定は _TANK_CACHE_TTL_S)。 TTL が _TANK_SYNC_MAX_TTL_S を
    超える tank は「重い COUNT(*)」とみなし、 **リクエストパスでは実行せず** stale 値を
    返してバックグラウンドスレッドで更新する (初回のみ値なし=None)。
    """
    ttls = ttls or {}
    now = time.monotonic()
    results: dict[str, tuple[float | None, str | None]] = {}
    to_query: dict[str, str] = {}
    to_bg: dict[str, str] = {}

    # 1. cache から取れるものは取る
    with _tank_cache_lock:
        for tid, sql in tank_sqls.items():
            ttl = float(ttls.get(tid) or _TANK_CACHE_TTL_S)
            cached = _tank_cache.get(tid)
            if cached and (now - cached[0]) < ttl:
                results[tid] = (cached[1], cached[2])
                continue
            if ttl > _TANK_SYNC_MAX_TTL_S:
                # 重い tank: stale を返して裏で更新 (リクエストは絶対にブロックしない)
                results[tid] = (cached[1], cached[2]) if cached else (None, None)
                if tid not in _tank_refresh_inflight:
                    _tank_refresh_inflight.add(tid)
                    to_bg[tid] = sql
            else:
                to_query[tid] = sql
    if to_bg:
        threading.Thread(target=_tank_refresh_bg, args=(to_bg,),
                         name="tank-refresh", daemon=True).start()
    if not to_query:
        return results
    results.update(_tank_query(to_query))
    return results


def _tank_refresh_bg(to_query: dict[str, str]) -> None:
    """重い tank をリクエストパス外で実行して cache を温める (daemon thread)。"""
    try:
        # 1.8億行の COUNT(*) は 30-40s かかる。 リクエストパス外なので余裕を持たせる。
        _tank_query(to_query, stmt_timeout_ms=_TANK_BG_STMT_TIMEOUT_MS)
    except Exception as e:                       # スレッドを絶対に落とさない
        log.warning("tank background refresh failed: %s", e)
    finally:
        with _tank_cache_lock:
            for tid in to_query:
                _tank_refresh_inflight.discard(tid)


def _tank_query(to_query: dict[str, str],
                stmt_timeout_ms: int | None = None,
                ) -> dict[str, tuple[float | None, str | None]]:
    """SQL を実行して cache を更新し tank_id → (value, error) を返す。"""
    now = time.monotonic()
    results: dict[str, tuple[float | None, str | None]] = {}

    # 2. DB 接続
    cfg = _read_db_cfg()
    if cfg is None:
        err = "no db env"
        for tid in to_query:
            results[tid] = (None, err)
        return results

    # ドライバ側の read_timeout は SQL の max_statement_time より少し長く取る
    # (SQL 側で先に打ち切らせ、 ソケットを途中で切らない)。
    _read_to = int((stmt_timeout_ms or 25000) / 1000) + 5
    # mariadb (C wrapper) があれば優先、 無ければ pure-python pymysql。
    try:
        import mariadb  # type: ignore
        connect = lambda c: mariadb.connect(**{**c, "read_timeout": _read_to})  # noqa: E731
    except ImportError:
        try:
            import pymysql  # type: ignore
            connect = lambda c: pymysql.connect(    # noqa: E731
                host=c["host"], port=c["port"], user=c["user"],
                password=c["password"], database=c["database"],
                connect_timeout=c.get("connect_timeout", 3),
                read_timeout=_read_to, autocommit=True,
            )
        except ImportError:
            err = "no mysql driver (install pymysql)"
            for tid in to_query:
                results[tid] = (None, err)
            return results

    # tank ごとに独立した短命接続を張る (= 1 query の失敗が他 tank を巻き込まない)。
    # 大きな表の COUNT が timeout しても他 tank の water level は読める。
    _STMT_TIMEOUT_MS = int(stmt_timeout_ms or 25000)  # 既定 25s — 大テーブル COUNT の上限
    for tid, sql in to_query.items():
        if not _safe_sql(sql):
            results[tid] = (None, "unsafe sql")
            continue
        conn = None
        try:
            conn = connect(cfg)
            cur = conn.cursor()
            # クエリタイムアウトをセッションで設定 (MariaDB max_statement_time = ms)
            try:
                cur.execute(f"SET SESSION max_statement_time={_STMT_TIMEOUT_MS}")
            except Exception:
                pass  # 古い MariaDB / 権限なし でも続行
            cur.execute(sql)
            row = cur.fetchone()
            # COUNT(*) は int だが、 容量系 tank は GB 等の小数を返す。 整数なら int の
            # まま返して既存 tank の表示 (= 桁区切り無しの件数) を変えない。
            v = _coerce_num(row[0]) if row and row[0] is not None else 0
            results[tid] = (v, None)
            with _tank_cache_lock:
                _tank_cache[tid] = (now, v, None)
            cur.close()
        except Exception as e:
            msg = f"{type(e).__name__}: {str(e)[:80]}"
            results[tid] = (None, msg)
        finally:
            if conn is not None:
                try:
                    conn.close()
                except Exception:
                    pass
    return results


def _trend_tank_sqls(nodes_raw: list[dict[str, Any]],
                     ) -> tuple[dict[str, str], dict[str, float]]:
    """トレンドを取る tank の (id→SQL, id→TTL)。

    除外するもの:
    - `accumulator: true` = 単調増加する総数タンク (crawl / *_hash / final 等)。
      「増えている」 のが正常なのでバックログ判定に混ぜると常時 赤 になる。
    - `unit` 付き (= GB 等 件数でない tank)。 件/分 として足し合わせられない。
    """
    sqls: dict[str, str] = {}
    ttls: dict[str, float] = {}
    for n in nodes_raw:
        if n.get("kind") != "tank" or not n.get("metric_sql"):
            continue
        if n.get("accumulator") or n.get("unit"):
            continue
        sqls[n["id"]] = n["metric_sql"]
        if n.get("metric_ttl_s"):
            ttls[n["id"]] = float(n["metric_ttl_s"])
    return sqls, ttls


def sample_tank_levels(db: Any) -> int:
    """各 tank の現在水位を flow_rate_1m (metric='tank_level') へ 1 分バケットで記録。

    server の _flow_rate_1m_loop から 60s 毎に呼ぶ。 **同期 SQL を投げるので
    必ず別スレッドで呼ぶこと** (event loop を止めるとフリートの heartbeat が滞留する)。
    `_exec_tanks` 経由なので tank ごとの TTL cache を尊重する (= 画面を開いている
    間は cache hit で追加クエリ 0、 誰も見ていない間だけ実際に COUNT が走る)。
    戻り値 = 書けた tank 数。
    """
    from pipeline.repositories.flow_rate import FlowRateRepository

    layout = _load_layout()
    sqls, ttls = _trend_tank_sqls(layout.get("nodes") or [])
    if not sqls:
        return 0
    results = _exec_tanks(sqls, ttls)
    frr = FlowRateRepository(db)
    now = _dt.datetime.now(_dt.timezone.utc)
    written = 0
    for tid, (val, err) in results.items():
        if err is not None or val is None:
            continue          # 初回 (背景更新待ち) / クエリ失敗は書かない
        frr.upsert(now, tid, "tank_level", float(val))
        written += 1
    return written


def _tank_level_trends(frr: Any, now_dt: _dt.datetime,
                       ) -> dict[str, tuple[float, float]]:
    """tank_id → (件/分, 実 span 分)。 flow_rate_1m の tank_level 系列を窓の両端で差分。

    窓内のサンプルが 1 点だけ / span が _TANK_TREND_MIN_SPAN_MIN 未満の tank は
    返さない (= UI は中立表示)。 回帰でなく両端差分にしているのは、 水位が 30s
    cache 由来の階段状で、 中間点を重み付けしても意味が増えないため。
    """
    since = (now_dt - _dt.timedelta(minutes=_TANK_TREND_WINDOW_MIN + 1)).isoformat()
    try:
        rows = frr.read_series(since, metric="tank_level")
    except Exception as e:                       # 履歴が無くても snapshot は壊さない
        log.warning("tank_level series read failed: %s", e)
        return {}
    first: dict[str, tuple[_dt.datetime, float]] = {}
    last: dict[str, tuple[_dt.datetime, float]] = {}
    for r in rows:
        tid = r.get("slug")
        try:
            ts = _dt.datetime.fromisoformat(str(r["ts_min"]))
            v = float(r["value"])
        except Exception:
            continue
        if tid not in first or ts < first[tid][0]:
            first[tid] = (ts, v)
        if tid not in last or ts > last[tid][0]:
            last[tid] = (ts, v)
    out: dict[str, tuple[float, float]] = {}
    for tid, (t0, v0) in first.items():
        t1, v1 = last[tid]
        span = (t1 - t0).total_seconds() / 60.0
        if span < _TANK_TREND_MIN_SPAN_MIN:
            continue
        out[tid] = ((v1 - v0) / span, span)
    return out


def _classify_state(latest_run: dict[str, Any] | None) -> str:
    if not latest_run:
        return "idle"
    if latest_run.get("success") is False:
        return "failed"
    finished = latest_run.get("finished_at")
    if not finished:
        return "running"
    out = latest_run.get("output_json") or {}
    adapt = out.get("adapt") or {}
    if isinstance(adapt, dict):
        if int(adapt.get("fail_streak") or 0) >= 2 or int(adapt.get("miss_streak") or 0) >= 3:
            return "backoff"
    return "running"


class _Pos(BaseModel):
    id: str
    x: float
    y: float


class _SaveLayoutReq(BaseModel):
    positions: list[_Pos]


@router.post("/layout")
def save_layout(payload: _SaveLayoutReq) -> dict[str, int]:
    """ドラッグ後の位置を yaml に書き戻す。 既存ノードの x/y のみ更新、
    他のフィールド/コメント/順序は維持 (ruamel が無い環境では PyYAML round-trip)。

    安全策:
    - 同じ id がレイアウトに無ければ skip。
    - ファイル書き込みは temp + os.replace で原子的。
    - 書込先 = `PAPRIKA_FLOW_LAYOUT_PATH` or 既定 path。
    """
    import tempfile
    path = Path(os.environ.get("PAPRIKA_FLOW_LAYOUT_PATH") or _LAYOUT_PATH_DEFAULT)
    if not path.exists():
        raise HTTPException(404, f"flow layout not found: {path}")
    try:
        import yaml  # type: ignore
    except ImportError as e:
        raise HTTPException(500, "PyYAML not installed") from e

    try:
        data = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    except Exception as e:
        raise HTTPException(500, f"yaml parse failed: {e}") from e

    nodes = data.get("nodes") or []
    pos_map = {p.id: p for p in payload.positions}
    updated = 0
    skipped = 0
    for n in nodes:
        nid = n.get("id")
        if not nid:
            continue
        p = pos_map.get(nid)
        if p is None:
            continue
        n["x"] = int(round(p.x))
        n["y"] = int(round(p.y))
        updated += 1
    skipped = len(pos_map) - updated

    # 原子的書込
    try:
        dump = yaml.safe_dump(
            data,
            allow_unicode=True,
            sort_keys=False,
            default_flow_style=False,
            indent=2,
        )
        tmp = tempfile.NamedTemporaryFile(
            "w", encoding="utf-8", dir=str(path.parent),
            prefix=path.name + ".", suffix=".tmp", delete=False,
        )
        try:
            tmp.write(dump)
            tmp.flush()
            os.fsync(tmp.fileno())
        finally:
            tmp.close()
        os.replace(tmp.name, str(path))
    except Exception as e:
        raise HTTPException(500, f"write failed: {e}") from e

    return {"updated": updated, "skipped": skipped}


@router.get("/snapshot", response_model=FlowSnapshot)
def snapshot(req: Request) -> FlowSnapshot:
    layout = _load_layout()
    canvas = layout.get("canvas") or {}
    nodes_raw = layout.get("nodes") or []
    edges_raw = layout.get("edges") or []

    runs_repo = RunsRepository(req.app.state.db)
    # enabled=0 (= 静止指定) workload の slug 集合。 node.state を idle 強制 + edge の
    # rate=0 強制で、 UI 上「アイドル状態」 と同じ表示に揃える (2026-06-28)。
    from pipeline.repositories.workloads import WorkloadRepository as _WLR
    _wlrepo = _WLR(req.app.state.db)
    _all_wls = _wlrepo.list_all()
    disabled_slugs = {w.slug for w in _all_wls if not w.enabled}
    # 「捌いた件数/min」 = scheduler の 30s metric aggregator が runs.output_json から
    # SUM 集計して workloads.observed_rate に書き込んだ値 (2026-06-30)。 ここでは
    # snapshot ごと SQL 集計せず DB 列 1 回 SELECT で済ませて負荷を抑える。
    rate_by_slug: dict[str, float] = {w.slug: float(w.observed_rate or 0.0) for w in _all_wls}
    # リアルタイム表示: flow_rate_1m の「最新の完了1分バケット」を優先する (2026-07-07)。
    # observed_rate は 20min 平均で ramp/restart 後に大きく遅れる (実 614/min が 175 表示等)。
    # items_per_min = その分に捌いた実件数、 runs_per_min = 完了 run 数。 短tick workload は
    # 毎分バケットが埋まるのでこれが最も実態に近い。 長tick で当該分に完了が無い slug は
    # ここに出ないので下流で _last_tick_rate にフォールバックする。
    from pipeline.repositories.flow_rate import FlowRateRepository as _FRR
    _frr = _FRR(req.app.state.db)
    import datetime as _dt
    # 直近 N 分の per-minute バケットを平均 = wall-clock の実 件数/分 (burst 誤りなし)。
    # 単一分だけだと noisy かつ tick 完了分にスパイクするので N 分平均で均す。
    # 窓は wall-clock でなく「実在する最新バケット」にアンカーする (aggregator の
    # 書込ラグで floor(now)-N が実データとズレ N-1 分しか拾えず過小になるのを防ぐ)。
    _RT_AVG_MIN = 3
    # 少し広め(N+3分)に読み、 実在する直近 N 分だけを平均対象にする。
    _rt_read_since = (_dt.datetime.now(_dt.timezone.utc)
                      - _dt.timedelta(minutes=_RT_AVG_MIN + 3)).isoformat()

    def _avg_1m(metric: str) -> dict[str, float]:
        rows = _frr.read_series(_rt_read_since, metric=metric)
        if not rows:
            return {}
        recent = sorted({r["ts_min"] for r in rows})[-_RT_AVG_MIN:]
        recent_set = set(recent)
        denom = len(recent) or 1
        agg: dict[str, float] = {}
        for r in rows:
            if r["ts_min"] in recent_set:
                agg[r["slug"]] = agg.get(r["slug"], 0.0) + float(r["value"] or 0.0)
        return {s: v / denom for s, v in agg.items()}

    rt_items_1m = _avg_1m("items_per_min")
    rt_runs_1m = _avg_1m("runs_per_min")
    rt_raw_items_1m = _avg_1m("raw_items_per_min")
    # paprika-job-submit は hub の「直近60秒に queued→running へ遷移した数」で置換する
    # (server の _hub_submit_rate_loop が 30s 毎に upsert)。 submitted 件数は
    # failed_other(hub timeout)/adopted のノイズを含み legacy fleet 分も欠くため。
    rt_hub_started = _frr.latest_rates("hub_started_per_min")

    now_dt = _dt.datetime.now(_dt.timezone.utc)
    # latest_by_slug 用に広く取る。 long-running self_loop (image-pull は hub read_timeout で
    # 1 tick が 30min 超になることがある) の進行中 run を state 判定で拾えるよう 60min にする
    # (窓が短いと長 tick が窓外→ 稼働中なのに state=idle 誤表示。 2026-07-07)。 index 化済で軽い。
    state_window = now_dt - _dt.timedelta(minutes=60)

    # runs テーブルは高スループット時に巨大化する (= 35k/min 級・数百万行)。
    # 旧実装は list_since(30min) で全行 (stdout/output_json 込み) を Python に
    # ロードしていたため、 高負荷時に snapshot が激重 + throughput が窓ずれで
    # 過小表示 (= 実態 35k/min が 0.8/min 等) になっていた。 SQL 集計に置換:
    #   - throughput_by_slug = 直近 1min に開始した成功 run 数 = runs/min (COUNT のみ)
    #   - latest_by_slug      = 30min 窓で slug ごと最新 run 1 件 (= state/last_output 用)
    # いずれも started_at index を使い、 返る行は slug 数分だけ。 30min 以上アイドルな
    # workload は latest に出ず node.state=idle (= 実態通り)。
    one_min = now_dt - _dt.timedelta(minutes=1)
    latest_by_slug = runs_repo.latest_by_slug(state_window.isoformat())
    throughput_by_slug: dict[str, float] = {
        slug: float(cnt)
        for slug, cnt in runs_repo.throughput_counts(one_min.isoformat()).items()
    }
    # 長 tick self_loop (image-pull/links-pull 等) は 1 tick が 20min 窓より長く、 現 tick が
    # 進行中(未完了)のため observed_rate も 1min 窓も 0 になり「稼働中なのに 0/min」表示になる。
    # 最後に完了した tick の output_json(inserted 等)/dispatch_secs から実レートを算定し fallback。
    sixty_min = now_dt - _dt.timedelta(minutes=60)
    last_completed_by_slug = runs_repo.latest_completed_by_slug(sixty_min.isoformat())
    from pipeline.models.metric_fields import WORKLOAD_METRIC_FIELDS as _WMF

    def _last_tick_rate(slug: str) -> float:
        lc = last_completed_by_slug.get(slug)
        oj = (lc or {}).get("output_json")
        if not isinstance(oj, dict):
            return 0.0
        try:
            ds = float(oj.get("dispatch_secs") or 0)
        except Exception:
            return 0.0
        if ds <= 0:
            return 0.0
        total = 0.0
        for f in (_WMF.get(slug) or []):
            try:
                total += float(oj.get(f) or 0)
            except Exception:
                pass
        return round(total / ds * 60.0, 1) if total > 0 else 0.0

    # 2. tank SQL を一括評価 (edge の metric_sql も同じ 1 接続に相乗りさせる)
    tank_sqls: dict[str, str] = {}
    tank_ttls: dict[str, float] = {}
    for n in nodes_raw:
        if n.get("kind") == "tank" and n.get("metric_sql"):
            tank_sqls[n["id"]] = n["metric_sql"]
            # 重い COUNT(*) は yaml で metric_ttl_s を長めに宣言する (背景更新に回る)
            if n.get("metric_ttl_s"):
                tank_ttls[n["id"]] = float(n["metric_ttl_s"])
        # 総容量も DB から引く tank (= RAM ディスクの total_bytes)。 yaml の
        # capacity_warn に固定値を書くと、 ディスクを拡張した時に嘘の分母になる。
        if n.get("kind") == "tank" and n.get("capacity_sql"):
            tank_sqls[f"__cap{n['id']}"] = n["capacity_sql"]
    for i, e in enumerate(edges_raw):
        if e.get("metric_sql"):
            tank_sqls[f"__edge{i}"] = e["metric_sql"]
    tank_results = _exec_tanks(tank_sqls, tank_ttls) if tank_sqls else {}

    # 3. nodes 組み立て
    nodes: list[FlowNode] = []
    for n in nodes_raw:
        node = FlowNode(
            id=n["id"],
            kind=n.get("kind", "workload"),
            x=float(n["x"]),
            y=float(n["y"]),
            label=n.get("label", n["id"]),
            icon=n.get("icon"),
            workload_slug=n.get("workload_slug"),
            url=n.get("url"),
            capacity_warn=n.get("capacity_warn"),
            unit=n.get("unit"),
            demand_from=n.get("demand_from"),
        )
        if node.kind == "workload":
            slug = node.workload_slug or node.id
            r = latest_by_slug.get(slug)
            if r:
                node.state = _classify_state(r)
                # リアルタイム優先順 (2026-07-07): wall-clock の実 件数/分を出す。
                # 20min 平均(observed_rate) と _last_tick_rate(burst率で dispatcher が桁跳ね) は
                # 主表示から外す。
                #  1. items_per_min 直近3分平均 = 宣言 metric(inserted/submitted/enqueued 等)の実 件数/分
                #  2. runs_per_min 直近3分平均 = 未宣言(1 run=1件: hash/embed/person-link)の実 件数/分
                #  3. _last_tick_rate = 上記に当分バケットが無い長tick/起動直後の保険 (最後の完了tick)
                #  4. observed_rate(20min平均) / runs/min 直近1min の最終フォールバック
                if slug in rt_hub_started:
                    # paprika-job-submit: hub の queued→running 遷移数/直近60秒 で置換
                    # (0 も正当な値なので明示分岐で採用)。
                    node.throughput_per_min = rt_hub_started[slug]
                else:
                    node.throughput_per_min = (
                        rt_items_1m.get(slug)
                        or rt_runs_1m.get(slug)
                        or _last_tick_rate(slug)
                        or rate_by_slug.get(slug)
                        or throughput_by_slug.get(slug, 0.0)
                    )
                # 重複排除前の実件数/分 (RAW_METRIC_FIELDS 宣言 slug のみ)。 バケットが
                # 無い長tick直後は None のままにし、 UI 側で「表示しない」 に倒す
                # (throughput_per_min のような多段フォールバックは今のところ不要 —
                # 未宣言 slug は元々出さない値なので 0 に埋めると誤解を招く)。
                node.raw_throughput_per_min = rt_raw_items_1m.get(slug)
                fin = r.get("finished_at")
                if not fin:
                    # 現 run が進行中(未完了)なら、 最後に完了した tick の finished_at を出す
                    fin = (last_completed_by_slug.get(slug) or {}).get("finished_at")
                if fin:
                    node.last_run_at = fin.isoformat() if hasattr(fin, "isoformat") else str(fin)
                node.last_output = r.get("output_json")
                if not isinstance(node.last_output, dict):
                    # 現 run が進行中(未完了)で output_json が無い場合、 last_run_at と
                    # 同じく最後に完了した tick の output_json にフォールバック
                    # (image-pull 等の interval_s が短い self_loop は常に進行中に
                    # 見えてしまい、 adapt バッジ/watermark が出せなくなるため)。
                    node.last_output = (last_completed_by_slug.get(slug) or {}).get("output_json")
                if isinstance(node.last_output, dict):
                    a = node.last_output.get("adapt")
                    if isinstance(a, dict):
                        node.adapt = a
                # 最新 run のエラー文言を workload node にも載せる (= 追加 SQL 無し、
                # latest_by_slug は既に取得済)。 UI 側で GPU 故障等の緊急エラーを
                # フロー左上の赤ボックスに集約表示するのに使う。
                _err = r.get("error")
                if _err:
                    node.error = str(_err)[:400]
                    node.error_worker = r.get("worker_id")
            else:
                node.state = "idle"
            # enabled=0 (= operator が停止指定) は state idle + throughput 0 強制。
            # claim 拒否で実質止まってるので、 UI も「アイドル」 表示に揃える。
            if slug in disabled_slugs:
                node.state = "idle"
                node.throughput_per_min = 0.0
                node.raw_throughput_per_min = 0.0
        elif node.kind == "tank":
            v, err = tank_results.get(node.id, (None, "no metric"))
            node.pending = v
            node.error = err
            # capacity_sql が取れた時だけ分母を差し替える。 失敗時は yaml の
            # capacity_warn (= 保険の固定値) にフォールバックして液面を出し続ける。
            if n.get("capacity_sql"):
                cap, cap_err = tank_results.get(f"__cap{node.id}", (None, "no capacity"))
                if cap_err is None and cap:
                    node.capacity_warn = cap
            if v is not None and node.capacity_warn:
                node.fill_ratio = min(1.0, v / float(node.capacity_warn))
        nodes.append(node)

    # 3.5 「停滞しているか」= 上流バックログの増減 (2026-08-31)
    # その workload が食う tank の水位が積み上がっていれば停滞、 減っていれば捌けている。
    # 上流 tank は demand_from 宣言を最優先する (dispatcher-elimination 後、 自 queue が
    # push-drain 済で 0 の workload の真の需要はそこにしか書いていない)。 宣言が無ければ
    # tank → workload の **実線** edge から拾う (dashed は参照線であって流路ではない)。
    tank_nodes = {n.id: n for n in nodes if n.kind == "tank"}
    trend_ids = set(_trend_tank_sqls(nodes_raw)[0])   # accumulator / 単位付きを除外済
    upstream_by_wl: dict[str, list[str]] = {}
    for e in edges_raw:
        if e.get("dashed") or e["from"] not in tank_nodes:
            continue
        upstream_by_wl.setdefault(e["to"], []).append(e["from"])
    tank_trends = _tank_level_trends(_frr, now_dt)
    for node in nodes:
        if node.kind != "workload":
            continue
        total = 0.0
        span = 0.0
        used: list[str] = []
        for tid in (node.demand_from or upstream_by_wl.get(node.id) or []):
            if tid not in trend_ids:
                continue
            tr = tank_trends.get(tid)
            if tr is None:
                continue
            total += tr[0]
            span = max(span, tr[1])
            used.append(tid)
        if used:
            node.backlog_trend_per_min = round(total, 2)
            node.backlog_trend_span_min = round(span, 1)
            node.backlog_tanks = used

    # 4. edges
    nodes_by_id = {n.id: n for n in nodes}
    edges: list[FlowEdge] = []
    for i, e in enumerate(edges_raw):
        eid = f"e{i}_{e['from']}__{e['to']}"
        edge = FlowEdge(
            id=eid,
            source=e["from"],
            target=e["to"],
            label=e.get("label"),
            metric_field=e.get("metric_field"),
            metric_sql=e.get("metric_sql"),
            dashed=bool(e.get("dashed", False)),
        )
        # rate 推定の優先順位:
        #  1. yaml の metric_field を source の last_output から取る (= 一番正確)
        #  2. source が workload → source.throughput_per_min を借りる
        #     (= 「produce 側」の流量を pipe 全体に投影)
        #  3. target が workload → target.throughput_per_min を借りる
        #     (= tank → worker の "pull" edge は consumer の処理速度を表示)
        #  4. source/target いずれかが running/backoff 状態の workload →
        #     最低速度 0.1 を割り当て (= AIMD で長 interval だが alive な
        #     workload もアニメ表示する。 アイドル/失敗との区別はつく)
        src = nodes_by_id.get(edge.source)
        tgt = nodes_by_id.get(edge.target)
        rate: float | None = None
        # 0. metric_sql (最優先)。 送信元が workload でない pipe 用。
        #    0 も有効値として扱う (= 「流れていない」 を fallback で埋めない)。
        if edge.metric_sql:
            v, err = tank_results.get(f"__edge{i}", (None, None))
            if err is None and v is not None:
                edge.rate_per_min = float(v)
                edges.append(edge)
                continue
        if edge.metric_field and src and isinstance(src.last_output, dict):
            v = src.last_output.get(edge.metric_field)
            if isinstance(v, (int, float)):
                rate = float(v)
        if (rate is None or rate == 0) and src and src.kind == "workload":
            t = src.throughput_per_min
            if t is not None and t > 0:
                rate = float(t)
        if (rate is None or rate == 0) and tgt and tgt.kind == "workload":
            t = tgt.throughput_per_min
            if t is not None and t > 0:
                rate = float(t)
        # src/tgt が disabled なら fallback も含めて完全 idle 化 (= 流れて見えない)。
        src_disabled = src and src.kind == "workload" and (src.workload_slug or src.id) in disabled_slugs
        tgt_disabled = tgt and tgt.kind == "workload" and (tgt.workload_slug or tgt.id) in disabled_slugs
        if src_disabled or tgt_disabled:
            rate = 0  # 強制 0 (= front の inactiveDim + 粒子無し)
        elif rate is None or rate == 0:
            for ep in (src, tgt):
                if ep and ep.kind == "workload" and ep.state in ("running", "backoff"):
                    rate = 0.1   # = アライブだが idle ぎみ。 frontend は粒子をゆっくり流す
                    break
        edge.rate_per_min = rate
        edges.append(edge)

    infra_alerts = [InfraAlert(**a) for a in
                    (*_storage_alerts(), *_pve_alerts(), *_ramdisk_alerts(),
                     *_tank_ramdisk_alerts(nodes), *_faiss_alerts(),
                     *_submit_paused_alerts(req.app.state.db),
                     *_gpu_alerts(req.app.state.db))]
    return FlowSnapshot(canvas=canvas, nodes=nodes, edges=edges, infra_alerts=infra_alerts)


@router.get("/rates")
def flow_rates(
    req: Request,
    since_min: int = 60,
    slug: str | None = None,
    metric: str | None = None,
) -> dict[str, Any]:
    """flow_rate_1m の 1 分バケット時系列を返す (グラフ用)。

    - since_min: 何分遡るか (既定 60、 上限 2880=48h)。
    - slug / metric: 任意フィルタ (metric 既定 = 全: runs_per_min / items_per_min)。
    runs を触らず flow_rate_1m (index 済) を引くだけなので、 複数ブラウザが叩いても軽い
    (= サーバ側 60s 集約が実質キャッシュ)。
    """
    import datetime as _dt

    from pipeline.repositories.flow_rate import FlowRateRepository

    n = max(1, min(int(since_min), 2880))
    since = (
        _dt.datetime.now(_dt.timezone.utc) - _dt.timedelta(minutes=n)
    ).replace(second=0, microsecond=0).isoformat()
    repo = FlowRateRepository(req.app.state.db)
    series = repo.read_series(since, slug=slug, metric=metric)
    if metric is None:
        # tank_level は rate ではなく「水位」 (slug も workload でなく tank id)。
        # 既定の rate 系列に混ぜると Throughput 画面の凡例に tank が並ぶので、
        # 明示的に metric='tank_level' を指定した時だけ返す。
        series = [r for r in series if r.get("metric") != "tank_level"]
    return {"since": since, "count": len(series), "series": series}
