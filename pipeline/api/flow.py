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

import logging
import os
import threading
import time
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
    """ストレージ等インフラ接続の死活 down 通知 (= フロー赤ボックスに表示)。"""
    name: str            # "minio:crawl" / "db:main"
    kind: str            # "minio" | "db"
    endpoint: str | None = None
    error: str | None = None


class FlowSnapshot(BaseModel):
    canvas: dict[str, Any]
    nodes: list[FlowNode]
    edges: list[FlowEdge]
    # ストレージ死活 down のみ (= reg.health() の ok=False)。GPU/silent-hang と同じ赤ボックスへ。
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
    if not env_path.exists():
        log.debug("flow: db env not found at %s", env_path)
        _db_cfg_cache = (now, None)
        return None
    env: dict[str, str] = {}
    for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        k, _, v = line.partition("=")
        env[k.strip()] = v.strip().strip('"').strip("'")
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
                fin = r.get("finished_at")
                if not fin:
                    # 現 run が進行中(未完了)なら、 最後に完了した tick の finished_at を出す
                    fin = (last_completed_by_slug.get(slug) or {}).get("finished_at")
                if fin:
                    node.last_run_at = fin.isoformat() if hasattr(fin, "isoformat") else str(fin)
                node.last_output = r.get("output_json")
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

    infra_alerts = [InfraAlert(**a) for a in _storage_alerts()]
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
    return {"since": since, "count": len(series), "series": series}
