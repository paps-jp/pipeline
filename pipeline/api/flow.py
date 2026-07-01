"""/api/v1/flow — プラント風 flow dashboard 用集約 endpoint。

`pipeline/control/flow_layout.yaml` を読み込み、 各 workload の最新 run +
各 tank の SQL count を 1 リクエストで返す。 UI は 3-5s ごとに poll。

設計方針:
- N+1 防止: tank の SQL は 1 接続で順次評価、 結果は 3s in-mem cache。
- yaml の path は env `PAPRIKA_FLOW_LAYOUT_PATH` で上書き可。
- tank metric_sql は SELECT のみ、 複数文 (`;`) を拒否。
- MariaDB 接続情報は env `PAPRIKA_FLOW_DB_ENV` (デフォルト `/mnt/paps-ai/ai/.env`)
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
_DEFAULT_DB_ENV = "/home/paps-ai/ai/.env"

# tank metric cache (= 30 秒)
# COUNT(*) on large tables can take 10-20s; poll every 30s to avoid query pile-up
_TANK_CACHE_TTL_S = 30.0
_tank_cache: dict[str, tuple[float, int | None, str | None]] = {}
_tank_cache_lock = threading.Lock()
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
    pending: int | None = None
    capacity_warn: int | None = None
    fill_ratio: float | None = None
    error: str | None = None


class FlowEdge(BaseModel):
    id: str
    source: str
    target: str
    label: str | None = None
    metric_field: str | None = None
    dashed: bool = False
    rate_per_min: float | None = None


class FlowSnapshot(BaseModel):
    canvas: dict[str, Any]
    nodes: list[FlowNode]
    edges: list[FlowEdge]


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


def _exec_tanks(tank_sqls: dict[str, str]) -> dict[str, tuple[int | None, str | None]]:
    """tank_id → (value, error) を 1 接続でまとめて返す。 cache + DB 接続失敗時は全 tank に error。"""
    now = time.monotonic()
    results: dict[str, tuple[int | None, str | None]] = {}
    to_query: dict[str, str] = {}

    # 1. cache から取れるものは取る
    with _tank_cache_lock:
        for tid, sql in tank_sqls.items():
            cached = _tank_cache.get(tid)
            if cached and (now - cached[0]) < _TANK_CACHE_TTL_S:
                results[tid] = (cached[1], cached[2])
            else:
                to_query[tid] = sql
    if not to_query:
        return results

    # 2. DB 接続
    cfg = _read_db_cfg()
    if cfg is None:
        err = "no db env"
        for tid in to_query:
            results[tid] = (None, err)
        return results

    # mariadb (C wrapper) があれば優先、 無ければ pure-python pymysql。
    try:
        import mariadb  # type: ignore
        connect = lambda c: mariadb.connect(**{**c, "read_timeout": 28})  # noqa: E731
    except ImportError:
        try:
            import pymysql  # type: ignore
            connect = lambda c: pymysql.connect(    # noqa: E731
                host=c["host"], port=c["port"], user=c["user"],
                password=c["password"], database=c["database"],
                connect_timeout=c.get("connect_timeout", 3),
                read_timeout=30, autocommit=True,
            )
        except ImportError:
            err = "no mysql driver (install pymysql)"
            for tid in to_query:
                results[tid] = (None, err)
            return results

    # tank ごとに独立した短命接続を張る (= 1 query の失敗が他 tank を巻き込まない)。
    # 大きな表の COUNT が timeout しても他 tank の water level は読める。
    _STMT_TIMEOUT_MS = 25000  # 25s — 大テーブル COUNT の上限
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
            v = int(row[0]) if row and row[0] is not None else 0
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

    import datetime as _dt
    now_dt = _dt.datetime.now(_dt.timezone.utc)
    # latest_by_slug 用に広く取る。 30min cutoff は long-running な workload
    # (= paprika-links-pull が sleep 含む 1 tick 10min かけて走る等) でも最新 run を
    # 必ず拾えるよう余裕を持たせた数字。 image-embed 等の高頻度 workload は 30min で
    # ~5000 runs だが index 化済みなので軽い。
    thirty_min = now_dt - _dt.timedelta(minutes=30)

    # runs テーブルは高スループット時に巨大化する (= 35k/min 級・数百万行)。
    # 旧実装は list_since(30min) で全行 (stdout/output_json 込み) を Python に
    # ロードしていたため、 高負荷時に snapshot が激重 + throughput が窓ずれで
    # 過小表示 (= 実態 35k/min が 0.8/min 等) になっていた。 SQL 集計に置換:
    #   - throughput_by_slug = 直近 1min に開始した成功 run 数 = runs/min (COUNT のみ)
    #   - latest_by_slug      = 30min 窓で slug ごと最新 run 1 件 (= state/last_output 用)
    # いずれも started_at index を使い、 返る行は slug 数分だけ。 30min 以上アイドルな
    # workload は latest に出ず node.state=idle (= 実態通り)。
    one_min = now_dt - _dt.timedelta(minutes=1)
    latest_by_slug = runs_repo.latest_by_slug(thirty_min.isoformat())
    throughput_by_slug: dict[str, float] = {
        slug: float(cnt)
        for slug, cnt in runs_repo.throughput_counts(one_min.isoformat()).items()
    }

    # 2. tank SQL を一括評価
    tank_sqls: dict[str, str] = {}
    for n in nodes_raw:
        if n.get("kind") == "tank" and n.get("metric_sql"):
            tank_sqls[n["id"]] = n["metric_sql"]
    tank_results = _exec_tanks(tank_sqls) if tank_sqls else {}

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
        )
        if node.kind == "workload":
            slug = node.workload_slug or node.id
            r = latest_by_slug.get(slug)
            if r:
                node.state = _classify_state(r)
                # observed_rate (= 30s aggregator が runs.output_json から算出した
                # 「捌いた件数/min」) を優先表示。 まだ 1 回も aggregate されてない
                # 起動直後や、 1 run = 1 件で aggregator が runs/min fallback を
                # 書いた slug でも整合する (= rate_by_slug は全 workload 網羅)。
                # 0 のときは throughput_by_slug (= runs/min 直近 1min) で穴埋め。
                node.throughput_per_min = (
                    rate_by_slug.get(slug)
                    or throughput_by_slug.get(slug, 0.0)
                )
                fin = r.get("finished_at")
                if fin:
                    node.last_run_at = fin.isoformat() if hasattr(fin, "isoformat") else str(fin)
                node.last_output = r.get("output_json")
                if isinstance(node.last_output, dict):
                    a = node.last_output.get("adapt")
                    if isinstance(a, dict):
                        node.adapt = a
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

    return FlowSnapshot(canvas=canvas, nodes=nodes, edges=edges)
