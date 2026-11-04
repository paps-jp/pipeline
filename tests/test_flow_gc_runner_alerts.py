"""Flow 赤ボックスの Paprika 常駐 GC (gc_runner) 死活アラートのテスト (2026-11-04)。

なぜ専用の probe が要るか: 2026-11-04 に別デプロイで hub の `_reaper.py` が 08-30 版に
巻き戻り、 gc_runner 3 シャードは **コンテナ Up のまま毎パス AttributeError で 0 件** を
繰り返した。 `restart: unless-stopped` はプロセスが exit しない限り何もしないし、
気付ける信号は退役遅れタンクが 60 分を超えて赤くなるのを待つことだけだった。

runner は paprika.gc_runner_heartbeat に 1 行/シャードを upsert する:
  beat_at      毎書込 (starting / pass / idle / fail_closed のどれでも)
  last_pass_at 1 行でも消したパスだけ
  state_since  state が変わった時刻 (サーバ側で IF() 判定)
ここは `_gc_runner_judge` (純関数) の判定表を固定する。 DB 読み・キャッシュは
`_storage_alerts` と同じ background refresh なのでここでは触らない。

守る性質:
  - beat が 3 分止まれば crit。 プロセスが生きていても固まっていれば同じ。
  - beat が新しくても「15 分パスが進まない」 は、 退役遅れタンク > 60 分と**両方**揃った
    時だけ crit。 lag 0 (drain 済) で鳴らすと平常時に赤ボックスが点きっぱなしになる。
  - fail_closed (watermark 待ち) は意図的停止なので warn、 しかも 10 分継続してから。
  - シャード数の不整合 (行数 ≠ shards_total / shards_total が割れている) は残基に穴が
    空くので crit。
  - 全シャード無応答のときだけ 「hub 内フォールバックまで最大 GC_LOCK_TTL_S」 を足す。
  - 表が空 (= 未配備) なら何も出さない。 表が読めないときは呼び出し側が warn ログのみ
    (片側 fail-open)。
"""

from __future__ import annotations

import datetime as dt

from pipeline.api import flow

_T = dt.datetime(2026, 11, 4, 12, 9, 0)


def _row(shard: int, *, total: int = 3, state: str = "pass", beat_age: float = 10,
         pass_age: float | None = 30, state_age: float = 30) -> dict:
    return {
        "shard": shard, "shards_total": total, "hub_id": "gc-runner" if shard == 0
        else f"gc-runner-s{shard}", "state": state, "beat_age_s": beat_age,
        "pass_age_s": pass_age, "state_age_s": state_age, "beat_at": _T,
        "cutoff_at": _T - dt.timedelta(days=2),
    }


def _healthy(n: int = 3) -> list[dict]:
    return [_row(i, total=n) for i in range(n)]


def _names(alerts: list[dict]) -> list[str]:
    return [a["name"] for a in alerts]


def test_empty_table_means_not_deployed_no_alert():
    assert flow._gc_runner_judge([], 2700.0) == []


def test_healthy_fleet_is_silent_even_with_lag():
    """drain 中 (lag 2,700 分) でもパスが進んでいれば鳴らさない。"""
    assert flow._gc_runner_judge(_healthy(), 2700.0) == []
    assert flow._gc_runner_judge(_healthy(), 0.0) == []
    assert flow._gc_runner_judge(_healthy(), None) == []


def test_stale_beat_is_crit_with_last_seen_time():
    rows = _healthy()
    rows[1]["beat_age_s"] = 3 * 60 + 1
    alerts = flow._gc_runner_judge(rows, 0.0)
    assert _names(alerts) == ["gc_runner s1 無応答"]
    a = alerts[0]
    assert a["severity"] == "crit" and a["kind"] == "gc_runner"
    assert "12:09Z" in a["error"]
    # 2 本はまだ生きているので「全シャード無応答」 は付かない
    assert "全シャード" not in " ".join(_names(alerts))


def test_beat_just_under_three_minutes_is_fine():
    rows = _healthy()
    rows[1]["beat_age_s"] = 179
    assert flow._gc_runner_judge(rows, 0.0) == []


def test_alive_but_no_pass_needs_lag_to_fire():
    """2026-11-04 の実例: beat (idle) は新しいがパスが進まない。"""
    rows = _healthy()
    rows[2]["state"] = "idle"
    rows[2]["pass_age_s"] = 16 * 60
    # lag が無ければ「消す物が無い」 だけ
    assert flow._gc_runner_judge(rows, 0.0) == []
    assert flow._gc_runner_judge(rows, 60.0) == []
    assert flow._gc_runner_judge(rows, None) == []
    # lag > 60 分と揃って crit
    alerts = flow._gc_runner_judge(rows, 61.0)
    assert _names(alerts) == ["gc_runner s2 停滞"]
    assert alerts[0]["severity"] == "crit"
    assert "16 分前" in alerts[0]["error"]


def test_never_passed_counts_as_stale_pass():
    rows = _healthy()
    rows[0]["pass_age_s"] = None   # last_pass_at NULL
    rows[0]["state"] = "starting"
    assert _names(flow._gc_runner_judge(rows, 500.0)) == ["gc_runner s0 停滞"]
    assert flow._gc_runner_judge(rows, 0.0) == []


def test_recent_pass_with_lag_is_fine():
    rows = _healthy()
    rows[0]["pass_age_s"] = 14 * 60
    assert flow._gc_runner_judge(rows, 2700.0) == []


def test_fail_closed_is_warn_after_ten_minutes():
    rows = _healthy()
    for r in rows:
        r["state"] = "fail_closed"
        r["pass_age_s"] = 60 * 60
        r["state_age_s"] = 9 * 60
    # 10 分未満: 何も出さない (watermark の一時的な遅れで点かせない)
    assert flow._gc_runner_judge(rows, 2700.0) == []
    for r in rows:
        r["state_age_s"] = 10 * 60
    alerts = flow._gc_runner_judge(rows, 2700.0)
    assert _names(alerts) == ["GC 停止中: watermark 待ち"]
    assert alerts[0]["severity"] == "warn"
    assert "0,1,2" in alerts[0]["error"]
    # 意図的停止なので「停滞」 crit には倒さない (lag があっても)
    assert all(a["severity"] == "warn" for a in alerts)


def test_shard_count_mismatch_is_crit():
    rows = _healthy()[:2]          # 3 本中 2 本しか行が無い
    alerts = flow._gc_runner_judge(rows, 0.0)
    assert _names(alerts) == ["gc_runner シャード不整合"]
    assert alerts[0]["severity"] == "crit"
    assert "3 本中 2 本" in alerts[0]["error"]

    rows = _healthy()
    rows[2]["shards_total"] = 4    # 1 本だけ古い GC_SHARDS
    alerts = flow._gc_runner_judge(rows, 0.0)
    assert _names(alerts) == ["gc_runner シャード不整合"]
    assert "4 本中 3 本" in alerts[0]["error"]


def test_all_shards_dead_adds_fallback_note():
    rows = [_row(i, beat_age=20 * 60) for i in range(3)]
    alerts = flow._gc_runner_judge(rows, 2700.0)
    names = _names(alerts)
    assert names[:3] == ["gc_runner s0 無応答", "gc_runner s1 無応答", "gc_runner s2 無応答"]
    assert names[-1] == "gc_runner 全シャード無応答"
    assert "GC_LOCK_TTL_S" in alerts[-1]["detail"]
    assert all(a["severity"] == "crit" for a in alerts)


def test_dead_shard_is_not_also_reported_stalled():
    rows = _healthy()
    rows[1]["beat_age_s"] = 10 * 60
    rows[1]["pass_age_s"] = 10 * 60
    assert _names(flow._gc_runner_judge(rows, 2700.0)) == ["gc_runner s1 無応答"]


def test_alert_names_are_unique_per_shard():
    """フロントは key=`infra-${name}` で並べるので name は衝突してはいけない。"""
    rows = [_row(i, beat_age=20 * 60) for i in range(3)]
    names = _names(flow._gc_runner_judge(rows, 2700.0))
    assert len(names) == len(set(names))


def test_lag_is_borrowed_from_tank_cache(monkeypatch):
    monkeypatch.setattr(flow, "_tank_cache", {})
    assert flow._gc_runner_lag_min() is None
    monkeypatch.setattr(flow, "_tank_cache",
                        {flow._GC_RUNNER_LAG_TANK_ID: (0.0, 2700, None)})
    assert flow._gc_runner_lag_min() == 2700.0
    monkeypatch.setattr(flow, "_tank_cache",
                        {flow._GC_RUNNER_LAG_TANK_ID: (0.0, None, "timeout")})
    assert flow._gc_runner_lag_min() is None


def test_unreadable_table_logs_and_raises_nothing(monkeypatch):
    """表が無い / 権限無し は alert にしない (片側 fail-open)。 ただし黙らず warn する。"""
    monkeypatch.setattr(flow, "_read_db_cfg", lambda: {"host": "x"})

    def _boom(cfg):
        raise RuntimeError("Table 'paprika.gc_runner_heartbeat' doesn't exist")

    monkeypatch.setattr(flow, "_gc_runner_rows", _boom)
    warned: list[str] = []
    monkeypatch.setattr(flow.log, "warning", lambda msg, *a: warned.append(msg % a))
    monkeypatch.setattr(flow, "_gc_runner_health",
                        {"ts": 0.0, "alerts": [{"name": "stale"}], "refreshing": True})
    flow._refresh_gc_runner_health()
    assert flow._gc_runner_health["alerts"] == []
    assert flow._gc_runner_health["refreshing"] is False
    assert flow._gc_runner_health["ts"] > 0.0
    assert any("gc_runner heartbeat unreadable" in w for w in warned)


def test_sql_reads_ages_in_utc_not_now():
    """MariaDB の NOW() は JST、 列は UTC。 経過秒は UTC_TIMESTAMP() との差で取る。"""
    assert "UTC_TIMESTAMP()" in flow._GC_RUNNER_SQL
    assert "NOW()" not in flow._GC_RUNNER_SQL
    assert "paprika.gc_runner_heartbeat" in flow._GC_RUNNER_SQL


def test_thresholds_match_the_spec():
    assert flow._GC_RUNNER_STALE_BEAT_S == 3 * 60
    assert flow._GC_RUNNER_STALE_PASS_S == 15 * 60
    assert flow._GC_RUNNER_FAIL_CLOSED_S == 10 * 60
    assert flow._GC_RUNNER_LAG_MIN == 60.0
    assert flow._GC_RUNNER_TTL_S <= 60.0
