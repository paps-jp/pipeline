"""プラグイン配布 (配信 API + agent の自動取得) のテスト。"""

from __future__ import annotations

import io
import tarfile
from pathlib import Path

import pytest
from fastapi.testclient import TestClient

from pipeline.agent.desired import Desired, WorkloadDesired
from pipeline.agent.plugin_fetch import PluginFetcher
from pipeline.agent.supervisor import AgentSupervisor
from pipeline.config import Settings
from pipeline.control.server import create_app
from pipeline.plugin_dist import (
    MARKER_NAME,
    PluginDistError,
    build_archive,
    digest_dir,
    iter_files,
    safe_extract,
)

BASE = "http://testserver"


def _make_plugin(root: Path, name: str = "demo_plugin", body: str = "print('v1')\n") -> Path:
    d = root / name
    d.mkdir(parents=True)
    (d / "main.py").write_text(body)
    (d / "lib").mkdir()
    (d / "lib" / "util.py").write_text("X = 1\n")
    return d


# ------------------------------------------------------------ 共通部品 ----


def test_digest_changes_with_content_and_ignores_noise(tmp_path: Path) -> None:
    d = _make_plugin(tmp_path)
    sha1, n, _ = digest_dir(d)
    assert n == 2
    # キャッシュ・バックアップ・印は配布対象外 → ダイジェストは不変
    (d / "__pycache__").mkdir()
    (d / "__pycache__" / "main.cpython-313.pyc").write_bytes(b"x")
    (d / "main.py.bak").write_text("old")
    (d / "main.py.bak-20260101").write_text("old")
    (d / MARKER_NAME).write_text("abc")
    assert digest_dir(d)[0] == sha1
    (d / "main.py").write_text("print('v2')\n")
    assert digest_dir(d)[0] != sha1


def test_archive_roundtrip(tmp_path: Path) -> None:
    src = _make_plugin(tmp_path / "src")
    out = tmp_path / "out"
    safe_extract(build_archive(src), out)
    assert digest_dir(out)[0] == digest_dir(src)[0]
    assert [r for r, _ in iter_files(out)] == ["lib/util.py", "main.py"]


def _tar(entries: list[tuple[str, bytes | None, bytes]]) -> bytes:
    """(name, linkname or None, data) から tar.gz を作る。"""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tar:
        for name, link, data in entries:
            ti = tarfile.TarInfo(name)
            if link is not None:
                ti.type = tarfile.SYMTYPE
                ti.linkname = link.decode()
                tar.addfile(ti)
            else:
                ti.size = len(data)
                tar.addfile(ti, io.BytesIO(data))
    return buf.getvalue()


@pytest.mark.parametrize("entries", [
    [("../evil.py", None, b"x")],
    [("/abs/evil.py", None, b"x")],
    [("ok.py", None, b"x"), ("link", b"/etc/passwd", b"")],
])
def test_safe_extract_rejects_dangerous_archives(tmp_path: Path, entries) -> None:
    with pytest.raises(PluginDistError):
        safe_extract(_tar(entries), tmp_path / "dest")
    assert not (tmp_path / "evil.py").exists()


# ------------------------------------------------------------ 配信 API ----


@pytest.fixture
def ctrl(tmp_path: Path, monkeypatch):
    root = tmp_path / "ctrl_plugins"
    root.mkdir()
    monkeypatch.setenv("PIPELINE_PLUGIN_ROOT", str(root))
    app = create_app(Settings(db_url="sqlite:///:memory:", mode="dev"))
    with TestClient(app) as c:
        yield c, root


def test_api_manifest_and_archive(ctrl) -> None:
    c, root = ctrl
    d = _make_plugin(root)
    m = c.get("/api/v1/plugin-dist/demo_plugin/manifest").json()
    assert m["sha256"] == digest_dir(d)[0] and m["files"] == 2 and m["has_requirements"] is False
    r = c.get("/api/v1/plugin-dist/demo_plugin/archive.tar.gz")
    assert r.status_code == 200 and r.headers["x-plugin-sha256"] == m["sha256"]
    # 内容を変えるとダイジェストが変わる (キャッシュが古いままにならない)
    (d / "main.py").write_text("print('v2')\n")
    assert c.get("/api/v1/plugin-dist/demo_plugin/manifest").json()["sha256"] != m["sha256"]


def test_api_missing_plugin_is_404(ctrl) -> None:
    c, _ = ctrl
    assert c.get("/api/v1/plugin-dist/nope/manifest").status_code == 404
    assert c.get("/api/v1/plugin-dist/nope/archive.tar.gz").status_code == 404


@pytest.mark.parametrize("name", ["..", ".", "a/b", "..\\x", ".hidden", "", "x" * 200, "a b"])
def test_plugin_dir_rejects_unsafe_names(ctrl, name: str) -> None:
    """パス区切り・親参照・隠しディレクトリは、 ディレクトリに触る前に 400 で拒否する。"""
    from fastapi import HTTPException

    from pipeline.api.plugin_dist import _plugin_dir

    with pytest.raises(HTTPException) as ei:
        _plugin_dir(name)
    assert ei.value.status_code == 400


def test_plugin_dir_rejects_symlink_escape(ctrl, tmp_path: Path) -> None:
    from fastapi import HTTPException

    from pipeline.api.plugin_dist import _plugin_dir

    _, root = ctrl
    outside = tmp_path / "outside"
    outside.mkdir()
    try:
        (root / "linked").symlink_to(outside, target_is_directory=True)
    except OSError:
        pytest.skip("symlink を作れない環境")
    with pytest.raises(HTTPException) as ei:
        _plugin_dir("linked")
    assert ei.value.status_code == 404


# ------------------------------------------------------- agent の取得 ----


def _register(c: TestClient, slug: str, source_path: str, executor: str = "python_module") -> None:
    r = c.post("/api/v1/workloads", json={
        "slug": slug, "name": slug, "executor_type": executor,
        "executor_config": {"module": "main", "source_path": source_path}
        if executor == "python_module" else {"command": "true"},
    })
    assert r.status_code == 201, r.text


def _fetcher(c: TestClient, host_root: Path) -> PluginFetcher:
    return PluginFetcher(lambda: BASE, plugin_root=str(host_root), client=c,
                         install_requirements=False, ok_ttl_s=0.0, fail_backoff_s=0.0)


def test_missing_plugin_is_downloaded(ctrl, tmp_path: Path) -> None:
    c, root = ctrl
    d = _make_plugin(root)
    host_root = tmp_path / "host"
    host_root.mkdir()
    _register(c, "demo", str(host_root / "demo_plugin"))

    assert _fetcher(c, host_root).ensure("demo") is True
    got = host_root / "demo_plugin"
    assert digest_dir(got)[0] == digest_dir(d)[0]
    assert (got / MARKER_NAME).read_text().strip() == digest_dir(d)[0]
    assert not [p for p in host_root.iterdir() if p.name.startswith(".")]   # 一時ディレクトリが残らない


def test_managed_plugin_is_updated_when_control_plane_changes(ctrl, tmp_path: Path) -> None:
    c, root = ctrl
    d = _make_plugin(root)
    host_root = tmp_path / "host"
    host_root.mkdir()
    _register(c, "demo", str(host_root / "demo_plugin"))
    f = _fetcher(c, host_root)
    assert f.ensure("demo")
    (d / "main.py").write_text("print('v2')\n")
    assert f.ensure("demo")
    assert (host_root / "demo_plugin" / "main.py").read_text() == "print('v2')\n"


def test_unmanaged_existing_plugin_is_left_alone(ctrl, tmp_path: Path) -> None:
    """手動 rsync で入れたもの (印なし) は、 中身が違っても上書きしない。"""
    c, root = ctrl
    _make_plugin(root, body="print('control')\n")
    host_root = tmp_path / "host"
    _make_plugin(host_root, body="print('hand-deployed')\n")
    _register(c, "demo", str(host_root / "demo_plugin"))

    assert _fetcher(c, host_root).ensure("demo") is True
    assert (host_root / "demo_plugin" / "main.py").read_text() == "print('hand-deployed')\n"
    assert not (host_root / "demo_plugin" / MARKER_NAME).exists()


def test_digest_mismatch_keeps_existing_and_blocks_spawn(ctrl, tmp_path: Path, monkeypatch) -> None:
    c, root = ctrl
    d = _make_plugin(root)
    host_root = tmp_path / "host"
    host_root.mkdir()
    _register(c, "demo", str(host_root / "demo_plugin"))
    f = _fetcher(c, host_root)
    assert f.ensure("demo")
    before = (host_root / "demo_plugin" / "main.py").read_text()

    (d / "main.py").write_text("print('v2')\n")
    import pipeline.api.plugin_dist as api_mod
    real = api_mod._manifest
    monkeypatch.setattr(api_mod, "_manifest",
                        lambda n, p: {**real(n, p), "sha256": "0" * 64})   # 改ざん/破損を模す
    assert f.ensure("demo") is False
    assert (host_root / "demo_plugin" / "main.py").read_text() == before    # 元は無傷
    assert not [p for p in host_root.iterdir() if p.name.startswith(".")]


def test_source_path_outside_plugin_root_is_not_fetched(ctrl, tmp_path: Path) -> None:
    c, root = ctrl
    _make_plugin(root)
    host_root = tmp_path / "host"
    host_root.mkdir()
    elsewhere = tmp_path / "elsewhere" / "demo_plugin"
    _register(c, "demo", str(elsewhere))
    assert _fetcher(c, host_root).ensure("demo") is True
    assert not elsewhere.exists()


def test_non_python_module_workload_needs_nothing(ctrl, tmp_path: Path) -> None:
    c, _ = ctrl
    _register(c, "plain", "-", executor="shell")
    assert _fetcher(c, tmp_path).ensure("plain") is True


def test_control_plane_down_blocks_spawn(tmp_path: Path) -> None:
    import httpx

    def boom(request):
        raise httpx.ConnectError("down")

    f = PluginFetcher(lambda: BASE, plugin_root=str(tmp_path),
                      client=httpx.Client(transport=httpx.MockTransport(boom)),
                      ok_ttl_s=0.0, fail_backoff_s=0.0)
    assert f.ensure("whatever") is False


# ------------------------------------------------- supervisor との接続 ----


def _sup(fetcher) -> tuple[AgentSupervisor, list[str]]:
    sup = AgentSupervisor(host="h", pipeline_exe="pipeline", control_url=BASE, plugin_fetcher=fetcher)
    spawned: list[str] = []
    sup._spawn = lambda slug, gpu, cvd=None: spawned.append(slug) or "c1"   # type: ignore[method-assign]
    return sup, spawned


def _desired() -> Desired:
    return Desired(control_url=BASE, workloads=[WorkloadDesired(slug="demo", count=1, gpu=False)])


def test_supervisor_spawns_only_after_fetch_succeeds() -> None:
    sup, spawned = _sup(lambda slug: False)
    sup.reconcile(_desired())
    assert spawned == []                       # 取得失敗 → 起動しない
    sup, spawned = _sup(lambda slug: True)
    sup.reconcile(_desired())
    assert spawned == ["demo"]


def test_supervisor_without_fetcher_behaves_as_before() -> None:
    sup, spawned = _sup(None)
    sup.reconcile(_desired())
    assert spawned == ["demo"]
