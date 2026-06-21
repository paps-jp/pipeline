"""daemon が control plane から受け取る admin cmd を実行する."""

from __future__ import annotations

import io
import logging
import os
import subprocess
import tarfile
import tempfile
import urllib.request
from pathlib import Path
from typing import Any

log = logging.getLogger("pipeline.worker.admin_executor")


def _exec_shell(payload: dict[str, Any]) -> dict[str, Any]:
    """payload = {"script": "...", "cwd": "...", "use_sudo": True/False}.

    use_sudo=True なら `sudo bash -c <script>` で実行 (= sudoers NOPASSWD 設定が必要)。
    timeout = payload.get("timeout_s", 300)。
    """
    script = payload.get("script") or ""
    cwd = payload.get("cwd") or None
    use_sudo = bool(payload.get("use_sudo", False))
    timeout_s = int(payload.get("timeout_s", 300))
    if not script:
        return {"success": False, "error": "empty script"}

    if use_sudo:
        argv = ["sudo", "-n", "bash", "-c", script]
    else:
        argv = ["bash", "-c", script]
    try:
        p = subprocess.run(
            argv, cwd=cwd, capture_output=True, text=True, timeout=timeout_s,
        )
        return {
            "success": p.returncode == 0,
            "exit_code": p.returncode,
            "stdout": p.stdout[-65535:],
            "stderr": p.stderr[-65535:],
        }
    except subprocess.TimeoutExpired:
        return {"success": False, "error": f"timeout after {timeout_s}s"}
    except Exception as e:
        return {"success": False, "error": f"exec failed: {e}"[:1000]}


def _fetch_archive(payload: dict[str, Any], control_url: str) -> dict[str, Any]:
    """payload = {"url": "/api/v1/.../archive", "dst": "/opt/pipeline/..."}.

    control plane から tar.gz を HTTP GET し dst に展開。
    URL は absolute or path (= control_url を prefix)。
    """
    url = payload.get("url")
    dst = payload.get("dst")
    if not url or not dst:
        return {"success": False, "error": "missing url/dst"}
    if url.startswith("/"):
        url = f"{control_url.rstrip('/')}{url}"
    try:
        with urllib.request.urlopen(url, timeout=120) as r:
            data = r.read()
        Path(dst).mkdir(parents=True, exist_ok=True)
        with tarfile.open(fileobj=io.BytesIO(data), mode="r:gz") as tar:
            tar.extractall(dst)
        size = len(data)
        return {"success": True, "exit_code": 0,
                "stdout": f"fetched {size} bytes from {url}, extracted to {dst}"}
    except Exception as e:
        return {"success": False, "error": f"fetch_archive failed: {e}"[:1000]}


def _install_systemd(payload: dict[str, Any]) -> dict[str, Any]:
    """payload = {"unit_name": "pipeline-X.service", "content": "<unit>", "restart": bool}.

    /etc/systemd/system/<unit_name> に書き込み + daemon-reload + (restart=true) restart。
    sudo NOPASSWD 必須。
    """
    unit_name = payload.get("unit_name") or ""
    content = payload.get("content") or ""
    restart = bool(payload.get("restart", True))
    if not unit_name or not content:
        return {"success": False, "error": "missing unit_name/content"}
    if "/" in unit_name or ".." in unit_name:
        return {"success": False, "error": f"invalid unit_name: {unit_name}"}

    tmp = tempfile.NamedTemporaryFile(mode="w", suffix=".service", delete=False)
    try:
        tmp.write(content)
        tmp.close()
        path = f"/etc/systemd/system/{unit_name}"
        cmds = [
            ["sudo", "-n", "cp", tmp.name, path],
            ["sudo", "-n", "chmod", "644", path],
            ["sudo", "-n", "systemctl", "daemon-reload"],
            ["sudo", "-n", "systemctl", "enable", "--quiet", unit_name],
        ]
        if restart:
            cmds.append(["sudo", "-n", "systemctl", "restart", unit_name])
        stdouts = []
        for argv in cmds:
            p = subprocess.run(argv, capture_output=True, text=True, timeout=60)
            stdouts.append(f"$ {' '.join(argv)}\n{p.stdout}{p.stderr}")
            if p.returncode != 0:
                return {"success": False, "exit_code": p.returncode,
                        "stdout": "\n".join(stdouts),
                        "error": f"{argv[0]} ... failed: {p.stderr[:300]}"}
        return {"success": True, "exit_code": 0, "stdout": "\n".join(stdouts)}
    finally:
        try:
            os.unlink(tmp.name)
        except Exception:
            pass


def execute_admin_cmd(cmd: dict[str, Any], control_url: str) -> dict[str, Any]:
    """admin cmd を実行する dispatcher."""
    ty = cmd.get("cmd_type")
    payload = cmd.get("cmd_payload") or {}
    if ty == "exec_shell":
        return _exec_shell(payload)
    if ty == "fetch_archive":
        return _fetch_archive(payload, control_url)
    if ty == "install_systemd":
        return _install_systemd(payload)
    return {"success": False, "error": f"unknown cmd_type: {ty}"}
