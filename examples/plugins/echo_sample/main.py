"""echo_sample — pipeline-oss プラグインの最小サンプル。

3 関数を export するだけ:
- setup(**kwargs)      … プロセス起動時 1 回 / モデルロードや DB 接続を ここで行う
- process(task, **ctx) … 1 タスク毎 / dict を return すると runs.output_json に保存
- cleanup(**ctx)       … プロセス停止時 1 回 / 接続のクローズなど (任意)

ctx には setup() の戻り dict が展開されて渡る。
"""

from __future__ import annotations

import time
from typing import Any


def setup(**kwargs: Any) -> dict[str, Any]:
    prefix = str(kwargs.get("prefix", "[echo]"))
    sleep_secs = float(kwargs.get("sleep_secs", 0))
    fail_pk_substr = str(kwargs.get("fail_pk_substr", ""))
    return {
        "prefix": prefix,
        "sleep_secs": sleep_secs,
        "fail_pk_substr": fail_pk_substr,
    }


def process(task: Any, **ctx: Any) -> dict[str, Any]:
    prefix = ctx["prefix"]
    sleep_secs = ctx["sleep_secs"]
    fail_pk_substr = ctx["fail_pk_substr"]

    pk = task.pk
    extra = task.extra

    if sleep_secs > 0:
        time.sleep(sleep_secs)

    if fail_pk_substr and fail_pk_substr in pk:
        raise RuntimeError(
            f"echo_sample: pk={pk!r} contains fail trigger {fail_pk_substr!r}"
        )

    line = f"{prefix} pk={pk} extra={extra}"
    print(line)
    return {"echoed": line, "pk": pk, "extra_keys": list(extra.keys())}


def cleanup(**ctx: Any) -> None:
    # 本サンプルでは特に何もしない (= プロセス停止時に呼ばれる)
    pass
