#!/usr/bin/env python3
"""SCRFD det_10g を動的 batch (`[N,3,640,640]`) 対応 ONNX に変換する graph surgery.

背景 (docs/SCRFD_DYNAMIC_BATCH_TASK.md):
  buffalo_l の顔検出 `det_10g.onnx` は入力 batch=1 ハードコード。 複数画像を 1 回の
  forward に渡せない。 これを別名 `det_10g_dynbatch.onnx` として動的 batch 化する。

真因と修正 (調査で確定):
  各出力の直前 Transpose の perm が **[2,3,0,1]** (NCHW -> [H,W,N,C])。 batch 軸を
  中央に落とすため、 続く Reshape `[-1,C]` が batch>1 で画像間を最内で interleave し
  per-image スライスを壊す (batch=1 では無害なので export 時に見逃される)。
  perm を **[0,2,3,1]** (NCHW -> NHWC, batch を先頭に維持) に書き換えると:
    - N=1: 出力は元モデルと **bit 完全一致** (batch 軸 size1 は flatten 順に無影響)。
    - N>1: Reshape `[-1,C]` が image-major `[N*anchor, C]` を吐き、
            `out[i*A:(i+1)*A]` で per-image 分離可能。
  → 再 export 不要。 純粋な graph surgery で精度不変を達成。

使い方 (GPU ホスト上の worker venv で):
  /home/www/face_search/bin/python make_scrfd_dynbatch.py \
      /home/www/.insightface/models/buffalo_l/det_10g.onnx \
      /tmp/det_10g_dynbatch.onnx

出力は本番モデルを **上書きしない** 別名で作ること (CLAUDE.md / §5 安全制約)。
"""
from __future__ import annotations

import sys

import onnx

# batch を先頭に維持する正しい perm。 [2,3,0,1] (batch=1 export の名残) を置換する。
BAD_PERM = [2, 3, 0, 1]
GOOD_PERM = [0, 2, 3, 1]


def convert(src: str, dst: str) -> None:
    m = onnx.load(src)
    g = m.graph

    # 1) 全出力直前の Transpose perm を batch 保持型へ書き換え
    changed = 0
    for n in g.node:
        if n.op_type != "Transpose":
            continue
        for a in n.attribute:
            if a.name == "perm" and list(a.ints) == BAD_PERM:
                del a.ints[:]
                a.ints.extend(GOOD_PERM)
                changed += 1
    if changed == 0:
        raise SystemExit(
            "no Transpose with perm=[2,3,0,1] found — "
            "モデル構造が想定と異なる。 手動確認が必要。"
        )

    # 2) 入力 batch 軸を symbolic 'N' へ
    d0 = g.input[0].type.tensor_type.shape.dim[0]
    d0.ClearField("dim_value")
    d0.dim_param = "N"

    # 3) 各出力の第0軸 (= N*anchor) を symbolic 化 (固定値だと shape 推論が衝突する)
    for o in g.output:
        od0 = o.type.tensor_type.shape.dim[0]
        od0.ClearField("dim_value")
        od0.dim_param = "NA_" + o.name

    # stale な value_info はクリア (shape 推論の誤 propagation 防止)
    del g.value_info[:]

    onnx.save(m, dst)
    onnx.checker.check_model(dst)
    print(f"OK: rewrote {changed} Transpose nodes  -> {dst}")


if __name__ == "__main__":
    if len(sys.argv) != 3:
        print(__doc__)
        raise SystemExit(f"usage: {sys.argv[0]} <src.onnx> <dst.onnx>")
    convert(sys.argv[1], sys.argv[2])
