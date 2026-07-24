#!/usr/bin/env python3
"""det_10g_dynbatch.onnx の精度検証 (batch == per-image を実証).

3 段の一致検証を **CPUExecutionProvider** で行う (§5 安全制約: 稼働中 GPU worker に
2 つ目のモデルをロードしない。 batch==逐次 は計算グラフの性質で provider 非依存)。

  [1] 生出力: 合成 batch の per-image スライス vs 元モデル 1 枚推論 (max abs diff)
  [2] N=1 no-op: dyn モデル == 元モデル (perm 書換が batch=1 で無害な証明)
  [3] 実画像 M≥32: FaceAnalysis 相当の後処理 (anchor decode+NMS) 後の顔が
      per-image detect と IoU / det_score / kps 一致

実画像は .17 raw MinIO から image_id keyed で取得 (production_main._fetch_raw_minio と同経路)。

使い方 (GPU ホスト worker venv, CPU 実行):
  /home/www/face_search/bin/python verify_scrfd_dynbatch.py \
      /home/www/.insightface/models/buffalo_l/det_10g.onnx \
      /tmp/det_10g_dynbatch.onnx \
      /mnt/paps-ai/ai/.env  64
"""
from __future__ import annotations

import sys

import cv2
import numpy as np
from insightface.model_zoo.scrfd import SCRFD, distance2bbox, distance2kps

INPUT = (640, 640)


def load_env(p: str) -> dict[str, str]:
    d: dict[str, str] = {}
    for ln in open(p, encoding="utf-8", errors="replace"):
        ln = ln.strip()
        if not ln or ln.startswith("#") or "=" not in ln:
            continue
        k, _, v = ln.partition("=")
        d[k.strip()] = v.strip().strip('"').strip("'")
    return d


def preprocess(img):
    im_ratio = float(img.shape[0]) / img.shape[1]
    model_ratio = float(INPUT[1]) / INPUT[0]
    if im_ratio > model_ratio:
        nh = INPUT[1]; nw = int(nh / im_ratio)
    else:
        nw = INPUT[0]; nh = int(nw * im_ratio)
    ds = float(nh) / img.shape[0]
    di = np.zeros((INPUT[1], INPUT[0], 3), np.uint8)
    di[:nh, :nw, :] = cv2.resize(img, (nw, nh))
    return di, ds


def anchor_centers(det, h, w, stride):
    key = (h, w, stride)
    if key in det.center_cache:
        return det.center_cache[key]
    ac = np.stack(np.mgrid[:h, :w][::-1], axis=-1).astype(np.float32)
    ac = (ac * stride).reshape((-1, 2))
    if det._num_anchors > 1:
        ac = np.stack([ac] * det._num_anchors, axis=1).reshape((-1, 2))
    if len(det.center_cache) < 100:
        det.center_cache[key] = ac
    return ac


def decode_from_outs(det, per, det_scale):
    """per = 9 本の per-image slice。 SCRFD.forward+detect の後処理を写経。"""
    fmc = det.fmc
    thr = det.det_thresh
    scores_list, bboxes_list, kpss_list = [], [], []
    for idx, stride in enumerate(det._feat_stride_fpn):
        scores = per[idx]
        bbox_preds = per[idx + fmc] * stride
        h = 640 // stride; w = 640 // stride
        ac = anchor_centers(det, h, w, stride)
        pos = np.where(scores >= thr)[0]
        bboxes = distance2bbox(ac, bbox_preds)
        scores_list.append(scores[pos]); bboxes_list.append(bboxes[pos])
        if det.use_kps:
            kps_preds = per[idx + fmc * 2] * stride
            kpss = distance2kps(ac, kps_preds).reshape((kps_preds.shape[0], -1, 2))
            kpss_list.append(kpss[pos])
    scores = np.vstack(scores_list); order = scores.ravel().argsort()[::-1]
    bboxes = np.vstack(bboxes_list) / det_scale
    kpss = np.vstack(kpss_list) / det_scale if det.use_kps else None
    pre = np.hstack((bboxes, scores)).astype(np.float32, copy=False)[order, :]
    keep = det.nms(pre)
    dets = pre[keep, :]
    if det.use_kps:
        kpss = kpss[order, :, :][keep, :, :]
    return dets, kpss


def detect_batch(det, imgs, asz):
    dis, scales = [], []
    for im in imgs:
        di, ds = preprocess(im); dis.append(di); scales.append(ds)
    blob = cv2.dnn.blobFromImages(dis, 1.0 / 128.0, INPUT, (127.5, 127.5, 127.5), swapRB=True)
    outs = det.session.run(det.output_names, {det.input_name: blob})
    res = []
    for i in range(len(imgs)):
        per = [outs[k][i * asz[k % 3]:(i + 1) * asz[k % 3]] for k in range(9)]
        res.append(decode_from_outs(det, per, scales[i]))
    return res


def iou(a, b):
    xx1 = max(a[0], b[0]); yy1 = max(a[1], b[1])
    xx2 = min(a[2], b[2]); yy2 = min(a[3], b[3])
    inter = max(0, xx2 - xx1) * max(0, yy2 - yy1)
    ua = (a[2] - a[0]) * (a[3] - a[1]) + (b[2] - b[0]) * (b[3] - b[1]) - inter
    return inter / ua if ua > 0 else 0.0


def main():
    orig_path, dyn_path, env_path = sys.argv[1], sys.argv[2], sys.argv[3]
    want = int(sys.argv[4]) if len(sys.argv) > 4 else 64
    sub = 8
    env = load_env(env_path)

    ref = SCRFD(orig_path); ref.prepare(-1, det_thresh=0.5, input_size=INPUT)
    dyn = SCRFD(dyn_path);  dyn.prepare(-1, det_thresh=0.5, input_size=INPUT)
    iname = ref.input_name; onames = ref.output_names
    asz = [(640 // s) * (640 // s) * dyn._num_anchors for s in dyn._feat_stride_fpn]

    # [1] raw output parity (synthetic) + [2] N=1 no-op
    rng = np.random.RandomState(0)
    one = rng.rand(1, 3, 640, 640).astype(np.float32)
    d1 = max(np.abs(a.astype(np.float64) - b.astype(np.float64)).max()
             for a, b in zip(ref.session.run(onames, {iname: one}),
                             dyn.session.run(dyn.output_names, {dyn.input_name: one})))
    batch = rng.rand(4, 3, 640, 640).astype(np.float32)
    per = [ref.session.run(onames, {iname: batch[i:i + 1]}) for i in range(4)]
    bout = dyn.session.run(dyn.output_names, {dyn.input_name: batch})
    A = [per[0][k].shape[0] for k in range(9)]
    raw_md = max(np.abs(bout[k][i * A[k]:(i + 1) * A[k]].astype(np.float64)
                        - per[i][k].astype(np.float64)).max()
                 for k in range(9) for i in range(4))
    print(f"[1] raw batch-slice vs per-image  max abs diff = {raw_md:.3e}")
    print(f"[2] N=1 dyn-vs-orig               max abs diff = {d1:.3e}")

    # [3] real images
    import mariadb
    from minio import Minio
    db = mariadb.connect(host=env["DB_HOST"], port=int(env.get("DB_PORT") or 3306),
                         user=env["DB_USER"], password=env["DB_PASS"], database=env["DB_NAME"])
    cur = db.cursor()
    cur.execute("SELECT DISTINCT image_id FROM crawl_face WHERE image_id IS NOT NULL "
                "ORDER BY id DESC LIMIT 800")
    cand = [r[0] for r in cur.fetchall()]; cur.close(); db.close()
    mc = Minio(env["MINIO_RAW_ENDPOINT"], access_key=env["MINIO_ACCESS_KEY"],
               secret_key=env["MINIO_SECRET_KEY"], secure=False)
    bucket = env.get("MINIO_RAW_BUCKET") or "raw"

    def fetch(iid):
        dir_no = ((int(iid) - 1) // 1000 + 1) * 1000
        try:
            r = mc.get_object(bucket, f"{dir_no}/{iid}.jpg")
            data = r.read(); r.close(); r.release_conn()
            return cv2.imdecode(np.frombuffer(data, np.uint8), cv2.IMREAD_COLOR)
        except Exception:
            return None

    imgs = []
    for iid in cand:
        if len(imgs) >= want:
            break
        if iid is None:
            continue
        im = fetch(iid)
        if im is None:
            continue
        if ref.detect(im)[0].shape[0] > 0:
            imgs.append(im)

    ref_res = [ref.detect(im) for im in imgs]
    dyn_res = []
    for s in range(0, len(imgs), sub):
        dyn_res.extend(detect_batch(dyn, imgs[s:s + sub], asz))

    tot = matched = cmm = 0
    max_sd = max_kd = 0.0; min_iou = 1.0
    for i in range(len(imgs)):
        rd, rk = ref_res[i]; dd, dk = dyn_res[i]
        if rd.shape[0] != dd.shape[0]:
            cmm += 1
        tot += rd.shape[0]; used = set()
        for a in range(rd.shape[0]):
            best, bi = -1.0, -1
            for b in range(dd.shape[0]):
                if b in used:
                    continue
                v = iou(rd[a][:4], dd[b][:4])
                if v > best:
                    best, bi = v, b
            if bi >= 0 and best > 0.99:
                used.add(bi); matched += 1; min_iou = min(min_iou, best)
                max_sd = max(max_sd, abs(float(rd[a][4]) - float(dd[bi][4])))
                if rk is not None and dk is not None:
                    max_kd = max(max_kd, np.abs(rk[a].astype(np.float64)
                                                - dk[bi].astype(np.float64)).max())

    print(f"[3] real images={len(imgs)} faces={tot} count_mismatch={cmm} "
          f"matched(IoU>0.99)={matched} min_iou={min_iou:.6f} "
          f"max_score_diff={max_sd:.3e} max_kps_diff={max_kd:.3e}")
    ok = (raw_md < 1e-3 and d1 < 1e-6 and cmm == 0 and matched == tot and max_sd < 1e-3)
    print("RESULT:", "PASS" if ok else "FAIL")
    sys.exit(0 if ok else 1)


if __name__ == "__main__":
    main()
