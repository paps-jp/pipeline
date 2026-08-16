"""Regression: failed jobs' already-downloaded images must be recovered.

A paprika job's asset list normally comes from ``GET /jobs/{id}/result``. That
row is written by the hub only when the worker sends ``WorkerJobComplete``, so a
job that ended ``failed`` has **no result at all** -- measured 2026-08-16, 40 of
40 sampled failed jobs returned ``assets: []``.

The images are not gone, though. The worker uploads each one to MinIO (.47) as
it captures it, so everything downloaded before the job died is still sitting
under the job's prefix. ``GET /jobs/{id}/assets.json`` enumerates that prefix
directly, so it sees them regardless of status: 33 of 35 sampled failed jobs
(94%) had images, averaging 46 per job.

Nobody read them. image-pull only walked ``/jobs/completes``, so the hub
eventually forgot the job, the prefix became an orphan, and asset-gc deleted it
-- crawled images thrown away at roughly 22 GB/hour.

The two payloads differ in exactly one field name (``source_url`` vs ``url``),
so normalising that lets failed jobs ride the existing ingest path unchanged --
same dedup, same crawl_id lookup, same prefix accounting.
"""

import importlib.util
import sys
import types
from pathlib import Path

import pytest

_PLUGIN = (
    Path(__file__).resolve().parents[1]
    / "plugins" / "paprika_image_pull" / "image_main.py"
)


@pytest.fixture(scope="module")
def mod():
    """Import image_main.py with its heavy deps stubbed.

    The module pulls in mariadb / PIL / requests / the pipeline core pool at
    import time; none of that is needed to exercise the pure normalisation.
    """
    stubs = {
        "mariadb": types.ModuleType("mariadb"),
        "requests": types.ModuleType("requests"),
        "PIL": types.ModuleType("PIL"),
        "PIL.Image": types.ModuleType("PIL.Image"),
        "pipeline": types.ModuleType("pipeline"),
        "pipeline.db": types.ModuleType("pipeline.db"),
        "pipeline.db.plugin_pool": types.ModuleType("pipeline.db.plugin_pool"),
    }
    stubs["PIL"].Image = stubs["PIL.Image"]
    stubs["pipeline.db.plugin_pool"].MariaPool = object
    saved = {k: sys.modules.get(k) for k in stubs}
    sys.modules.update(stubs)
    try:
        spec = importlib.util.spec_from_file_location("_image_main", _PLUGIN)
        m = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(m)
        yield m
    finally:
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v


# A real assets.json item, copied from job 7d8b67fe6cdb (failed, 98 assets).
_ITEM = {
    "name": "167036615.webp",
    "href": "/jobs/7d8b67fe6cdb/assets/167036615.webp",
    "size": 7328,
    "size_h": "7.2 KB",
    "ext": "webp",
    "kind": "image",
    "source_url": "https://img.doppiocdn.com/thumbs/1786879973/167036615",
    "page_url": "https://freelivec.com/56424/",
    "mime": "image/webp",
}


def test_source_url_becomes_url(mod):
    """THE mapping. Everything downstream -- dedup, the deadlink table, the
    crawl_image INSERT -- keys off ``url``; an item that keeps calling it
    ``source_url`` is silently dropped as a URL-less asset."""
    out = mod._assets_from_assets_json({"items": [_ITEM]})
    assert len(out) == 1
    assert out[0]["url"] == _ITEM["source_url"]


def test_normalised_item_survives_the_image_filter(mod):
    """It must look like an image to the same predicate the completed path
    uses, or the recovery ingests nothing."""
    out = mod._assets_from_assets_json({"items": [_ITEM]})
    assert mod._is_image_asset(out[0])


def test_fields_the_ingest_path_reads_are_carried_over(mod):
    out = mod._assets_from_assets_json({"items": [_ITEM]})[0]
    for k in ("name", "size", "mime", "url", "page_url", "href"):
        assert out[k] == _ITEM["source_url" if k == "url" else k]


@pytest.mark.parametrize(
    "bad",
    [
        {"name": "x.jpg", "mime": "image/jpeg"},                    # no source_url
        {"name": "x.jpg", "mime": "image/jpeg", "source_url": ""},
        {"name": "x.jpg", "mime": "image/jpeg", "source_url": "   "},
        {"name": "x.png", "mime": "image/png",
         "source_url": "data:image/png;base64,iVBOR"},
    ],
)
def test_unusable_items_are_dropped_not_counted(mod, bad):
    """A URL-less or inline-data asset can never be deduped or INSERTed, so it
    must not enter job_expected -- otherwise the job never reaches
    consumed >= expected and its prefix is pinned on .47 forever."""
    assert mod._assets_from_assets_json({"items": [bad]}) == []


def test_empty_and_missing_items_are_safe(mod):
    assert mod._assets_from_assets_json({}) == []
    assert mod._assets_from_assets_json({"items": []}) == []


def test_failed_is_a_resultless_status(mod):
    """The whole point: `failed` must route to assets.json. Hitting /result
    for it returns assets=[], which the caller reads as 'no images' and
    deletes the prefix -- destroying the very images we came for."""
    assert "failed" in mod.RESULTLESS_STATUSES
    assert "completed" not in mod.RESULTLESS_STATUSES
    assert "review" not in mod.RESULTLESS_STATUSES


def test_review_is_never_treated_as_terminal(mod):
    """review jobs gain images later, so they must stay out of both the
    resultless route and the skip cache."""
    src = _PLUGIN.read_text(encoding="utf-8")
    assert 'j.get("status") in ("completed", "failed")' in src, (
        "the consumed-jobs skip cache must cover failed but not review"
    )
