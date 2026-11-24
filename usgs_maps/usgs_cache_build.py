"""Cache builder for usgs_maps.

Builds the USGS NWIS station/series catalog (filtered by state code(s) and/or
a lat/lon bounding box) and optionally pre-downloads time series for every
catalog row into the local Parquet cache.

Configuration can be supplied either as keyword arguments or via a YAML/JSON
config file (see :func:`load_build_config`).  Example YAML::

    states: [ca, nv]
    bbox: "-122.5,37.5,-121.0,38.5"   # optional; post-filters when states set
    param_cds: ["00060", "00065"]
    site_types: ST                     # optional NWIS siteType filter
    start: "2020-01-01"                # optional; for series pre-download
    end: "2024-12-31"
    download: true                     # download series, not just the catalog
"""

from __future__ import annotations

import json
import logging
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

import pandas as pd
import tqdm

from . import build_job, usgs

logger = logging.getLogger(__name__)


def load_build_config(config_path: str) -> dict:
    """Load a YAML or JSON cache-build config file into a dict."""
    with open(config_path, "r", encoding="utf-8") as fh:
        text = fh.read()
    if config_path.lower().endswith((".yml", ".yaml")):
        import yaml

        return yaml.safe_load(text) or {}
    return json.loads(text)


CATALOG_REFRESH_INTERVAL = 7 * 24 * 60 * 60


def build_catalog(
    reader,
    state_cds=None,
    bbox=None,
    param_cds=None,
    site_types=None,
    force_refresh=False,
):
    """Build the catalog, falling back to the last good saved catalog."""
    logger.info(
        "build_catalog: states=%s bbox=%s params=%s",
        state_cds, bbox, param_cds or usgs.DEFAULT_PARAM_CDS,
    )
    catalog_path = reader.catalog_csv_path()
    if (
        not force_refresh
        and os.path.exists(catalog_path)
        and time.time() - os.path.getmtime(catalog_path)
        <= CATALOG_REFRESH_INTERVAL
    ):
        catalog = reader.read_saved_stations_info()
        if len(catalog):
            logger.info(
                "Catalog is current; using %d saved rows from %s",
                len(catalog),
                catalog_path,
            )
            return catalog
    try:
        return reader.save_all_stations_info(
            state_cds=state_cds,
            bbox=bbox,
            param_cds=param_cds,
            site_types=site_types,
        )
    except RuntimeError as exc:
        try:
            catalog = reader.read_saved_stations_info()
        except Exception:
            raise exc
        if len(catalog) == 0:
            raise exc
        logger.warning(
            "Catalog refresh failed; using %d rows from %s: %s",
            len(catalog),
            reader.catalog_csv_path(),
            exc,
        )
        return catalog


def download_all_series(reader, catalog, start=None, end=None, heartbeat=None):
    """Download every catalog series into the Parquet cache (gap-filling).

    Returns ``{"ok", "failed", "failed_items"}``; *heartbeat* (if given) is called
    after each series so a build lock stays fresh.
    """
    if end is None:
        end = pd.Timestamp.now().strftime("%Y-%m-%d")
    if start is None:
        # Default to two years of history when not specified.
        start = (pd.Timestamp(end) - pd.DateOffset(years=2)).strftime("%Y-%m-%d")

    def _download(row):
        try:
            reader.read_station_data(
                str(row["site_no"]),
                str(row["param_cd"]),
                str(row["duration_code"]),
                start,
                end,
            )
            return True, None
        except Exception as exc:
            logger.warning(
                "download failed for %s/%s/%s: %s",
                row.get("site_no"), row.get("param_cd"), row.get("duration_code"), exc,
            )
            return False, (
                f"{row.get('site_no')}/{row.get('param_cd')}/"
                f"{row.get('duration_code')}"
            )

    n_ok = n_fail = 0
    failed_items = []
    max_workers = 4
    rows = [row for _, row in catalog.iterrows()]
    with ThreadPoolExecutor(max_workers=max_workers) as executor:
        futures = [executor.submit(_download, row) for row in rows]
        progress = tqdm.tqdm(
            as_completed(futures),
            total=len(futures),
            desc="usgs",
            mininterval=30,
        )
        for future in progress:
            try:
                ok, failed_item = future.result()
            except Exception as exc:
                ok, failed_item = False, None
                logger.warning("download worker failed unexpectedly: %s", exc)
            if ok:
                n_ok += 1
            else:
                n_fail += 1
                if failed_item:
                    failed_items.append(failed_item)
            if heartbeat:
                heartbeat()
    logger.info("download_all_series: %d ok, %d failed", n_ok, n_fail)
    return {"ok": n_ok, "failed": n_fail, "failed_items": failed_items}


def run_build(
    dbase_dir="usgs_db",
    state_cds=None,
    bbox=None,
    param_cds=None,
    site_types=None,
    start=None,
    end=None,
    download=True,
    config=None,
):
    """Top-level cache build: catalog (+ optional series download).

    Parameters mirror the CLI flags; ``config`` (a path) overrides defaults and
    is itself overridden by any explicitly-passed keyword arguments.
    """
    cfg = {}
    if config:
        cfg = load_build_config(config)

    state_cds = state_cds if state_cds else cfg.get("states") or cfg.get("state_cds")
    bbox = bbox if bbox else cfg.get("bbox")
    param_cds = param_cds if param_cds else cfg.get("param_cds")
    site_types = site_types if site_types else cfg.get("site_types")
    start = start if start else cfg.get("start")
    end = end if end else cfg.get("end")
    if config and "download" in cfg and download is True:
        download = bool(cfg.get("download"))

    if not state_cds and not bbox:
        raise ValueError("Cache build requires at least one of --state or --bbox")

    reader = usgs.Reader(dbase_dir=dbase_dir)
    built = {}

    def build(heartbeat):
        built["catalog"] = build_catalog(reader, state_cds, bbox, param_cds, site_types)
        heartbeat()
        if not download:
            return {}
        return download_all_series(reader, built["catalog"], start, end, heartbeat)

    build_job.run_locked_build(dbase_dir, build, name="usgs")
    return built["catalog"]
