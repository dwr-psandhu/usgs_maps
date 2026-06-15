"""Shared pytest fixtures for usgs_maps tests.

All fixtures here are offline: network access is monkeypatched so the test
suite runs without contacting the USGS NWIS web services.  Tests that *do*
require the network are marked ``@pytest.mark.live`` and skipped by default
(see ``setup.cfg``); run them explicitly with ``pytest -m live``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from usgs_maps import usgs


# ---------------------------------------------------------------------------
# Live-network test gating
# ---------------------------------------------------------------------------

def pytest_addoption(parser):
    parser.addoption(
        "--run-live", action="store_true", default=False,
        help="run tests marked 'live' that require network access to USGS NWIS",
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-live"):
        return
    skip_live = pytest.mark.skip(reason="needs --run-live (live NWIS network access)")
    for item in items:
        if "live" in item.keywords:
            item.add_marker(skip_live)


# ---------------------------------------------------------------------------
# Fake NWIS responses
# ---------------------------------------------------------------------------

def _fake_site_catalog_df():
    """A minimal NWIS series-catalog frame (as get_info would return)."""
    return pd.DataFrame(
        {
            "agency_cd": ["USGS", "USGS", "USGS"],
            "site_no": ["11447650", "11447650", "11303500"],
            "station_nm": [
                "SACRAMENTO R A FREEPORT CA",
                "SACRAMENTO R A FREEPORT CA",
                "SAN JOAQUIN R NR VERNALIS CA",
            ],
            "site_tp_cd": ["ST", "ST", "ST"],
            "dec_lat_va": [38.4557, 38.4557, 37.6764],
            "dec_long_va": [-121.5008, -121.5008, -121.2660],
            "huc_cd": ["18020111", "18020111", "18040003"],
            "parm_cd": ["00060", "00060", "00065"],
            "stat_cd": ["", "00003", ""],
            "data_type_cd": ["uv", "dv", "uv"],
            "begin_date": ["2007-10-01", "2007-10-01", "1996-01-01"],
            "end_date": ["2024-09-30", "2024-09-30", "2024-09-30"],
        }
    )


def _fake_iv_series(site_no, param_cd, start, end):
    """A tz-aware (UTC) instantaneous-value frame like nwis.get_iv returns."""
    idx = pd.date_range(start, end, freq="15min", tz="UTC")
    vals = np.linspace(1000.0, 2000.0, len(idx))
    return pd.DataFrame({param_cd: vals, f"{param_cd}_cd": ["A"] * len(idx)}, index=idx)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def reader(tmp_path):
    """A :class:`usgs.Reader` writing its cache under a temp directory."""
    return usgs.Reader(dbase_dir=str(tmp_path / "usgs_db"))


@pytest.fixture
def catalog_reader(reader, monkeypatch):
    """Reader whose site-service query returns the fake catalog frame."""

    def _fake_fetch(self, nwis, param_cds, site_types, **major_filter):
        return _fake_site_catalog_df()

    monkeypatch.setattr(usgs.Reader, "_fetch_site_catalog", _fake_fetch)
    return reader


@pytest.fixture
def data_reader(reader, monkeypatch):
    """Reader whose raw IV/DV fetch returns synthetic tz-naive Pacific data."""

    def _fake_fetch_raw(self, site_no, param_cd, duration_code, start, end):
        idx = pd.date_range(start, end, freq="15min", tz="UTC")
        if len(idx) == 0:
            return pd.DataFrame(columns=["VALUE"])
        vals = np.linspace(1000.0, 2000.0, len(idx))
        out = pd.DataFrame({"VALUE": vals}, index=idx)
        out.index = usgs._to_pacific_naive(out.index, self.output_tz)
        out.index.name = "datetime"
        out = out[~out.index.duplicated(keep="last")].sort_index()
        return out

    monkeypatch.setattr(usgs.Reader, "_fetch_raw", _fake_fetch_raw)
    return reader


@pytest.fixture
def fake_catalog(catalog_reader):
    """A built (and saved) catalog GeoDataFrame from the fake site service."""
    return catalog_reader.save_all_stations_info(state_cds=["ca"])
