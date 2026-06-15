"""Tests for station 11447650 (Sacramento R. at Freeport) — offline and live.

The live tests require ``--run-live`` and a working NWIS connection::

    pytest tests/test_live_station.py --run-live -v

They are designed to catch regressions in:
- ``_to_pacific_naive`` handling of PeriodIndex (DV daily data)
- ``_find_value_column`` detecting ``"00060_Mean"`` vs ``"00060_Mean_cd"``
- The Parquet cache round-trip for daily and instantaneous series
- ``read_station_data`` returning non-empty data in a recent window
"""

from __future__ import annotations

import pandas as pd
import numpy as np
import pytest

from usgs_maps import usgs

# ---------------------------------------------------------------------------
# Station under test
# ---------------------------------------------------------------------------
SITE = "11447650"          # Sacramento R. at Freeport, CA
PARAM_CD = "00060"         # Discharge (cfs)
DV_DURATION = "D"          # daily values
IV_DURATION = "E"          # instantaneous (15-min) values

LIVE_START = "2024-01-01"
LIVE_END = "2024-12-31"


# ===========================================================================
# Offline tests — no network access required
# ===========================================================================

class TestFindValueColumn:
    """_find_value_column covers both NWIS DV and IV column-naming conventions."""

    def test_exact_match(self):
        # IV response: column is exactly the param code
        df = pd.DataFrame(columns=["00060", "00060_cd"])
        assert usgs._find_value_column(df, "00060") == "00060"

    def test_dv_mean_suffix(self):
        # DV response: column is "00060_Mean" (stat 00003 = mean)
        df = pd.DataFrame(columns=["00060_Mean", "00060_Mean_cd"])
        assert usgs._find_value_column(df, "00060") == "00060_Mean"

    def test_exact_beats_prefixed(self):
        # When both exist, exact match wins
        df = pd.DataFrame(columns=["00060", "00060_Mean", "00060_Mean_cd"])
        assert usgs._find_value_column(df, "00060") == "00060"

    def test_qualifier_not_returned(self):
        # Qualifier columns (ending in _cd) must never be returned
        df = pd.DataFrame(columns=["00060_cd"])
        assert usgs._find_value_column(df, "00060") is None

    def test_wrong_param_not_returned(self):
        df = pd.DataFrame(columns=["00065", "00065_cd"])
        assert usgs._find_value_column(df, "00060") is None

    def test_dv_other_stat_suffix(self):
        # Some NWIS DV responses use "Maximum" / "Minimum"
        df = pd.DataFrame(columns=["00060_Maximum", "00060_Maximum_cd"])
        assert usgs._find_value_column(df, "00060") == "00060_Maximum"


class TestToPacificNaive:
    """_to_pacific_naive handles UTC DatetimeIndex, tz-naive index, and PeriodIndex."""

    def test_utc_converted_to_pacific(self):
        idx = pd.date_range("2024-01-01 08:00", periods=4, freq="h", tz="UTC")
        out = usgs._to_pacific_naive(idx)
        assert out.tz is None
        # UTC 08:00 = PST 00:00 (UTC-8)
        assert out[0] == pd.Timestamp("2024-01-01 00:00")

    def test_tz_naive_unchanged(self):
        idx = pd.date_range("2024-01-01", periods=5, freq="D")
        out = usgs._to_pacific_naive(idx)
        assert out.tz is None
        assert list(out) == list(idx)

    def test_period_index_daily(self):
        """DV data from nwis.get_dv may return a PeriodIndex ('D' frequency).

        The old implementation did ``pd.DatetimeIndex(period_index)`` which
        in pandas ≥ 2.0 raises TypeError.  The fix calls ``.to_timestamp()``
        first.
        """
        pidx = pd.period_range("2024-01-01", periods=5, freq="D")
        # Must not raise
        out = usgs._to_pacific_naive(pidx)
        assert isinstance(out, pd.DatetimeIndex)
        assert out.tz is None
        assert len(out) == 5
        assert out[0] == pd.Timestamp("2024-01-01")

    def test_period_index_utc_equivalent(self):
        """PeriodIndex does not carry timezone; result is midnight-aligned."""
        pidx = pd.period_range("2024-06-01", periods=3, freq="D")
        out = usgs._to_pacific_naive(pidx)
        assert out[0] == pd.Timestamp("2024-06-01 00:00")


class TestFetchRawOffline:
    """_fetch_raw offline — monkeypatched nwis returns synthetic DV frames."""

    def _make_dv_frame(self, site_no, param_cd, start, end, use_period_index=False):
        """Return a frame shaped like ``nwis.get_dv`` with ``multi_index=False``."""
        if use_period_index:
            idx = pd.period_range(start, end, freq="D")
        else:
            idx = pd.date_range(start, end, freq="D", tz="UTC")
        vals = np.random.uniform(1000, 5000, size=len(idx))
        return pd.DataFrame(
            {f"{param_cd}_Mean": vals, f"{param_cd}_Mean_cd": ["A"] * len(idx)},
            index=idx,
        )

    def test_dv_utc_datetimeindex(self, reader, monkeypatch):
        """DV data with a UTC DatetimeIndex is correctly converted."""
        frame = self._make_dv_frame(SITE, PARAM_CD, "2024-01-01", "2024-01-10")

        def _fake_get_dv(**kw):
            return frame, {}

        import dataretrieval.nwis as nwis_mod
        monkeypatch.setattr(nwis_mod, "get_dv", lambda **kw: _fake_get_dv(**kw))

        out = reader._fetch_raw(SITE, PARAM_CD, DV_DURATION, "2024-01-01", "2024-01-10")
        assert isinstance(out.index, pd.DatetimeIndex)
        assert out.index.tz is None
        assert "VALUE" in out.columns
        assert len(out) == len(frame)

    def test_dv_period_index(self, reader, monkeypatch):
        """DV data with a PeriodIndex (some dataretrieval versions) is handled."""
        frame = self._make_dv_frame(
            SITE, PARAM_CD, "2024-01-01", "2024-01-10", use_period_index=True
        )

        import dataretrieval.nwis as nwis_mod
        monkeypatch.setattr(nwis_mod, "get_dv", lambda **kw: (frame, {}))

        out = reader._fetch_raw(SITE, PARAM_CD, DV_DURATION, "2024-01-01", "2024-01-10")
        assert isinstance(out.index, pd.DatetimeIndex)
        assert out.index.tz is None
        assert len(out) == len(frame)

    def test_dv_fallback_when_get_dv_empty(self, reader, monkeypatch):
        """When nwis.get_dv returns empty, _fetch_raw falls back to IV+daily-resample."""
        import dataretrieval.nwis as nwis_mod

        empty_dv = pd.DataFrame({"site_no": pd.Series(dtype=str)})
        monkeypatch.setattr(nwis_mod, "get_dv", lambda **kw: (empty_dv, {}))

        iv_idx = pd.date_range("2024-01-01", "2024-01-04", freq="15min", tz="UTC")
        iv_vals = np.random.uniform(1000, 5000, len(iv_idx))
        iv_frame = pd.DataFrame(
            {PARAM_CD: iv_vals, f"{PARAM_CD}_cd": ["A"] * len(iv_idx)},
            index=iv_idx,
        )
        monkeypatch.setattr(nwis_mod, "get_iv", lambda **kw: (iv_frame, {}))

        out = reader._fetch_raw(SITE, PARAM_CD, DV_DURATION, "2024-01-01", "2024-01-03")
        # UTC "2024-01-01 00:00" = PST "2023-12-31 16:00" so resample to "D"
        # can produce an extra 2023-12-31 bucket; allow 3 or 4 rows.
        assert len(out) >= 3, f"Expected ≥3 daily rows, got {len(out)}"
        assert isinstance(out.index, pd.DatetimeIndex)
        assert out.index.tz is None
        assert "VALUE" in out.columns
        assert out["VALUE"].notna().all()

    def test_missing_column_returns_empty(self, reader, monkeypatch):
        """If the response has no recognisable value column, return empty."""
        import dataretrieval.nwis as nwis_mod

        frame = pd.DataFrame(
            {"some_other_col": [1.0], "some_other_col_cd": ["A"]},
            index=pd.date_range("2024-01-01", periods=1, tz="UTC"),
        )
        monkeypatch.setattr(nwis_mod, "get_dv", lambda **kw: (frame, {}))
        # Also patch get_iv so the fallback doesn't hit live NWIS
        monkeypatch.setattr(nwis_mod, "get_iv", lambda **kw: (pd.DataFrame(), {}))

        out = reader._fetch_raw(SITE, PARAM_CD, DV_DURATION, "2024-01-01", "2024-01-02")
        assert len(out) == 0
        assert "VALUE" in out.columns

    def test_empty_nwis_response(self, reader, monkeypatch):
        """When both get_dv AND get_iv return empty, _fetch_raw returns an empty frame."""
        import dataretrieval.nwis as nwis_mod
        monkeypatch.setattr(nwis_mod, "get_dv", lambda **kw: (pd.DataFrame(), {}))
        monkeypatch.setattr(nwis_mod, "get_iv", lambda **kw: (pd.DataFrame(), {}))

        out = reader._fetch_raw(SITE, PARAM_CD, DV_DURATION, "2024-01-01", "2024-01-02")
        assert len(out) == 0
        assert "VALUE" in out.columns


# ===========================================================================
# Live tests — require --run-live flag
# ===========================================================================

@pytest.mark.live
def test_live_get_dv_raw_columns(reader):
    """Check exact column names returned by nwis.get_dv for 11447650/00060.

    If get_dv returns empty (the DV endpoint is decommissioned in newer
    dataretrieval), the test checks that _fetch_raw's IV fallback produces
    valid daily data instead.
    """
    from dataretrieval import nwis

    with usgs._ignore_dataretrieval_warnings():
        df, _ = nwis.get_dv(
            sites=SITE, start="2024-01-01", end="2024-01-31",
            parameterCd=PARAM_CD, statCd="00003", multi_index=False,
        )
    if df is not None and len(df) > 0:
        print(f"\nnwis.get_dv columns: {list(df.columns)}")
        print(f"nwis.get_dv index type: {type(df.index).__name__}")
        value_col = usgs._find_value_column(df, PARAM_CD)
        assert value_col is not None, (
            f"_find_value_column could not find '{PARAM_CD}' in: {list(df.columns)}"
        )
    else:
        # DV endpoint decommissioned — verify the IV fallback path works
        print("\nnwis.get_dv empty: testing IV-fallback path")
        out = reader._fetch_raw(SITE, PARAM_CD, DV_DURATION, "2024-01-01", "2024-01-31")
        assert len(out) > 0, "IV fallback also returned empty data"
        assert isinstance(out.index, pd.DatetimeIndex)


@pytest.mark.live
def test_live_fetch_raw_dv(reader):
    """_fetch_raw for daily discharge must return a non-empty tz-naive frame."""
    out = reader._fetch_raw(SITE, PARAM_CD, DV_DURATION, LIVE_START, LIVE_END)
    assert len(out) > 200, (
        f"Expected >200 daily rows for {SITE}/{PARAM_CD}/D, got {len(out)}"
    )
    assert isinstance(out.index, pd.DatetimeIndex), (
        f"Expected DatetimeIndex, got {type(out.index).__name__}"
    )
    assert out.index.tz is None, "Index must be tz-naive Pacific"
    assert "VALUE" in out.columns
    assert out["VALUE"].notna().sum() > 100


@pytest.mark.live
def test_live_read_station_data_dv(tmp_path):
    """End-to-end: read_station_data for 11447650/00060/D returns data."""
    r = usgs.Reader(dbase_dir=str(tmp_path / "usgs_db"))
    df = r.read_station_data(SITE, PARAM_CD, DV_DURATION, LIVE_START, LIVE_END)
    assert len(df) > 200, (
        f"Expected >200 rows for {SITE}/{PARAM_CD}/D, got {len(df)}"
    )
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is None
    # Parquet cache file must have been written
    cache_path = r.build_db_path(SITE, PARAM_CD, DV_DURATION)
    import os
    assert os.path.exists(cache_path)
    cached = r.load_from_db(SITE, PARAM_CD, DV_DURATION)
    assert len(cached) >= len(df)


@pytest.mark.live
def test_live_read_station_data_iv(tmp_path):
    """End-to-end: read_station_data for 11447650/00060/E returns data."""
    r = usgs.Reader(dbase_dir=str(tmp_path / "usgs_db"))
    # Small window to keep the test fast
    df = r.read_station_data(SITE, PARAM_CD, IV_DURATION, "2024-01-01", "2024-01-07")
    assert len(df) > 100, (
        f"Expected >100 15-min rows for {SITE}/{PARAM_CD}/E, got {len(df)}"
    )
    assert isinstance(df.index, pd.DatetimeIndex)
    assert df.index.tz is None


@pytest.mark.live
def test_live_existing_cache_not_empty(reader):
    """If the cache file already exists for 11447650/00060/D, it must not be 0 rows.

    An empty cache file indicates the file was built during the
    _ignore_dataretrieval_warnings bug (before the contextmanager fix).
    Delete it manually and re-run build-cache if this test fails.
    """
    import os
    cache_path = reader.build_db_path(SITE, PARAM_CD, DV_DURATION)
    if not os.path.exists(cache_path):
        pytest.skip(f"Cache file not found: {cache_path}")
    df = reader.load_from_db(SITE, PARAM_CD, DV_DURATION)
    assert len(df) > 0, (
        f"Cache file {cache_path} exists but is empty. "
        "It was likely written during a bug. Delete usgs_db/ and re-run build-cache."
    )
