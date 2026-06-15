"""USGSDataUIManager / catalog integration tests (offline)."""

import pandas as pd
import pytest

from usgs_maps.usgsuimgr import (
    USGSDataReference,
    USGSDataReferenceReader,
    USGSDataUIManager,
)


def test_reference_from_catalog_row(fake_catalog):
    row = fake_catalog.iloc[0]
    ref = USGSDataReference.from_catalog_row(row)
    assert ref.site_no == row["site_no"]
    assert ref.param_cd == row["param_cd"]
    assert ref.duration_code == row["duration_code"]
    assert ref.name == f"{row['site_no']}/{row['param_cd']}/{row['duration_code']}"


def test_reference_requires_keys():
    try:
        USGSDataReference(site_no="11447650")  # missing param_cd, duration_code
    except ValueError as exc:
        assert "param_cd" in str(exc)
    else:
        raise AssertionError("expected ValueError for missing required attributes")


def test_manager_builds_catalog(fake_catalog, catalog_reader):
    mgr = USGSDataUIManager(fake_catalog, catalog_reader)
    cat = mgr.data_catalog
    assert len(cat._references) == len(fake_catalog)
    # lookup by name
    row = fake_catalog.iloc[0]
    name = f"{row['site_no']}/{row['param_cd']}/{row['duration_code']}"
    ref = cat.get(name)
    assert isinstance(ref, USGSDataReference)


def test_manager_get_data_reference_by_name(fake_catalog, catalog_reader):
    mgr = USGSDataUIManager(fake_catalog, catalog_reader)
    dfcat = mgr.data_catalog.to_dataframe().reset_index()
    row = dfcat.iloc[0]
    ref = mgr.get_data_reference(row)
    assert isinstance(ref, USGSDataReference)
    assert ref.name == row["name"]


def test_manager_loads_data_through_reference(fake_catalog, data_reader):
    # data_reader monkeypatches _fetch_raw; reuse it as the manager's reader.
    mgr = USGSDataUIManager(fake_catalog, data_reader)
    mgr.time_range = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03"))
    row = fake_catalog[fake_catalog["duration_code"] == "E"].iloc[0]
    ref = mgr.data_catalog.get(
        f"{row['site_no']}/{row['param_cd']}/{row['duration_code']}"
    )
    df = ref.getData(time_range=mgr.time_range)
    assert len(df) > 0
    assert df.attrs.get("unit") == row["unit"]


def test_load_empty_data_has_datetimeindex(fake_catalog, reader, monkeypatch):
    """Regression: load() must return DatetimeIndex even when there is no data.

    When read_station_data returns an empty DataFrame (no data for the requested
    window) the old code returned pd.DataFrame(columns=[...]) which has a
    RangeIndex.  dvue's _process_curve_data then crashed with:
        TypeError: '>=' not supported between instances of 'numpy.ndarray' and 'Timestamp'
    """
    from usgs_maps import usgs

    def _no_data(self, site_no, param_cd, duration_code, start, end):
        return pd.DataFrame(columns=["VALUE"])

    monkeypatch.setattr(usgs.Reader, "read_station_data", _no_data)

    dvue_reader = USGSDataReferenceReader(reader)
    row = fake_catalog.iloc[0]
    ref = USGSDataReference.from_catalog_row(row, reader=dvue_reader)
    time_range = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03"))
    df = ref.getData(time_range=time_range)

    # Must be a DatetimeIndex — never a RangeIndex
    assert isinstance(df.index, pd.DatetimeIndex), (
        f"Expected DatetimeIndex, got {type(df.index).__name__}"
    )
    assert len(df) == 0  # correctly empty
    assert df.attrs.get("unit") is not None


def test_process_curve_data_tolerates_range_index(fake_catalog, catalog_reader):
    """Regression: _process_curve_data must not crash on a non-DatetimeIndex frame.

    Even if a reader somehow returns a RangeIndex DataFrame, the dvue guard
    should log a warning and return the frame unchanged (not raise TypeError).
    """
    mgr = USGSDataUIManager(fake_catalog, catalog_reader)
    bad_df = pd.DataFrame({"value": [1.0, 2.0]})  # RangeIndex
    time_range = (pd.Timestamp("2024-01-01"), pd.Timestamp("2024-01-03"))
    row = fake_catalog.iloc[0].to_dict()
    # Must not raise
    result = mgr._process_curve_data(bad_df, row, time_range)
    assert result is not None
