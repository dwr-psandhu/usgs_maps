"""Catalog-building tests for usgs_maps (offline)."""

import geopandas as gpd
import pandas as pd

from usgs_maps import usgs


def test_param_helpers():
    assert usgs.param_name("00060") == "Discharge"
    assert usgs.param_unit("00060") == "cfs"
    # Unknown codes fall back to the bare code with an empty unit.
    assert usgs.param_name("99999") == "99999"
    assert usgs.param_unit("99999") == ""


def test_duration_mapping():
    assert usgs.duration_from_data_type("uv") == "E"
    assert usgs.duration_from_data_type("iv") == "E"
    assert usgs.duration_from_data_type("dv") == "D"
    assert usgs.duration_from_data_type("xx") == ""


def test_read_sites_catalog_columns(catalog_reader):
    catalog = catalog_reader.read_sites_catalog(state_cds=["ca"])
    assert isinstance(catalog, gpd.GeoDataFrame)
    assert len(catalog) == 3
    for col in usgs.CATALOG_COLUMNS:
        assert col in catalog.columns
    # site_no must be a string to preserve leading zeros
    assert catalog["site_no"].map(type).eq(str).all()
    # duration codes only E/D
    assert set(catalog["duration_code"]).issubset({"E", "D"})
    # geometry present and WGS84
    assert str(catalog.crs) == "EPSG:4326"


def test_read_sites_catalog_requires_filter(reader):
    try:
        reader.read_sites_catalog()
    except ValueError as exc:
        assert "state_cds or bbox" in str(exc)
    else:
        raise AssertionError("expected ValueError when no filter given")


def test_save_and_read_saved_catalog(catalog_reader):
    saved = catalog_reader.save_all_stations_info(state_cds=["ca"])
    assert len(saved) == 3
    reloaded = catalog_reader.read_saved_stations_info()
    assert isinstance(reloaded, gpd.GeoDataFrame)
    assert len(reloaded) == 3
    # leading-zero preservation across CSV roundtrip
    assert reloaded["site_no"].map(type).eq(str).all()
    assert "11303500" in set(reloaded["site_no"])


def test_bbox_filter():
    df = pd.DataFrame(
        {
            "dec_lat_va": [38.0, 30.0],
            "dec_long_va": [-121.0, -100.0],
            "site_no": ["a", "b"],
        }
    )
    filtered = usgs._filter_bbox(df, "-122,37,-120,39")
    assert list(filtered["site_no"]) == ["a"]


def test_bbox_to_str_validation():
    assert usgs._bbox_to_str([-122, 37, -120, 39]) == "-122,37,-120,39"
    try:
        usgs._bbox_to_str([1, 2, 3])
    except ValueError:
        pass
    else:
        raise AssertionError("expected ValueError for malformed bbox")


def test_ignore_dataretrieval_warnings_reentrant():
    # Regression: the old implementation manually called __enter__() inside
    # the function body, so the `with` statement called __enter__() a second
    # time → "Cannot enter catch_warnings() twice".  Both calls must succeed.
    import warnings
    with usgs._ignore_dataretrieval_warnings():
        pass  # first use
    with usgs._ignore_dataretrieval_warnings():
        pass  # second use — would have crashed before the fix


def test_fetch_site_catalog_called_twice(catalog_reader):
    # Simulate querying two states in one cache-build run.
    # _fetch_site_catalog calls _ignore_dataretrieval_warnings each time;
    # with the broken implementation the second call raised
    # "Cannot enter catch_warnings() twice".
    catalog = catalog_reader.read_sites_catalog(state_cds=["ca", "nv"])
    assert len(catalog) > 0
