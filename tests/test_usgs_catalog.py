"""Catalog-building tests for usgs_maps (offline)."""

import geopandas as gpd
import pandas as pd
import pytest

from usgs_maps import build_job, usgs, usgs_cache_build


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


class TestCatalogProtection:
    """A failed NWIS query must never replace the last good catalog."""

    @staticmethod
    def _empty_catalog_result(monkeypatch):
        # What read_sites_catalog returns when every NWIS query failed or matched nothing.
        monkeypatch.setattr(
            usgs.Reader, "read_sites_catalog", lambda self, **kw: self._empty_catalog()
        )

    def test_empty_result_keeps_existing_catalog(self, reader, monkeypatch):
        good = pd.DataFrame({"site_no": ["11447650"], "param_cd": ["00060"]})
        good.to_csv(reader.catalog_csv_path(), index=False)
        before = open(reader.catalog_csv_path()).read()
        self._empty_catalog_result(monkeypatch)

        with pytest.raises(RuntimeError, match="no catalog rows"):
            reader.save_all_stations_info(state_cds=["ca"])

        assert open(reader.catalog_csv_path()).read() == before

    def test_build_with_failed_query_fails_and_records_status(self, tmp_path, monkeypatch):
        dbase = str(tmp_path / "usgs_db")
        self._empty_catalog_result(monkeypatch)

        with pytest.raises(RuntimeError):
            usgs_cache_build.run_build(dbase_dir=dbase, state_cds=["ca"])

        assert build_job.read_status(dbase)["state"] == "failed"

    def test_build_uses_saved_catalog_after_refresh_failure(
        self, catalog_reader, monkeypatch
    ):
        expected = catalog_reader.save_all_stations_info(state_cds=["ca"])
        self._empty_catalog_result(monkeypatch)

        actual = usgs_cache_build.build_catalog(
            catalog_reader, state_cds=["ca"]
        )

        assert len(actual) == len(expected) == 3

    def test_build_reuses_current_saved_catalog_without_nwis_call(
        self, catalog_reader, monkeypatch
    ):
        catalog_reader.save_all_stations_info(state_cds=["ca"])

        def unexpected(*args, **kwargs):
            raise AssertionError("fresh saved catalog should avoid NWIS")

        monkeypatch.setattr(
            usgs.Reader, "save_all_stations_info", unexpected
        )

        actual = usgs_cache_build.build_catalog(
            catalog_reader, state_cds=["ca"]
        )

        assert len(actual) == 3

    def test_dropped_response_is_retried(self, reader, monkeypatch):
        monkeypatch.setattr(usgs, "CATALOG_QUERY_RETRY_DELAY", 0)
        calls = []

        class FakeNwis:
            @staticmethod
            def get_info(**kwargs):
                calls.append(1)
                if len(calls) < 3:
                    raise ConnectionError("Response ended prematurely")
                return pd.DataFrame({"site_no": ["1"]}), None

        out = reader._fetch_site_catalog(FakeNwis, ["00060"], None, stateCd="ca")

        assert len(calls) == 3
        assert list(out["site_no"]) == ["1"]

    def test_gives_up_after_all_attempts(self, reader, monkeypatch):
        monkeypatch.setattr(usgs, "CATALOG_QUERY_RETRY_DELAY", 0)
        calls = []

        class FakeNwis:
            @staticmethod
            def get_info(**kwargs):
                calls.append(1)
                raise ConnectionError("Response ended prematurely")

        assert reader._fetch_site_catalog(FakeNwis, ["00060"], None, stateCd="ca") is None
        assert len(calls) == usgs.CATALOG_QUERY_ATTEMPTS


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
