"""Time-series retrieval / caching tests for usgs_maps (offline)."""

import os

import pandas as pd

from usgs_maps import usgs


def test_to_pacific_naive():
    idx = pd.date_range("2024-01-01 00:00", periods=3, freq="h", tz="UTC")
    out = usgs._to_pacific_naive(idx)
    assert out.tz is None
    # UTC 00:00 -> PST 16:00 previous day (UTC-8)
    assert out[0] == pd.Timestamp("2023-12-31 16:00")


def test_find_value_column():
    df = pd.DataFrame(columns=["00060", "00060_cd"])
    assert usgs._find_value_column(df, "00060") == "00060"
    df2 = pd.DataFrame(columns=["00060_Mean", "00060_Mean_cd"])
    assert usgs._find_value_column(df2, "00060") == "00060_Mean"
    df3 = pd.DataFrame(columns=["00065_cd"])
    assert usgs._find_value_column(df3, "00060") is None


def test_read_station_data_caches(data_reader):
    df = data_reader.read_station_data(
        "11447650", "00060", "E", "2024-01-01", "2024-01-03"
    )
    assert len(df) > 0
    assert df.index.tz is None  # tz-naive Pacific
    assert "VALUE" in df.columns
    # parquet cache file written
    path = data_reader.build_db_path("11447650", "00060", "E")
    assert os.path.exists(path)


def test_read_station_data_returns_window(data_reader):
    df = data_reader.read_station_data(
        "11447650", "00060", "E", "2024-01-01", "2024-01-05"
    )
    sub = data_reader.read_station_data(
        "11447650", "00060", "E", "2024-01-02", "2024-01-03"
    )
    assert sub.index.min() >= pd.Timestamp("2024-01-02")
    assert sub.index.max() <= pd.Timestamp("2024-01-03 23:59:59")
    assert len(sub) <= len(df)


def test_cache_gap_fill_extends_range(data_reader):
    # First fetch a small window, then a larger one — cache should extend.
    data_reader.read_station_data("11447650", "00060", "E", "2024-01-02", "2024-01-03")
    df_big = data_reader.read_station_data(
        "11447650", "00060", "E", "2024-01-01", "2024-01-05"
    )
    cached = data_reader.load_from_db("11447650", "00060", "E")
    assert cached.index.min() <= pd.Timestamp("2024-01-01 00:00")
    assert len(df_big) > 0


def test_remove_from_db(data_reader):
    data_reader.read_station_data("11447650", "00060", "E", "2024-01-01", "2024-01-02")
    path = data_reader.build_db_path("11447650", "00060", "E")
    assert os.path.exists(path)
    data_reader.remove_from_db("11447650", "00060", "E")
    assert not os.path.exists(path)
