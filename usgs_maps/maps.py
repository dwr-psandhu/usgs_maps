"""Geospatial helpers for usgs_maps.

Lightweight utilities for filtering the USGS station catalog by a lat/lon
bounding box or an arbitrary polygon (GeoJSON), and for converting plain
lat/lon tables into GeoDataFrames.
"""

from __future__ import annotations

import geopandas as gpd


def convert_to_gpd(df, lat_col="dec_lat_va", lon_col="dec_long_va", crs="EPSG:4326"):
    """Convert a DataFrame with lat/lon columns into a WGS84 GeoDataFrame."""
    return gpd.GeoDataFrame(
        df.copy(),
        geometry=gpd.points_from_xy(df[lon_col], df[lat_col], crs=crs),
    )


def filter_bbox(catalog, bbox, lat_col="dec_lat_va", lon_col="dec_long_va"):
    """Filter ``catalog`` to rows whose point falls inside ``bbox``.

    ``bbox`` is ``"west,south,east,north"`` (str) or a 4-sequence of floats.
    """
    if isinstance(bbox, str):
        west, south, east, north = (float(v) for v in bbox.split(","))
    else:
        west, south, east, north = (float(v) for v in bbox)
    lat = catalog[lat_col]
    lon = catalog[lon_col]
    mask = (lon >= west) & (lon <= east) & (lat >= south) & (lat <= north)
    return catalog[mask].copy()


def station_within_polygon(catalog, polygon_file):
    """Filter ``catalog`` (a GeoDataFrame) to points inside a GeoJSON polygon."""
    poly = gpd.read_file(polygon_file).to_crs("EPSG:4326")
    if not isinstance(catalog, gpd.GeoDataFrame):
        catalog = convert_to_gpd(catalog)
    catalog = catalog.to_crs("EPSG:4326")
    joined = gpd.sjoin(catalog, poly[["geometry"]], predicate="within", how="inner")
    return joined.drop(columns=[c for c in joined.columns if c.startswith("index_right")])
