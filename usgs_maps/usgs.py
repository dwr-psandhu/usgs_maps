"""USGS NWIS data-access layer for usgs_maps.

This module wraps the official `dataretrieval <https://github.com/DOI-USGS/dataretrieval-python>`_
library to:

* build a **station/series catalog** from the NWIS Site Service
  (:func:`dataretrieval.nwis.get_info` with ``seriesCatalogOutput=True``),
  optionally filtered by one or more state codes and/or a lat/lon bounding box;
* retrieve **Instantaneous Values** (IV, ~15-min) and **Daily Values** (DV)
  time series for a single site/parameter, with a local Parquet cache and
  gap-fill-on-read (mirroring the ``cdec_maps`` design).

Conventions
-----------
* ``site_no`` is always kept as a *string* to preserve leading zeros.
* Output time series use a tz-naive ``DatetimeIndex`` in Pacific local time
  (UTC is converted to ``America/Los_Angeles`` then the tz is dropped), to
  match the vtools tidal-filter expectations used downstream.
* Catalog geometry is WGS84 (``EPSG:4326``).
"""

from __future__ import annotations

import logging
import os
import warnings
from contextlib import contextmanager

import param
import pandas as pd

logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)


# ---------------------------------------------------------------------------
# Parameter metadata
# ---------------------------------------------------------------------------

#: Parameter code -> (human-readable name, unit).  Covers the default display
#: set plus a few common extras.  Codes not listed fall back to the bare code.
PARAM_INFO = {
    "00060": ("Discharge", "cfs"),
    "00065": ("Gage height", "ft"),
    "00010": ("Temperature, water", "deg C"),
    "00095": ("Specific conductance", "uS/cm"),
    "00400": ("pH", "std units"),
    "63680": ("Turbidity", "FNU"),
    "00045": ("Precipitation", "in"),
    "00300": ("Dissolved oxygen", "mg/l"),
    "00480": ("Salinity", "ppt"),
    "72019": ("Depth to water level", "ft"),
}

#: Default parameter codes displayed by the dashboard / built into the cache.
DEFAULT_PARAM_CDS = ["00060", "00065", "00010", "00095", "00400", "63680"]

#: NWIS ``data_type_cd`` (from ``seriesCatalogOutput``) -> internal duration code.
#: ``uv`` = unit (instantaneous) values; ``dv`` = daily values.
_DATA_TYPE_TO_DURATION = {"uv": "E", "iv": "E", "dv": "D"}

#: Internal duration code -> human-readable label.
DURATION_LABEL = {"E": "instantaneous", "D": "daily"}

#: Default statistic code for Daily Values (``00003`` = mean).
DEFAULT_STAT_CD = "00003"

#: Output timezone for retrieved series (Option A: convert UTC -> Pacific, drop tz).
OUTPUT_TZ = "America/Los_Angeles"

#: Canonical catalog columns produced by :meth:`Reader.read_sites_catalog`.
CATALOG_COLUMNS = [
    "site_no",
    "station_nm",
    "param_cd",
    "param_name",
    "unit",
    "duration_code",
    "duration",
    "stat_cd",
    "data_type_cd",
    "begin_date",
    "end_date",
    "dec_lat_va",
    "dec_long_va",
    "site_tp_cd",
    "huc_cd",
    "agency_cd",
    "source",
]


def ensure_dir_exists(directory: str) -> None:
    if not os.path.exists(directory):
        os.makedirs(directory)


def param_name(param_cd: str) -> str:
    """Human-readable name for a parameter code (falls back to the code)."""
    return PARAM_INFO.get(str(param_cd), (str(param_cd), ""))[0]


def param_unit(param_cd: str) -> str:
    """Unit string for a parameter code (falls back to empty string)."""
    return PARAM_INFO.get(str(param_cd), (str(param_cd), ""))[1]


def duration_from_data_type(data_type_cd: str) -> str:
    """Map an NWIS ``data_type_cd`` to the internal duration code (E/D)."""
    return _DATA_TYPE_TO_DURATION.get(str(data_type_cd).lower(), "")


@contextmanager
def _ignore_dataretrieval_warnings():
    """Context manager that silences dataretrieval's deprecation/qw warnings."""
    with warnings.catch_warnings():
        warnings.simplefilter("ignore", DeprecationWarning)
        warnings.simplefilter("ignore", UserWarning)
        yield


class Reader(param.Parameterized):
    """Reads the USGS NWIS site catalog and time series via ``dataretrieval``.

    Parameters
    ----------
    dbase_dir : str
        Directory for the local Parquet cache and saved catalog CSV.
    output_tz : str
        Target timezone for retrieved series; the tz is dropped after the
        conversion so the returned index is tz-naive Pacific local time.
    stat_cd : str
        Statistic code used for Daily Values (default ``"00003"`` = mean).
    """

    dbase_dir = param.String(default="usgs_db", allow_None=False)
    output_tz = param.String(default=OUTPUT_TZ, allow_None=False)
    stat_cd = param.String(default=DEFAULT_STAT_CD, allow_None=False)

    def __init__(self, dbase_dir="usgs_db", output_tz=OUTPUT_TZ, stat_cd=DEFAULT_STAT_CD):
        super().__init__(dbase_dir=dbase_dir, output_tz=output_tz, stat_cd=stat_cd)
        ensure_dir_exists(self.dbase_dir)

    # ------------------------------------------------------------------
    # Catalog
    # ------------------------------------------------------------------

    def read_sites_catalog(
        self,
        state_cds=None,
        bbox=None,
        param_cds=None,
        site_types=None,
    ):
        """Build a station/series catalog from the NWIS Site Service.

        At least one of ``state_cds`` or ``bbox`` must be supplied.

        Parameters
        ----------
        state_cds : str or list of str, optional
            One or more 2-letter US state codes (e.g. ``"ca"`` or
            ``["ca", "nv"]``).  NWIS allows only one state per request, so
            each is queried separately and the results concatenated.
        bbox : str or sequence of float, optional
            Lat/lon bounding box ``"west,south,east,north"`` (decimal
            degrees).  When both ``state_cds`` and ``bbox`` are given, the
            box is used to post-filter the state-derived catalog.
        param_cds : list of str, optional
            Parameter codes to keep (default :data:`DEFAULT_PARAM_CDS`).
        site_types : str or list of str, optional
            NWIS ``siteType`` filter (e.g. ``"ST"`` for streams).  Defaults
            to no site-type restriction.

        Returns
        -------
        geopandas.GeoDataFrame
            One row per (site, parameter, duration) series, with the columns
            in :data:`CATALOG_COLUMNS` plus a WGS84 ``geometry`` column.
        """
        from dataretrieval import nwis

        if not state_cds and not bbox:
            raise ValueError(
                "read_sites_catalog requires at least one of state_cds or bbox"
            )
        if param_cds is None:
            param_cds = list(DEFAULT_PARAM_CDS)
        param_cds = [str(p) for p in param_cds]
        bbox_str = _bbox_to_str(bbox) if bbox is not None else None

        frames = []
        if state_cds:
            if isinstance(state_cds, str):
                state_cds = [state_cds]
            for state in state_cds:
                frames.append(
                    self._fetch_site_catalog(
                        nwis, param_cds, site_types, stateCd=state
                    )
                )
        elif bbox_str is not None:
            frames.append(
                self._fetch_site_catalog(
                    nwis, param_cds, site_types, bBox=bbox_str
                )
            )

        frames = [f for f in frames if f is not None and len(f)]
        if not frames:
            logger.warning("read_sites_catalog: no sites returned")
            return self._empty_catalog()

        raw = pd.concat(frames, ignore_index=True)
        catalog = self._normalize_catalog(raw, param_cds)

        # When both state and bbox are supplied, post-filter to the box.
        if state_cds and bbox_str is not None:
            catalog = _filter_bbox(catalog, bbox)

        catalog = catalog.drop_duplicates(
            subset=["site_no", "param_cd", "duration_code"]
        ).reset_index(drop=True)
        logger.info("read_sites_catalog: %d series across %d sites",
                    len(catalog), catalog["site_no"].nunique())
        return catalog

    def _fetch_site_catalog(self, nwis, param_cds, site_types, **major_filter):
        """Single NWIS site-service query with ``seriesCatalogOutput=True``."""
        kwargs = dict(major_filter)
        kwargs["parameterCd"] = param_cds
        kwargs["seriesCatalogOutput"] = True
        if site_types:
            kwargs["siteType"] = site_types
        try:
            with _ignore_dataretrieval_warnings():
                df, _ = nwis.get_info(**kwargs)
        except Exception as exc:
            logger.warning("site-service query failed for %s: %s", major_filter, exc)
            return None
        if df is None or len(df) == 0:
            return None
        return pd.DataFrame(df)

    def _normalize_catalog(self, raw: pd.DataFrame, param_cds):
        """Normalize a raw NWIS series-catalog frame to :data:`CATALOG_COLUMNS`."""
        import geopandas as gpd

        df = raw.copy()

        # parm_cd / data_type_cd may vary in casing/name across responses
        parm_col = _first_present(df, ["parm_cd", "parameter_cd", "param_cd"])
        dtype_col = _first_present(df, ["data_type_cd"])
        if parm_col is None or dtype_col is None:
            logger.warning("_normalize_catalog: missing parm_cd/data_type_cd columns")
            return self._empty_catalog()

        df["param_cd"] = df[parm_col].astype(str).str.zfill(5)
        df["data_type_cd"] = df[dtype_col].astype(str).str.lower()
        df["duration_code"] = df["data_type_cd"].map(_DATA_TYPE_TO_DURATION)

        # Keep only instantaneous / daily series for the requested parameters
        df = df[df["duration_code"].isin(["E", "D"])]
        df = df[df["param_cd"].isin([str(p).zfill(5) for p in param_cds])]
        if len(df) == 0:
            return self._empty_catalog()

        df["site_no"] = df["site_no"].astype(str)
        df["station_nm"] = df.get("station_nm", "").astype(str)
        df["param_name"] = df["param_cd"].map(param_name)
        df["unit"] = df["param_cd"].map(param_unit)
        df["duration"] = df["duration_code"].map(DURATION_LABEL)
        df["stat_cd"] = df.get("stat_cd", "").astype(str)
        df["begin_date"] = pd.to_datetime(df.get("begin_date"), errors="coerce")
        df["end_date"] = pd.to_datetime(df.get("end_date"), errors="coerce")
        df["dec_lat_va"] = pd.to_numeric(df.get("dec_lat_va"), errors="coerce")
        df["dec_long_va"] = pd.to_numeric(df.get("dec_long_va"), errors="coerce")
        df["site_tp_cd"] = df.get("site_tp_cd", "").astype(str)
        df["huc_cd"] = df.get("huc_cd", "").astype(str)
        df["agency_cd"] = df.get("agency_cd", "USGS").astype(str)
        df["source"] = "USGS NWIS"

        df = df.dropna(subset=["dec_lat_va", "dec_long_va"])
        gdf = gpd.GeoDataFrame(
            df[CATALOG_COLUMNS].copy(),
            geometry=gpd.points_from_xy(
                df["dec_long_va"], df["dec_lat_va"], crs="EPSG:4326"
            ),
        )
        return gdf

    @staticmethod
    def _empty_catalog():
        import geopandas as gpd

        return gpd.GeoDataFrame(
            {c: pd.Series(dtype="object") for c in CATALOG_COLUMNS},
            geometry=[],
            crs="EPSG:4326",
        )

    # ------------------------------------------------------------------
    # Catalog persistence
    # ------------------------------------------------------------------

    def catalog_csv_path(self) -> str:
        return f"{self.dbase_dir}/usgs_sites_catalog.csv"

    def save_all_stations_info(
        self, state_cds=None, bbox=None, param_cds=None, site_types=None
    ):
        """Build the catalog and persist it to ``usgs_sites_catalog.csv``."""
        catalog = self.read_sites_catalog(
            state_cds=state_cds, bbox=bbox, param_cds=param_cds, site_types=site_types
        )
        out = pd.DataFrame(catalog.drop(columns="geometry"))
        out.to_csv(self.catalog_csv_path(), index=False)
        logger.info("save_all_stations_info: wrote %d rows to %s",
                    len(out), self.catalog_csv_path())
        return catalog

    def read_saved_stations_info(self):
        """Read the saved catalog CSV back as a WGS84 ``GeoDataFrame``."""
        import geopandas as gpd

        df = pd.read_csv(
            self.catalog_csv_path(),
            dtype={
                "site_no": str,
                "param_cd": str,
                "stat_cd": str,
                "huc_cd": str,
                "duration_code": str,
            },
        )
        df["begin_date"] = pd.to_datetime(df.get("begin_date"), errors="coerce")
        df["end_date"] = pd.to_datetime(df.get("end_date"), errors="coerce")
        gdf = gpd.GeoDataFrame(
            df,
            geometry=gpd.points_from_xy(
                df["dec_long_va"], df["dec_lat_va"], crs="EPSG:4326"
            ),
        )
        return gdf

    # ------------------------------------------------------------------
    # Parquet cache helpers
    # ------------------------------------------------------------------

    def build_db_path(self, site_no, param_cd, duration_code) -> str:
        return f"{self.dbase_dir}/{site_no}__{param_cd}__{duration_code}.prq"

    def remove_from_db(self, site_no, param_cd, duration_code) -> None:
        try:
            os.remove(self.build_db_path(site_no, param_cd, duration_code))
        except OSError:
            pass

    def load_from_db(self, site_no, param_cd, duration_code) -> pd.DataFrame:
        return pd.read_parquet(self.build_db_path(site_no, param_cd, duration_code))

    def save_to_db(self, df, site_no, param_cd, duration_code) -> None:
        df.to_parquet(self.build_db_path(site_no, param_cd, duration_code))

    # ------------------------------------------------------------------
    # Time series retrieval
    # ------------------------------------------------------------------

    def _fetch_raw(self, site_no, param_cd, duration_code, start, end) -> pd.DataFrame:
        """Fetch a single IV/DV window and return a tz-naive ``VALUE`` frame.

        For daily values (``duration_code == "D"``), ``nwis.get_dv`` is tried
        first.  If it returns empty (the DV NWIS REST endpoint has been
        decommissioned in newer versions of *dataretrieval*), we fall back to
        fetching IV data via ``nwis.get_iv`` and resampling to a daily mean —
        which is equivalent to ``statCd="00003"`` and will work as long as the
        instantaneous-value endpoint is alive.
        """
        from dataretrieval import nwis

        start_s = pd.to_datetime(start).strftime("%Y-%m-%d")
        end_s = pd.to_datetime(end).strftime("%Y-%m-%d")

        def _get_iv():
            with _ignore_dataretrieval_warnings():
                return nwis.get_iv(
                    sites=site_no, start=start_s, end=end_s,
                    parameterCd=param_cd, multi_index=False,
                )

        df = None
        if duration_code == "D":
            try:
                with _ignore_dataretrieval_warnings():
                    df_dv, _ = nwis.get_dv(
                        sites=site_no, start=start_s, end=end_s,
                        parameterCd=param_cd, statCd=self.stat_cd, multi_index=False,
                    )
                if df_dv is not None and len(df_dv) > 0:
                    value_col = _find_value_column(df_dv, param_cd)
                    if value_col is not None:
                        df = df_dv
            except Exception as exc:
                logger.debug("get_dv failed for %s/%s, will fallback to IV resample: %s",
                             site_no, param_cd, exc)

            if df is None:
                # DV endpoint returned no data (decommissioned) — compute daily
                # mean from IV data and resample.  This is equivalent to
                # statCd="00003" (mean daily value).
                logger.debug(
                    "_fetch_raw %s/%s/D: get_dv empty, falling back to IV+daily-resample",
                    site_no, param_cd,
                )
                try:
                    df_iv, _ = _get_iv()
                    if df_iv is not None and len(df_iv) > 0:
                        value_col = _find_value_column(df_iv, param_cd)
                        if value_col is not None:
                            df_iv = df_iv[[value_col]].copy()
                            df_iv.columns = ["VALUE"]
                            df_iv["VALUE"] = pd.to_numeric(df_iv["VALUE"], errors="coerce")
                            df_iv.index = _to_pacific_naive(df_iv.index, self.output_tz)
                            df_iv.index.name = "datetime"
                            df_iv = df_iv[~df_iv.index.duplicated(keep="last")].sort_index()
                            # Resample to daily mean — midnight of each calendar day.
                            daily = df_iv.resample("D").mean()
                            return daily[~daily.index.duplicated(keep="last")].sort_index()
                except Exception as exc:
                    logger.debug("IV fallback fetch failed for %s/%s: %s",
                                 site_no, param_cd, exc)
                return pd.DataFrame(columns=["VALUE"])
        else:
            try:
                df, _ = _get_iv()
            except Exception as exc:
                logger.debug("fetch %s/%s/%s %s-%s failed: %s",
                             site_no, param_cd, duration_code, start_s, end_s, exc)
                return pd.DataFrame(columns=["VALUE"])

        if df is None or len(df) == 0:
            return pd.DataFrame(columns=["VALUE"])

        value_col = _find_value_column(df, param_cd)
        if value_col is None:
            logger.warning(
                "_fetch_raw %s/%s/%s: no value column found in %s",
                site_no, param_cd, duration_code, list(df.columns),
            )
            return pd.DataFrame(columns=["VALUE"])

        out = df[[value_col]].copy()
        out.columns = ["VALUE"]
        out["VALUE"] = pd.to_numeric(out["VALUE"], errors="coerce")
        out.index = _to_pacific_naive(out.index, self.output_tz)
        out.index.name = "datetime"
        out = out[~out.index.duplicated(keep="last")].sort_index()
        return out

    def _fetch_data(self, site_no, param_cd, duration_code, start, end) -> pd.DataFrame:
        """Fetch a (possibly multi-year) window, returning a tz-naive VALUE frame.

        Strategy
        --------
        * **Daily (D)**: one single ``_fetch_raw`` call for the whole range —
          daily data is tiny (~365 rows/year) so there is no reason to chunk
          it.  This also ensures the ``get_dv``-empty fallback log fires at
          most *once* per station rather than once per year.
        * **Instantaneous (E)**: yearly-chunked via ``dask.delayed`` (parallel)
          to avoid NWIS timeouts on large IV windows.
        """
        import dask

        stime = pd.to_datetime(start)
        etime = pd.to_datetime(end)
        if etime < stime:
            stime, etime = etime, stime

        if duration_code == "D":
            # Single request for the whole range — no chunking needed for daily data.
            df = self._fetch_raw(site_no, param_cd, duration_code, stime, etime)
            return df if df is not None else pd.DataFrame(columns=["VALUE"])

        # Instantaneous: chunk by year so each request stays manageable.
        windows = []
        for year in range(stime.year, etime.year + 1):
            ws = max(stime, pd.Timestamp(f"{year}-01-01"))
            we = min(etime, pd.Timestamp(f"{year}-12-31 23:59:59"))
            windows.append((ws, we))

        if len(windows) == 1:
            frames = [
                self._fetch_raw(site_no, param_cd, duration_code, ws, we)
                for ws, we in windows
            ]
        else:
            tasks = [
                dask.delayed(self._fetch_raw)(site_no, param_cd, duration_code, ws, we)
                for ws, we in windows
            ]
            frames = list(dask.compute(*tasks))

        frames = [f for f in frames if f is not None and len(f)]
        if not frames:
            return pd.DataFrame(columns=["VALUE"])
        df = pd.concat(frames)
        df = df[~df.index.duplicated(keep="last")].sort_index()
        return df

    def read_station_data(self, site_no, param_cd, duration_code, start, end) -> pd.DataFrame:
        """Return cached series for a site/param/duration, gap-filling from NWIS.

        Mirrors the ``cdec_maps`` caching strategy: load whatever is cached,
        fetch only the missing head/tail windows, persist, and return the
        requested slice.
        """
        stime = pd.to_datetime(start)
        etime = pd.to_datetime(end)
        try:
            df = self.load_from_db(site_no, param_cd, duration_code)
        except Exception:
            df = None

        if df is not None and len(df):
            needs_saving = False
            if stime < df.index.min():
                head = self._fetch_data(
                    site_no, param_cd, duration_code, start, df.index.min()
                )
                if len(head):
                    df = head.combine_first(df)
                    needs_saving = True
            if etime > df.index.max():
                tail = self._fetch_data(
                    site_no, param_cd, duration_code, df.index.max(), end
                )
                if len(tail):
                    df = tail.combine_first(df)
                    needs_saving = True
            if needs_saving:
                self.save_to_db(df, site_no, param_cd, duration_code)
        else:
            df = self._fetch_data(site_no, param_cd, duration_code, start, end)
            if len(df):
                self.save_to_db(df, site_no, param_cd, duration_code)

        if df is None or len(df) == 0:
            return pd.DataFrame(columns=["VALUE"])
        return df.loc[stime:etime]


# ---------------------------------------------------------------------------
# Module-level helpers
# ---------------------------------------------------------------------------


def _first_present(df: pd.DataFrame, candidates):
    for c in candidates:
        if c in df.columns:
            return c
    return None


def _find_value_column(df: pd.DataFrame, param_cd: str):
    """Return the value column for ``param_cd`` (ignoring qualifier ``*_cd`` cols)."""
    param_cd = str(param_cd)
    # Exact match first
    for c in df.columns:
        if c == param_cd:
            return c
    # Prefixed (e.g. '00060_Mean') but not a qualifier column
    for c in df.columns:
        if c.endswith("_cd"):
            continue
        if c.startswith(param_cd + "_"):
            return c
    return None


def _to_pacific_naive(index, output_tz=OUTPUT_TZ):
    """Convert a (possibly tz-aware or Period) index to tz-naive Pacific time.

    ``nwis.get_dv`` can return a ``PeriodIndex`` for daily data.  Convert it
    to a ``DatetimeIndex`` via ``to_timestamp()`` before the timezone dance,
    matching the pattern documented in the pydsm conventions.
    """
    if isinstance(index, pd.PeriodIndex):
        index = index.to_timestamp()
    idx = pd.DatetimeIndex(index)
    if idx.tz is not None:
        idx = idx.tz_convert(output_tz).tz_localize(None)
    return idx


def _bbox_to_str(bbox) -> str:
    """Normalize a bbox (str or sequence) to a ``"w,s,e,n"`` string."""
    if isinstance(bbox, str):
        parts = [p.strip() for p in bbox.split(",")]
    else:
        parts = [str(v).strip() for v in bbox]
    if len(parts) != 4:
        raise ValueError(
            f"bbox must have 4 values (west,south,east,north); got {bbox!r}"
        )
    return ",".join(parts)


def _bbox_bounds(bbox):
    """Return ``(west, south, east, north)`` floats from a bbox str/sequence."""
    s = _bbox_to_str(bbox)
    west, south, east, north = (float(v) for v in s.split(","))
    return west, south, east, north


def _filter_bbox(catalog, bbox):
    """Filter a catalog GeoDataFrame to rows inside ``bbox``."""
    west, south, east, north = _bbox_bounds(bbox)
    lat = catalog["dec_lat_va"]
    lon = catalog["dec_long_va"]
    mask = (lon >= west) & (lon <= east) & (lat >= south) & (lat <= north)
    return catalog[mask].copy()
