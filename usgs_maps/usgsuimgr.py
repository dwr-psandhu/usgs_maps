"""dvue integration for usgs_maps.

Defines the catalog reference / reader / manager / plot-action classes that
plug the USGS NWIS data layer (:mod:`usgs_maps.usgs`) into the reusable
``dvue`` Panel framework.

Class hierarchy::

    USGSDataReferenceReader(DataReferenceReader)   # flyweight, shared by all refs
    USGSDataReference(DataReference)               # one per (site, param, duration)
    USGSDataUIManager(TimeSeriesDataUIManager)     # dashboard manager
    USGSTimeSeriesPlotAction(TimeSeriesPlotAction) # curve labels & titles
"""

from __future__ import annotations

from datetime import datetime, timedelta
import logging
import warnings

import pandas as pd
import holoviews as hv
import param

from dvue.catalog import DataReferenceReader, DataReference, DataCatalog
from dvue.dataui import DataUI
from dvue.tsdataui import TimeSeriesDataUIManager, TimeSeriesPlotAction

try:
    from dvue.dataui import DWR_DISCLAIMER_TEXT
except Exception:  # pragma: no cover - older dvue
    DWR_DISCLAIMER_TEXT = ""

from . import usgs

warnings.filterwarnings("ignore")
hv.extension("bokeh")

logger = logging.getLogger(__name__)

__all__ = [
    "USGSDataReferenceReader",
    "USGSDataReference",
    "USGSDataUIManager",
    "USGSTimeSeriesPlotAction",
    "show_usgs_ui",
]


class USGSDataReferenceReader(DataReferenceReader):
    """Reads a single USGS NWIS series given site/param/duration attributes.

    A single instance is shared across all DataReference objects in the
    catalog (flyweight pattern).  When ``time_range`` is present in the
    attributes passed by :meth:`DataReference.getData`, only that window is
    requested from NWIS.

    Parameters
    ----------
    source_or_reader : str or usgs.Reader or None
        When a :class:`usgs.Reader` is passed it is used directly.  When a
        string is passed (registry construction) it is treated as a source
        spec and ignored — a default :class:`usgs.Reader` is created lazily.
    """

    def __init__(self, source_or_reader=None) -> None:
        if isinstance(source_or_reader, str):
            self._reader = None
        else:
            self._reader = source_or_reader

    def _get_reader(self) -> "usgs.Reader":
        if self._reader is None:
            self._reader = usgs.Reader()
        return self._reader

    def load(self, **attributes) -> pd.DataFrame:
        site_no = attributes["site_no"]
        param_cd = attributes["param_cd"]
        duration_code = attributes["duration_code"]
        unit = attributes.get("unit", "")
        param_nm = attributes.get("param_name", param_cd)
        time_range = attributes.get("time_range")
        if time_range is not None:
            start = pd.Timestamp(time_range[0]).strftime("%Y-%m-%d")
            end = pd.Timestamp(time_range[1]).strftime("%Y-%m-%d")
        else:
            start = "1900-01-01"
            end = pd.Timestamp.now().strftime("%Y-%m-%d")

        df = self._get_reader().read_station_data(
            site_no, param_cd, duration_code, start, end
        )
        ptype = "instantaneous" if duration_code == "E" else "period-averaged"
        if df is None or len(df) == 0:
            # Return an empty DataFrame with a DatetimeIndex (not RangeIndex) so
            # dvue's _process_curve_data can safely compare index to Timestamp
            # bounds without raising TypeError.
            empty = pd.DataFrame(
                index=pd.DatetimeIndex([], name="datetime"),
                columns=[f"{site_no}/{param_nm}"],
            )
            empty.attrs["unit"] = unit
            empty.attrs["ptype"] = ptype
            return empty

        df = df[["VALUE"]].copy()
        df.columns = [f"{site_no}/{param_nm}"]
        df = df[slice(df.first_valid_index(), df.last_valid_index())]
        df.attrs["unit"] = unit
        df.attrs["ptype"] = ptype

        # Resample to a regular frequency so the DatetimeIndex carries a freq
        # attribute — required by the vtools cosine_lanczos tidal filter.
        _DURATION_FREQ = {"E": "15min", "D": "D"}
        if len(df) > 1:
            inferred = pd.infer_freq(df.index)
            if inferred is None and len(df) >= 3:
                median_dt = pd.Series(df.index.to_numpy()).diff().dropna().median()
                try:
                    inferred = pd.tseries.frequencies.to_offset(median_dt)
                except Exception:
                    inferred = None
            dur_freq = inferred or _DURATION_FREQ.get(duration_code)
            if dur_freq is not None:
                attrs = df.attrs
                try:
                    df = df.resample(dur_freq).mean()
                    df.attrs = attrs
                except Exception:
                    df.attrs = attrs

        # Defensive: ensure the index is always a DatetimeIndex so that dvue's
        # _process_curve_data can compare it to Timestamp bounds without TypeError.
        if not isinstance(df.index, pd.DatetimeIndex):
            try:
                df.index = pd.to_datetime(df.index)
            except Exception:
                logger.warning(
                    "load %s/%s/%s: could not coerce %s to DatetimeIndex",
                    site_no, param_cd, duration_code, type(df.index).__name__,
                )
        return df

    def __repr__(self) -> str:
        return f"USGSDataReferenceReader(reader={self._reader!r})"

    @classmethod
    def catalog_crs(cls) -> str:
        """USGS geometry is geographic WGS84 (EPSG:4326)."""
        return "EPSG:4326"

    @classmethod
    def scan(cls, path: str) -> list:
        """Scan the USGS catalog and return :class:`USGSDataReference` objects.

        Called by :class:`~dvue.registry.ReaderRegistry` when a ``usgs:``
        source spec is passed to ``dvue ui``.  The ``path`` is interpreted as
        either a comma-separated list of state codes (e.g. ``usgs:ca,nv``) or
        a bbox spec ``usgs:bbox=w,s,e,n``.  An empty spec defaults to ``ca``.

        Cache-first: uses the saved catalog CSV (``usgs_db/``) when present,
        otherwise performs a live NWIS site-service query.
        """
        reader = usgs.Reader()
        shared = cls(reader)

        catalog = None
        try:
            catalog = reader.read_saved_stations_info()
            logger.info("USGS scan: loaded %d rows from local cache", len(catalog))
        except Exception as exc:
            logger.warning("USGS scan: local cache not found (%s); querying NWIS", exc)
            state_cds, bbox = _parse_scan_path(path)
            catalog = reader.read_sites_catalog(state_cds=state_cds, bbox=bbox)

        refs = []
        skipped = 0
        for _, row in catalog.iterrows():
            try:
                ref = USGSDataReference.from_catalog_row(row, reader=shared)
                ref.source = "usgs"
                ref._attributes["source"] = "usgs"
                ref.set_attribute("station", str(row.get("site_no", "")))
                ref.set_attribute("variable", str(row.get("param_name", "")))
                if pd.notna(row.get("begin_date")):
                    ref.set_attribute("time_extent_start", str(row["begin_date"]))
                if pd.notna(row.get("end_date")):
                    ref.set_attribute("time_extent_end", str(row["end_date"]))
                refs.append(ref)
            except Exception as exc:
                skipped += 1
                if skipped <= 5:
                    logger.debug("USGS scan: skipped row — %s", exc)
        logger.info("USGS scan: returning %d refs (%d skipped)", len(refs), skipped)
        return refs


class USGSDataReference(DataReference):
    """USGS-specific :class:`~dvue.catalog.DataReference`.

    Each instance carries enough attributes (``site_no``, ``param_cd``,
    ``duration_code``, ``unit`` …) for :class:`USGSDataReferenceReader` to
    fetch data without any external manager context, so refs can participate
    in mixed-catalog workflows.
    """

    _REQUIRED = ("site_no", "param_cd", "duration_code")

    def __init__(self, reader=None, name: str = "", cache: bool = True, **attributes):
        missing = [k for k in self._REQUIRED if not attributes.get(k)]
        if missing:
            raise ValueError(f"USGSDataReference requires attributes: {missing}")
        if reader is None:
            reader = USGSDataReferenceReader()
        super().__init__(reader=reader, name=name, cache=cache, **attributes)

    @classmethod
    def from_catalog_row(cls, row, reader=None):
        """Build a standalone :class:`USGSDataReference` from a catalog row."""
        site_no = str(row["site_no"])
        param_cd = str(row["param_cd"])
        duration_code = str(row["duration_code"])
        name = f"{site_no}/{param_cd}/{duration_code}"
        attrs = {k: v for k, v in row.items() if k != "geometry"}
        if "geometry" in row.index and row["geometry"] is not None:
            attrs["geometry"] = row["geometry"]
        attrs["site_no"] = site_no
        attrs["param_cd"] = param_cd
        attrs["duration_code"] = duration_code
        return cls(reader=reader, name=name, cache=True, **attrs)

    # Typed accessors -------------------------------------------------
    @property
    def site_no(self) -> str:
        return self.get_attribute("site_no")

    @property
    def param_cd(self) -> str:
        return self.get_attribute("param_cd")

    @property
    def duration_code(self) -> str:
        return self.get_attribute("duration_code")

    @property
    def unit(self) -> str:
        return self.get_attribute("unit")

    @property
    def param_name(self) -> str:
        return self.get_attribute("param_name")

    @property
    def geometry(self):
        return self.get_attribute("geometry")


class USGSTimeSeriesPlotAction(TimeSeriesPlotAction):
    """USGS plot action: builds curve labels and titles from NWIS metadata."""

    def _append_value(self, new_value, value):
        if new_value not in value:
            value += f'{", " if value else ""}{new_value}'
        return value

    def append_to_title_map(self, title_map, unit, r):
        value = title_map.get(unit, ["", "", "", ""])
        value[0] = self._append_value(str(r["param_name"]), value[0])
        value[1] = self._append_value(str(r["site_no"]), value[1])
        value[2] = self._append_value(str(r["duration"]), value[2])
        value[3] = self._append_value(str(r.get("station_nm", "")), value[3])
        title_map[unit] = value

    def create_title(self, v):
        return f"{v[1]} @ {v[3]} ({v[2]}::{v[0]})"


class USGSDataUIManager(TimeSeriesDataUIManager):
    """Time-series dashboard manager for USGS NWIS data."""

    show_math_ref_editor = param.Boolean(default=True)
    show_clear_cache = param.Boolean(default=False)
    show_reset_session_button = param.Boolean(default=True)
    session_cookie_name = param.String(default="usgs_user_id")
    disclaimer_text = DWR_DISCLAIMER_TEXT

    bypass_cache = param.Boolean(
        default=False,
        doc="Bypass cache for reading data. Still builds cache but refetches from NWIS.",
    )
    do_tidal_filter = param.Boolean(
        default=False,
        doc="Apply cosine-Lanczos tidal filter to data before plotting.",
    )

    def __init__(self, dfcat, reader, **kwargs):
        """
        Parameters
        ----------
        dfcat : geopandas.GeoDataFrame
            The station/series catalog (see :data:`usgs.CATALOG_COLUMNS`).
        reader : usgs.Reader
            The NWIS reader used for data retrieval and cache management.
        """
        time_range = kwargs.pop("time_range", None)
        self.reader = reader
        self.station_id_column = kwargs.pop("station_id_column", "site_no")
        self.dfcat = dfcat

        self._dvue_reader = USGSDataReferenceReader(reader)
        geo_crs = (
            str(self.dfcat.crs)
            if hasattr(self.dfcat, "crs") and self.dfcat.crs is not None
            else "EPSG:4326"
        )
        self._dvue_catalog = self._build_dvue_catalog(geo_crs)
        super().__init__(**kwargs)
        # Set time_range after super().__init__ — setting a Parameter before the
        # Parameterized base is fully instantiated is deprecated in param.
        self.time_range = time_range
        self.color_cycle_column = "site_no"
        self.dashed_line_cycle_column = "duration"
        self.marker_cycle_column = "param_name"

    @property
    def data_catalog(self) -> DataCatalog:
        return self._dvue_catalog

    def _build_dvue_catalog(self, crs=None) -> DataCatalog:
        import time

        t0 = time.perf_counter()
        dedup = self.dfcat.drop_duplicates(
            subset=["site_no", "param_cd", "duration_code"]
        )
        n_raw, n_dedup = len(self.dfcat), len(dedup)
        if n_raw != n_dedup:
            logger.warning(
                "_build_dvue_catalog: dropped %d duplicate rows (%d -> %d)",
                n_raw - n_dedup, n_raw, n_dedup,
            )
        catalog = DataCatalog(
            primary_key=["site_no", "param_cd", "duration_code"],
            crs=crs,
        )
        skipped = 0
        for _, row in dedup.iterrows():
            try:
                ref = USGSDataReference.from_catalog_row(row, reader=self._dvue_reader)
                if ref.name:
                    catalog._references[ref.name] = ref
                else:
                    skipped += 1
            except Exception as exc:
                logger.warning("_build_dvue_catalog: skipping ref — %s", exc)
                skipped += 1
        logger.info(
            "_build_dvue_catalog: catalog ready — %d refs, %d skipped (%.1fs)",
            len(catalog._references), skipped, time.perf_counter() - t0,
        )
        return catalog

    # data related methods --------------------------------------------
    def get_data_reference(self, row):
        if "name" in row.index:
            ref = self._dvue_catalog.get(row["name"])
        else:
            ref_name = f"{row['site_no']}/{row['param_cd']}/{row['duration_code']}"
            ref = self._dvue_catalog.get(ref_name)
        if self.bypass_cache:
            self.reader.remove_from_db(
                ref.site_no, ref.param_cd, ref.duration_code
            )
            ref.invalidate_cache()
        return ref

    def build_station_name(self, r):
        return str(r["site_no"])

    def get_convertible_unit_groups(self):
        """Unit groups for dual y-axis from USGS NWIS data.

        USGS NWIS parameter units come from :data:`~usgs_maps.usgs.PARAM_INFO`.
        Unit strings are compared after lowercasing in the renderer.
        """
        return [
            {"ppt", "us/cm"},
        ]

    # Reference secondary axis for common NWIS unit pairs.
    _SECONDARY_AXIS_SPECS = {
        "cfs":   {"label": "m\u00b3/s",      "js_code": "tick / 35.3147"},
        "ft":    {"label": "meters",          "js_code": "tick * 0.3048"},
        "deg c": {"label": "\u00b0F",         "js_code": "tick * 1.8 + 32"},
        "us/cm": {"label": "PSU\u2248",       "js_code": "tick / 1600"},
        "ppt":   {"label": "\u00b5S/cm\u2248", "js_code": "tick * 1600"},
    }

    def get_secondary_axis_spec(self, unit: str):
        """Reference secondary axis for common USGS NWIS unit pairs."""
        return self._SECONDARY_AXIS_SPECS.get(unit.lower())

    def get_time_range(self, dfcat):
        if self.time_range is None:
            self.time_range = (
                datetime.now() - timedelta(days=30),
                datetime.now(),
            )
        return self.time_range

    def _get_table_column_width_map(self):
        return {
            "site_no": "8%",
            "station_nm": "22%",
            "param_name": "12%",
            "param_cd": "6%",
            "duration": "8%",
            "unit": "7%",
            "stat_cd": "6%",
            "site_tp_cd": "6%",
            "begin_date": "9%",
            "end_date": "9%",
        }

    def get_table_filters(self):
        def _like(placeholder="Enter match"):
            return {"type": "input", "func": "like", "placeholder": placeholder}

        return {
            "site_no": _like(),
            "station_nm": _like(),
            "param_name": _like(),
            "param_cd": _like(),
            "duration": _like(),
            "unit": _like(),
            "stat_cd": _like(),
            "site_tp_cd": _like(),
        }

    def is_irregular(self, r):
        # Daily values are regular; instantaneous values are treated as
        # regular and resampled in the reader. Truly irregular series fail the
        # median-dt check in tsdataui and are skipped there.
        return False

    def _make_plot_action(self):
        return USGSTimeSeriesPlotAction(curve_creator=self.create_curve)

    def get_data_actions(self):
        return super().get_data_actions()

    def create_curve(self, df, r, unit, file_index=None):
        file_index_label = f"{file_index}:" if file_index is not None else ""
        crvlabel = f'{file_index_label}{r["site_no"]}/{r["param_name"]}/{r["duration"]}'
        ylabel = f'{r["param_name"]} ({unit})'
        title = f'{r["param_name"]} @ {r["site_no"]} ({r["duration"]}/{r.get("station_nm", "")})'
        df_plot = df.iloc[:, [0]].copy()
        df_plot.index.name = "Time"
        crv = hv.Curve(df_plot, label=crvlabel).redim(value=crvlabel)
        return crv.opts(
            xlabel="Time",
            ylabel=ylabel,
            title=title,
            responsive=True,
            active_tools=["wheel_zoom"],
            tools=["hover"],
        )

    # geolocation / map methods ---------------------------------------
    def get_tooltips(self):
        return [
            ("Site No", "@site_no"),
            ("Station", "@station_nm"),
            ("Parameter", "@param_name"),
            ("Units", "@unit"),
            ("Duration", "@duration"),
        ]

    def get_map_color_columns(self):
        return ["param_name", "duration", "unit", "site_tp_cd"]

    def get_map_marker_columns(self):
        return ["param_name", "duration", "unit", "site_tp_cd"]

    def get_name_to_color(self):
        return hv.Cycle("Category10").values

    def get_name_to_marker(self):
        from bokeh.core.enums import MarkerType

        return list(MarkerType)

    def get_version(self):
        try:
            from usgs_maps._version import version
            return version
        except Exception:
            return "unknown"

    def get_about_text(self):
        return """
        # USGS NWIS Data UI Manager

        Interactive map and time-series explorer for USGS National Water
        Information System (NWIS) data.

        ## Features
        - Browse stations on an interactive map
        - Filter the catalog by site, parameter, duration, and more
        - Plot Instantaneous (IV) and Daily (DV) values
        - Optional cosine-Lanczos tidal filtering
        """


def _parse_scan_path(path: str):
    """Parse a ``usgs:`` scan spec into ``(state_cds, bbox)``."""
    spec = (path or "").strip()
    if not spec:
        return ["ca"], None
    if spec.lower().startswith("bbox="):
        return None, spec[len("bbox="):]
    return [s.strip() for s in spec.split(",") if s.strip()], None


def show_usgs_ui():
    """Build a USGS DataUI from the saved catalog and return ``(view, ui)``."""
    import cartopy.crs as ccrs

    reader = usgs.Reader()
    catalog = reader.read_saved_stations_info()
    crs_cartopy = ccrs.PlateCarree()
    time_range = (
        datetime.now() - timedelta(days=30),
        datetime.now(),
    )
    uimgr = USGSDataUIManager(catalog, reader, time_range=time_range)
    ui = DataUI(uimgr, crs=crs_cartopy, station_id_column="site_no")
    return ui.create_view(), ui
