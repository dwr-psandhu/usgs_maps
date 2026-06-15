# usgs_maps — Copilot Instructions

**usgs_maps** is a Python toolkit for interactive visualization of **USGS NWIS**
(National Water Information System) data. It caches a station/series catalog from
the NWIS Site Service and serves time series from the Instantaneous Values (IV)
and Daily Values (DV) services. It is a sibling of **cdec_maps** and is built on
the reusable **dvue** Panel/HoloViews/GeoViews framework.

See [README.md](../README.md) for usage. Read `../../dvue/AGENTS.md` before
changing any manager/catalog/reader behavior.

---

## Environment & Install

```bash
conda env create -f environment.yml --name usgs_maps
conda activate usgs_maps
pip install -e .
```

- Conda channels: `cadwr-dms` (for `vtools3`, `dvue`) + `conda-forge`.
- `dataretrieval` (DOI-USGS) is the NWIS client; it is on conda-forge.
- Dynamic versioning via `setuptools_scm` — do not hand-edit `usgs_maps/_version.py`.

```bash
pytest -m "not live"   # offline suite (default); network tests are marked `live`
flake8 usgs_maps tests
```

## Terminal & Environment Rules

- Activate the env before any project command: `conda activate usgs_maps`.
- If the env is unavailable, stop and ask — do not continue.

---

## Architecture

| Module | Role |
|--------|------|
| `usgs_maps/usgs.py` | **Core data layer**: `Reader` class — NWIS catalog + IV/DV retrieval, Parquet cache, gap-fill |
| `usgs_maps/usgsuimgr.py` | dvue integration: `USGSDataReferenceReader`, `USGSDataReference`, `USGSDataUIManager`, `USGSTimeSeriesPlotAction`, `show_usgs_ui()` |
| `usgs_maps/readers.py` | dvue plugin: `register_readers()` → `ReaderRegistry.register("usgs", …)` |
| `usgs_maps/maps.py` | Geo helpers: bbox/polygon filtering, lat/lon → GeoDataFrame |
| `usgs_maps/usgs_cache_build.py` | Cache builder: `run_build()`, `build_catalog()`, `download_all_series()` |
| `usgs_maps/cli.py` | **click** CLI (`usgs_maps` command) |
| `usgs_maps/usgs_maps_servable.py` | `panel serve` entry point |
| `usgsui.py` (root) | `pn.serve` server entry (argparse `--address/--port/--no-show`) |
| `build_usgs_cache.py` (root) | Standalone click cache-build script (server/cron use) |

CLI uses **click** (not argparse). The server entry `usgsui.py` uses argparse
(mirrors `cdecui.py`).

---

## NWIS / dataretrieval Conventions

### dataretrieval API (the `nwis` module)
- `nwis.get_info(stateCd=/bBox=, parameterCd=[...], seriesCatalogOutput=True, siteType=)`
  → `(GeoDataFrame, metadata)`. Series catalog columns: `agency_cd`, `site_no`,
  `station_nm`, `site_tp_cd`, `dec_lat_va`, `dec_long_va`, `huc_cd`, `parm_cd`,
  `stat_cd`, `data_type_cd`, `begin_date`, `end_date`. CRS is NAD83 (EPSG:4269);
  we re-emit as WGS84 (EPSG:4326).
- `nwis.get_iv(sites, start, end, parameterCd, multi_index=False)` → `(df, md)`.
  Index is **tz-aware UTC**; value column is the param code (e.g. `"00060"`),
  qualifier column is `"00060_cd"`.
- `nwis.get_dv(sites, start, end, parameterCd, statCd="00003", multi_index=False)`.
- bBox string format: `"west,south,east,north"`.
- NWIS allows only **one state per request** — query each state separately and
  concat.

### dataretrieval deprecation warnings
The `nwis` functions are deprecated (removal ~2027) but functional. They emit
`DeprecationWarning`/`UserWarning`. Suppress with the
`usgs._ignore_dataretrieval_warnings()` context manager around every call.

### data_type_cd → duration_code mapping
`{"uv": "E", "iv": "E", "dv": "D"}` (uv = unit/instantaneous, dv = daily).
Duration labels: `{"E": "instantaneous", "D": "daily"}`.

---

## Critical Conventions

### site_no is always a string
Preserve leading zeros everywhere (`site_no` like `"11447650"`, but many are
`"0xxxxxxx"`). Read CSVs with `dtype={"site_no": str, ...}`. Never cast to int.

### IV timezone — convert UTC → Pacific, drop tz (Option A)
NWIS IV timestamps are tz-aware UTC. Convert to `America/Los_Angeles`, then
`tz_localize(None)` to make tz-naive Pacific. Done by `usgs._to_pacific_naive`.
This matches the vtools cosine-Lanczos tidal filter expectation (tz-naive).

### Parquet cache + gap-fill-on-read
`usgs_db/{site_no}__{param_cd}__{duration_code}.prq`. `read_station_data` loads
whatever is cached, fetches only the missing head/tail windows via
`_fetch_data` (yearly-chunked, dask.delayed for IV), `combine_first`, saves, and
returns `df.loc[start:end]`. Mirrors the `cdec_maps` design.

### Resample before tidal filter
`USGSDataReferenceReader.load` resamples to a regular frequency
(`{"E": "15min", "D": "D"}`, or inferred) so the `DatetimeIndex` carries a freq —
required by the vtools filter.

### Catalog primary key & reference name
- `DataCatalog(primary_key=["site_no", "param_cd", "duration_code"], crs=...)`.
- Reference name: `f"{site_no}/{param_cd}/{duration_code}"`.
- `get_data_reference(row)` looks up by `row["name"]` (never branch on NaN
  `filename`/`source` — see `dvue/AGENTS.md`).
- `_build_dvue_catalog` bypasses `catalog.add()` and writes
  `catalog._references[ref.name] = ref` directly (bulk-load optimisation; rows
  are de-duplicated first by the primary key).

### Default parameters
`["00060", "00065", "00010", "00095", "00400", "63680"]`
(discharge, gage height, water temp, specific conductance, pH, turbidity).

### Geo scope
Cache build requires at least one of `--state` (repeatable) or `--bbox`
(`"w,s,e,n"`). When both are given, bbox post-filters the state catalog.

### Groundwater
Groundwater service is a stub for later — IV + DV only for now.

---

## dvue Integration (read `../../dvue/AGENTS.md`)

- `USGSDataReferenceReader(DataReferenceReader)` — flyweight; `load(**attributes)`
  is time-range-aware; `catalog_crs()` → `"EPSG:4326"`; `scan(path)` parses a
  `usgs:` spec (state codes or `bbox=…`), cache-first via
  `read_saved_stations_info`.
- `USGSDataReference(DataReference)` — `_REQUIRED = ("site_no","param_cd",
  "duration_code")`; `from_catalog_row` sets the explicit name.
- `USGSDataUIManager(TimeSeriesDataUIManager)` — `color_cycle_column="site_no"`,
  `dashed_line_cycle_column="duration"`, `marker_cycle_column="param_name"`.
  Do **not** pass `url_column`/`url_num_column`/`identity_key_columns` to
  `super().__init__()` — those params were removed from dvue.
- Plugin: `readers.register_readers()` → `ReaderRegistry.register("usgs",
  USGSDataReferenceReader)`; wired via `[project.entry-points."dvue.plugins"]`.

---

## Testing

Tests are offline by default (network is monkeypatched in `tests/conftest.py`).
Fixtures: `reader` (temp `usgs_db`), `catalog_reader` (fake site service),
`data_reader` (fake `_fetch_raw`), `fake_catalog` (built+saved catalog).
Network tests use `@pytest.mark.live` and are skipped unless `pytest -m live`.

---

## Primary Data Conventions

- Time series: `pandas.DataFrame` with tz-naive Pacific `DatetimeIndex`.
- EC/specific conductance: µS/cm. Discharge: cfs. Gage height: ft.
- Geometry: WGS84 (`EPSG:4326`).
- Do not mix tz-aware and tz-naive timestamps.
