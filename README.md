# usgs_maps

Interactive maps and dashboards for **USGS National Water Information System
(NWIS)** data, built on the reusable [`dvue`](https://github.com/CADWRDeltaModeling/dvue)
Panel + HoloViews + GeoViews framework.

`usgs_maps` is a sibling of
[`cdec_maps`](https://github.com/dwr-psandhu/cdec-maps): it caches a station
catalog derived from the NWIS **Site Service** and serves time series from the
NWIS **Instantaneous Values (IV)** and **Daily Values (DV)** services, with a
local Parquet cache and gap-fill-on-read.

## Features

- Catalog builder filtered by **state code(s)** and/or a **lat/lon bounding box**
- Interactive map + filterable catalog table + time-series plots
- Instantaneous (IV, ~15-min) and Daily (DV) values
- Local Parquet cache (`usgs_db/`) with gap-fill-on-read
- Optional cosine-Lanczos tidal filtering (via `vtools3`)
- dvue plugin registration (`dvue ui usgs:<spec>`)

## Default parameters

| Code  | Name                 | Unit  |
|-------|----------------------|-------|
| 00060 | Discharge            | cfs   |
| 00065 | Gage height          | ft    |
| 00010 | Temperature, water   | deg C |
| 00095 | Specific conductance | uS/cm |
| 00400 | pH                   | std   |
| 63680 | Turbidity            | FNU   |

## Install

```bash
conda env create -f environment.yml --name usgs_maps
conda activate usgs_maps
pip install -e .
```

The `cadwr-dms` conda channel is required for `vtools3` and `dvue`.

## Build the cache

At least one of `--state` or `--bbox` is required.

```bash
# California, default parameters, with series download
usgs_maps build-cache --state ca

# Bounding box, two parameters, catalog only (no download)
usgs_maps build-cache --bbox "-122.5,37.5,-121.0,38.5" \
    --param 00060 --param 00065 --no-download

# From a YAML/JSON config
usgs_maps build-cache --config usgs_cache_config.yml
```

Example `usgs_cache_config.yml`:

```yaml
states: [ca, nv]
bbox: "-122.5,37.5,-121.0,38.5"   # optional; post-filters when states set
param_cds: ["00060", "00065"]
site_types: ST                     # optional NWIS siteType filter
start: "2020-01-01"
end: "2024-12-31"
download: true
```

## Run the dashboard

```bash
# Local interactive window
usgs_maps show-all-stations

# Or via Panel
panel serve usgs_maps/usgs_maps_servable.py --show

# Or the server entry-point
python usgsui.py --address 0.0.0.0 --port 80 --no-show
```

## Hosting

Mirrors `cdec_maps`:

- `run_server.sh` — installs the package and starts `usgsui.py` on `0.0.0.0:80`.
- `run_server_cache_build.sh` — runs `build_usgs_cache.py` in the background and
  serves `usgs_maps_servable.py` with `--allow-websocket-origin="*"`.
- `CARTO_API_KEY` — set it in the environment (an App Service application setting
  in production) to a [CARTO basemaps API key](https://carto.com/basemaps/apikey);
  without it the map tiles show CARTO's "API key required" watermark.

## dvue plugin

Once installed, the reader is auto-discovered by `dvue`:

```bash
dvue ui usgs:ca               # scan California
dvue ui usgs:ca,nv            # multiple states
dvue ui usgs:bbox=-122.5,37.5,-121,38.5
```

## CLI

| Command | Description |
|---------|-------------|
| `usgs_maps version` | Print the package version |
| `usgs_maps build-cache` | Build the catalog (+ optional series download) |
| `usgs_maps show-all-stations` | Launch the dashboard |
| `usgs_maps show-all-sensors` | Alias for `show-all-stations` |

## Tests

```bash
pytest -m "not live"   # offline suite (default)
pytest -m live         # network tests against live NWIS
```

## Conventions

- `site_no` is always a **string** (preserves leading zeros).
- IV timestamps are tz-aware UTC from NWIS; they are converted to
  `America/Los_Angeles` and the tz is **dropped** (tz-naive Pacific) to match
  the vtools tidal-filter expectations.
- Catalog geometry is WGS84 (`EPSG:4326`).
- Catalog primary key: `["site_no", "param_cd", "duration_code"]`.
- Reference name format: `f"{site_no}/{param_cd}/{duration_code}"`.

## License

MIT — see [LICENSE](LICENSE).
