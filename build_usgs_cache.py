"""Standalone cache-build script for usgs_maps (server/cron use).

Builds the USGS NWIS catalog and pre-downloads series into the Parquet cache.
This is the script invoked by ``run_server_cache_build.sh``.

Examples
--------
Build a California catalog and download the default parameters::

    python build_usgs_cache.py --state ca

Build from a YAML config file::

    python build_usgs_cache.py --config usgs_cache_config.yml

Restrict to a bounding box and two parameters, catalog only (no download)::

    python build_usgs_cache.py --bbox "-122.5,37.5,-121.0,38.5" \
        --param 00060 --param 00065 --no-download
"""

from __future__ import annotations

import logging

import click

from usgs_maps import usgs_cache_build


@click.command(context_settings={"help_option_names": ["-h", "--help"]})
@click.option("--state", "states", multiple=True, help="2-letter state code (repeatable).")
@click.option("--bbox", default=None, help='Lat/lon box "west,south,east,north".')
@click.option("--param", "params", multiple=True, help="USGS parameter code (repeatable).")
@click.option("--site-type", "site_types", multiple=True, help="NWIS siteType (repeatable).")
@click.option("--start", default=None, help="Series download start (YYYY-MM-DD).")
@click.option("--end", default=None, help="Series download end (YYYY-MM-DD).")
@click.option("--download/--no-download", default=True, help="Download series after catalog.")
@click.option("--dbase-dir", default="usgs_db", show_default=True, help="Cache directory.")
@click.option("--config", "-c", default=None, help="YAML/JSON config file.")
def main(states, bbox, params, site_types, start, end, download, dbase_dir, config):
    """Build the USGS NWIS cache (catalog + optional series download)."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-28s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    usgs_cache_build.run_build(
        dbase_dir=dbase_dir,
        state_cds=list(states) or None,
        bbox=bbox,
        param_cds=list(params) or None,
        site_types=list(site_types) or None,
        start=start,
        end=end,
        download=download,
        config=config,
    )


if __name__ == "__main__":
    main()
