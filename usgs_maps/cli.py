"""Command-line interface for usgs_maps.

Commands
--------
- ``usgs_maps version``            Print the package version.
- ``usgs_maps build-cache``        Build the NWIS catalog (+ optional series).
- ``usgs_maps show-all-stations``  Launch the interactive map + plot dashboard.
- ``usgs_maps show-all-sensors``   Alias for ``show-all-stations``.
"""

from __future__ import annotations

import logging

import click

from usgs_maps import __version__


@click.group(context_settings={"help_option_names": ["-h", "--help"]})
@click.version_option(__version__, "-V", "--version", prog_name="usgs_maps")
def cli():
    """USGS NWIS Maps and Dashboards."""
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-28s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )


@cli.command("version")
def version_cmd():
    """Print the package version."""
    click.echo(f"usgs_maps {__version__}")


@cli.command("build-cache")
@click.option(
    "--state", "states", multiple=True,
    help="2-letter state code (repeatable, e.g. --state ca --state nv).",
)
@click.option(
    "--bbox", default=None,
    help='Lat/lon box "west,south,east,north" (decimal degrees).',
)
@click.option(
    "--param", "params", multiple=True,
    help="USGS parameter code to include (repeatable). Defaults to the standard set.",
)
@click.option(
    "--site-type", "site_types", multiple=True,
    help="NWIS siteType filter (repeatable, e.g. ST for streams).",
)
@click.option("--start", default=None, help="Series download start (YYYY-MM-DD).")
@click.option("--end", default=None, help="Series download end (YYYY-MM-DD).")
@click.option(
    "--download/--no-download", default=True,
    help="Download series into the cache after building the catalog.",
)
@click.option("--dbase-dir", default="usgs_db", show_default=True, help="Cache directory.")
@click.option(
    "--config", "-c", default=None,
    help="YAML/JSON config file (states, bbox, param_cds, site_types, start, end, download).",
)
def build_cache_cmd(states, bbox, params, site_types, start, end, download, dbase_dir, config):
    """Build the USGS catalog and optionally pre-download series data."""
    from usgs_maps import usgs_cache_build

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


@cli.command("show-all-stations")
def show_all_stations_cmd():
    """Launch the interactive map + time-series dashboard."""
    from usgs_maps import usgsuimgr

    view, _ = usgsuimgr.show_usgs_ui()
    view.show()


@cli.command("show-all-sensors")
def show_all_sensors_cmd():
    """Alias for show-all-stations."""
    from usgs_maps import usgsuimgr

    view, _ = usgsuimgr.show_usgs_ui()
    view.show()


if __name__ == "__main__":
    cli()
