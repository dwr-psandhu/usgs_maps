"""USGS NWIS Map Explorer — pn.serve entry-point.

Launch with::

    python usgsui.py [--address 0.0.0.0] [--port 80] [--no-show]

Reads the saved catalog (``usgs_db/usgs_sites_catalog.csv``) built by
``usgs_maps build-cache`` and serves the interactive dashboard.
"""

from __future__ import annotations

import argparse
import logging
from datetime import datetime, timedelta

logger = logging.getLogger(__name__)


def build_usgs_manager(time_range=None):
    """Build and return a fresh :class:`USGSDataUIManager` from the saved catalog."""
    from usgs_maps import usgs
    from usgs_maps.usgsuimgr import USGSDataUIManager

    reader = usgs.Reader()
    catalog = reader.read_saved_stations_info()
    logger.info("build_usgs_manager: catalog has %d series", len(catalog))

    if time_range is None:
        time_range = (datetime.now() - timedelta(days=30), datetime.now())
    return USGSDataUIManager(catalog, reader, time_range=time_range)


def main():
    import cartopy.crs as ccrs
    import panel as pn
    from dvue.dataui import DataUI

    parser = argparse.ArgumentParser(
        description="USGS NWIS Map Explorer — Panel server",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    parser.add_argument("--port", type=int, default=80, help="TCP port to serve on")
    parser.add_argument("--address", default="0.0.0.0", help="Network address to bind to")
    parser.add_argument(
        "--no-show", action="store_true",
        help="Do not open a browser window automatically",
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(name)-28s %(levelname)-8s %(message)s",
        datefmt="%H:%M:%S",
    )
    logger.info("Starting USGS NWIS Map Explorer on %s:%s", args.address, args.port)

    pn.extension(
        "tabulator", "codeeditor", notifications=True, design="native",
    )
    crs_cartopy = ccrs.PlateCarree()

    def make_app():
        mgr = build_usgs_manager()
        ui = DataUI(mgr, crs=crs_cartopy, station_id_column="site_no")
        return ui.create_view()

    pn.serve(
        {"usgs-map-explorer": make_app},
        address=args.address,
        port=args.port,
        show=not args.no_show,
        allow_websocket_origin=["*"],
        title="USGS NWIS Map Explorer",
    )


if __name__ == "__main__":
    main()
