"""USGS NWIS Map Explorer — pn.serve entry-point.

Launch with::

    python usgsui.py [--address 0.0.0.0] [--port 80] [--no-show]

Reads the saved catalog (``usgs_db/usgs_sites_catalog.csv``) built by
``usgs_maps build-cache`` and serves the interactive dashboard.
"""

from __future__ import annotations

import argparse
import asyncio
import logging
from functools import lru_cache

logger = logging.getLogger(__name__)


@lru_cache(maxsize=1)
def _shared_usgs_catalog():
    """Read the immutable saved USGS catalog once per server process."""
    from usgs_maps import usgs

    catalog = usgs.Reader().read_saved_stations_info()
    return catalog.copy(deep=True)


def build_usgs_manager(time_range=None):
    """Build and return a fresh :class:`USGSDataUIManager` from the saved catalog."""
    from usgs_maps import usgs
    from usgs_maps.usgsuimgr import USGSDataUIManager

    reader = usgs.Reader()
    catalog = _shared_usgs_catalog().copy(deep=True)
    logger.info("build_usgs_manager: catalog has %d series", len(catalog))

    return USGSDataUIManager(catalog, reader, time_range=time_range)


def main():
    import cartopy.crs as ccrs
    import panel as pn
    import panel_material_ui as pmui
    from dvue.dataui import DataUI
    from dvue.session_persistence import install_session_handler, SessionManager

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
        "tabulator", "codeeditor", notifications=True, design="material",
    )
    crs_cartopy = ccrs.PlateCarree()
    install_session_handler(cookie_name="usgs_user_id")
    session_mgr = SessionManager(cookie_name="usgs_user_id", persist=False)

    def make_app():
        user_id = session_mgr.current_user_id
        reg_key = session_mgr.make_reg_key(user_id, "usgs")
        entry = session_mgr.get_entry(reg_key)
        if entry:
            template = entry["template"]
            ui = entry["ui"]

            def _reattach():
                ui.setup_location_sync()
                ui.setup_url_sync()

            pn.state.onload(_reattach)
            template.servable()
            return

        loading_status = pn.pane.Markdown(
            "**Loading saved USGS station and series metadata...**",
            sizing_mode="stretch_width",
            align="center",
        )
        main_panel = pn.Column(
            pn.indicators.LoadingSpinner(value=True, color="primary", size=50),
            loading_status,
            sizing_mode="stretch_both",
            align="center",
        )
        sidebar_panel = pn.Column(
            pn.indicators.LoadingSpinner(value=True, color="primary", size=50),
        )
        template = pmui.Page(
            title="USGS NWIS Map Explorer",
            sidebar=[sidebar_panel],
            main=[main_panel],
            sidebar_width=400,
            sidebar_open=False,
            sidebar_variant="auto",
        )

        async def _load_app():
            try:
                await asyncio.sleep(0)
                loading_status.object = (
                    "**Preparing the USGS catalog and available time ranges...**"
                )
                await asyncio.sleep(0)
                mgr = build_usgs_manager()
                loading_status.object = (
                    "**Building the station table, map, and plot controls...**"
                )
                await asyncio.sleep(0)
                ui = DataUI(
                    mgr,
                    crs=crs_cartopy,
                    station_id_column="site_no",
                    initial_map_extent=(-122.65, 37.55, -121.15, 38.75),
                )
                ui_page = ui.create_view(title="USGS NWIS Map Explorer")
                sidebar_panel.objects = list(ui_page.sidebar)
                main_panel.objects = list(ui_page.main)
                template.header = list(ui_page.header)
                ui_page.sidebar, ui_page.main, ui_page.header = [], [], []
                if user_id:
                    session_mgr.set_entry(
                        reg_key,
                        {"template": template, "ui": ui, "main_panel": main_panel},
                    )
            except Exception:
                import traceback

                logger.error("_load_app failed:\n%s", traceback.format_exc())
                main_panel.objects = [
                    pn.pane.Markdown(
                        "## Error loading application\n\nCheck the server logs."
                    )
                ]

        pn.state.onload(_load_app)
        template.servable()

    pn.serve(
        {"usgs-map-explorer": make_app},
        address=args.address,
        port=args.port,
        show=not args.no_show,
        allow_websocket_origin=["*"],
        title="USGS NWIS Map Explorer",
        # Bokeh's default (300s) expires the reconnect token after a few
        # minutes idle; the browser's automatic reconnect then fails
        # permanently with "Token is expired" until the page is reloaded.
        unused_session_lifetime_milliseconds=1_800_000,
        session_token_expiration=2_592_000,
    )


if __name__ == "__main__":
    main()
