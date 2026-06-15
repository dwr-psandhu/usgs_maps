"""Panel ``.servable()`` entry point for usgs_maps.

Launch with::

    panel serve usgs_maps/usgs_maps_servable.py --show
"""

import panel as pn

pn.extension("tabulator", "codeeditor", notifications=True, design="native")

from usgs_maps import usgsuimgr  # noqa: E402

view, _ui = usgsuimgr.show_usgs_ui()
view.servable(title="USGS NWIS Map Explorer")
