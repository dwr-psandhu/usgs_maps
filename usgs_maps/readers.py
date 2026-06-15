"""dvue plugin registration for usgs_maps.

Imported by dvue at startup via the ``dvue.plugins`` entry point group.
Registers the USGS NWIS reader with the ReaderRegistry so that the catalog
can be scanned with ``dvue ui usgs:<spec>``.

Entry point (declared in pyproject.toml)::

    [project.entry-points."dvue.plugins"]
    usgs_maps = "usgs_maps.readers:register_readers"

Once installed, the reader is auto-discovered::

    dvue ui --list-plugins
"""


def register_readers():
    """Register usgs_maps readers with dvue.

    Registers:
    - USGS NWIS reader (ref_type="usgs", no file extensions — API-based source)
    """
    from dvue.registry import ReaderRegistry
    from usgs_maps.usgsuimgr import USGSDataReferenceReader

    ReaderRegistry.register(
        "usgs",
        USGSDataReferenceReader,
    )
