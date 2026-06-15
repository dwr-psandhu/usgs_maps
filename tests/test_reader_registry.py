"""dvue plugin / ReaderRegistry wiring tests for usgs_maps."""

from usgs_maps import readers


def test_register_readers_wires_usgs():
    from dvue.registry import ReaderRegistry
    from usgs_maps.usgsuimgr import USGSDataReferenceReader

    readers.register_readers()
    assert ReaderRegistry.has_ref_type("usgs")
    assert ReaderRegistry._registry["usgs"] is USGSDataReferenceReader


def test_reader_catalog_crs():
    from usgs_maps.usgsuimgr import USGSDataReferenceReader

    assert USGSDataReferenceReader.catalog_crs() == "EPSG:4326"
