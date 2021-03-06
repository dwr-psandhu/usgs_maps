from usgs_maps import nwis
import pytest

def test_stations():
    r = nwis.NWISReader()
    df = r.read_stations_for_state('ca')
    assert not df.empty
    assert len(df) > 80000 # lots of stations
    dfbenicia = df[df.site_no=='11455780']
    assert not dfbenicia.empty
    assert len(dfbenicia) == 1
    assert dfbenicia.station_nm.str.contains('BENICIA').all()


site_test_data = [('11455780','2021-10-01','2021-10-15')]
@pytest.mark.parametrize('siteid,start_date,end_date',site_test_data)
def test_station_data_with_dates(siteid,start_date,end_date):
    r = nwis.NWISReader()
    df = r.read_station_data(siteid, start_date, end_date)
    assert not df.empty
    assert len(df) > 10


@pytest.mark.parametrize('siteid',['11455780'])
def test_station_data(siteid):
    r = nwis.NWISReader()
    dfparams = r.read_station_parameters(siteid)
    assert not dfparams.empty
