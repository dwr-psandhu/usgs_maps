import pandas as pd
from .nwis_cache import cache_to_file

def infer_dtypes(row0):
    column_types = row0.to_dict()
    for k in column_types:
        if column_types[k].endswith('s'):
            column_types[k] = 'category'
        elif column_types[k].endswith('n'):
            column_types[k] = 'float'
        elif column_types[k].endswith('d'):
            column_types[k] = 'datetime64[ns]'
    return column_types


def parse_param_table(data_url):
    dfcomments = pd.read_csv(data_url, sep='#', nrows=100)

    dfcomments = dfcomments[dfcomments.iloc[:, 0].isna()].iloc[:, 1]
    dfcomments

    param_table_begin = dfcomments[dfcomments.str.find(
        'TS_ID       Parameter Description') > 0].index[0]
    # dfcomments.str.split('\s+',expand=True)

    dfparam = dfcomments.iloc[param_table_begin:]
    param_table_end = dfparam[dfparam.isna()].index[0]

    dfparams = dfcomments.iloc[param_table_begin:param_table_end]
    param_columns = dfparams.iloc[0:1].str.strip().str.split().iloc[0]
    col_index = [dfparams.iloc[0].find(col) for col in param_columns]

    df = pd.DataFrame(columns=param_columns)

    for i in range(len(col_index) - 1):
        df.iloc[:, i] = (dfparams.iloc[1:].str.slice(col_index[i], col_index[i + 1])).str.strip()
    df.iloc[:, len(col_index) - 1] = dfparams.iloc[1:].str.slice(col_index[len(col_index) - 1],).str.strip()
    return df.reset_index().drop(columns='index')


def read_rdb(file):
    df = pd.read_csv(file, sep='\t', comment='#')
    # infer types from 1 line after header and then drop it
    return df.iloc[1:].reset_index(drop=True).astype(dtype=infer_dtypes(df.iloc[0]))


class NWISReader:
    def __init__(self, base_url = 'https://waterservices.usgs.gov/nwis'):
        self.base_url = base_url

    @cache_to_file(expires='1D')
    def read_stations_for_state(self, state='ca'):
        stations_list_file = f'{self.base_url}/site/?format=rdb,1.0&stateCd={state}&siteStatus=all'
        return read_rdb(stations_list_file)

    def _undecorated_read_station_data(self, siteid, start_date, end_date):
        if end_date == '':
            end_param=''
        else:
            end_param=f'&endDT={end_date}'
        data_url=f'{self.base_url}/iv/?format=rdb,1.0&sites={siteid}&startDT={start_date}{end_param}&siteStatus=all'
        df = read_rdb(data_url)
        df = df.set_index('datetime')
        return df

    def read_entire_station_data_for(self, siteid):
        return self._undecorated_read_station_data(siteid, '1900-01-01','')

    @cache_to_file(expires='1H')
    def read_station_data(self, siteid, start_date, end_date):
        return self._undecorated_read_station_data(siteid, start_date, end_date)

    @cache_to_file(expires='1D')
    def read_station_parameters(self, siteid):
        data_url=f'{self.base_url}/iv/?format=rdb,1.0&sites={siteid}&siteStatus=all'
        return parse_param_table(data_url)

    @cache_to_file(expires='1D')
    def read_station_detailed_info(self, siteid):
        data_url = f'{self.base_url}/site/?format=rdb,1.0&sites={siteid}&seriesCatalogOutput=true&siteStatus=all'
        return read_rdb(data_url)
        
    @cache_to_file(expires='1D')
    def read_site_type_codes(self):
        # site type code
        site_type_url='https://help.waterdata.usgs.gov/code/site_tp_query?fmt=html'
        dflist=pd.read_html(site_type_url)
        assert len(dflist) == 1
        df=dflist[0]
        site_type_dtype_map = {'Site Tp Cd': 'category',
        'Site Tp Srt Nu': 'int',
        'Site Tp Vld Fg': 'category',
        'Site Tp Prim Fg': 'category',
        'Site Tp Nm': 'string',
        'Site Tp Ln': 'string',
        'Site Tp Ds': 'string'}
        df.astype(dtype=site_type_dtype_map)
        return df