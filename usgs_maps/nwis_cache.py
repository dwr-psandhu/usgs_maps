"""Copied from cdec_cache from cdec_maps. 
FIXME: Need to combine common logic from both as they deal with data frames
    """
import functools
import os
import glob
import pandas as pd
from decorator import decorator

@decorator
def cache_to_file(func, list=False, expires='1D', cache_dir='usgs_cache', *args, **kwargs):
    """Caches Dataframe returned by function to filename (name of function + '.pkl')

    Cache expires after "expires" Timedelta at which point the function is called and refreshes the cache before returning.
    Args:
        list (boolean, optional): Indicates that the function returns a list (or other iterable) of DataFrame(s)
        expires (str, optional): [Cache expires after the Timedelta str]. Defaults to '1D'
        cache_dir (str, optional): Directory in which to store the pickled dataframe.
    """
    def expired(mtime):
        return pd.Timestamp.now() - pd.Timestamp.fromtimestamp(mtime) > pd.to_timedelta(expires)

    def ensure_dir(cache_dir):
        if not os.path.exists(cache_dir):
            os.mkdir(cache_dir)

    def needs_creation(fname):
        return not os.path.exists(fname)

    def needs_updating(fname):
        return expired(os.path.getmtime(fname))

    def needs_refresh(fname):
        return needs_creation(fname) or needs_updating(fname)
    #

    def read_cache(fname):
        df = pd.read_pickle(fname)
        return df
    #

    def write_cache(df, fname):
        df.to_pickle(fname)
    #
    ensure_dir(cache_dir)
    if list:
        cached_files = glob.glob(f"{cache_dir}/{func.__name__}.{args[1]}.*.pkl")
        if not cached_files or any([needs_refresh(fname) for fname in cached_files]):
            result = func(*args, **kwargs)
            for i, r in enumerate(result):
                write_cache(r, f"{cache_dir}/{func.__name__}.{args[1]}.{i}.pkl")
        else:
            result = [read_cache(fname) for fname in cached_files]
    else:
        if func.__name__ == 'read_station_data':
            cache_file = f"{cache_dir}/{args[1]}.pkl"
        else:
            cache_file = f"{cache_dir}/{func.__name__}.pkl"
        if needs_creation(cache_file):
            if func.__name__ == 'read_station_data':  # then cache and fetch differently
                # station_id, sensor_number, duration_code
                result = args[0].read_entire_station_data_for(args[1])
            else:
                result = func(*args, **kwargs)
            write_cache(result, cache_file)
        elif needs_updating(cache_file):
            if func.__name__ == 'read_station_data':  # need update strategy here
                result = read_cache(cache_file)
                if result.empty: # edge case if cache file has no data :(
                    result = args[0].read_entire_station_data_for(args[1])
                else:
                    sdate = result.index[-1].strftime('%Y-%m-%d')
                    dflatest = args[0]._undecorated_read_station_data(
                        args[1], sdate, '')
                    result = dflatest.combine_first(result)  # updates with latest fetched
            else:
                result = func(*args, **kwargs)
            write_cache(result, cache_file)
        else:
            result = read_cache(cache_file)
        if func.__name__ == 'read_station_data':  # then subset to desired time window
            start, end = pd.to_datetime(args[2]), pd.to_datetime(args[3])
            result = result[(result.index >= start) & (result.index <= end)] # more robust then result.loc[pd.to_datetime(start):pd.to_datetime(end)]
    return result
