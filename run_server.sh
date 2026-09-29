git config --global --add safe.directory /home/data/usgs-maps
pip install git+https://github.com/CADWRDeltaModeling/vtools3
pip install --force-reinstall git+https://github.com/CADWRDeltaModeling/dvue.git#egg=dvue --no-deps
SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 pip install --no-deps .
# CARTO_API_KEY must be set in the environment (get a key at https://carto.com/basemaps/apikey)
# or basemap tiles will show CARTO's "API key required" watermark.
python usgsui.py --address 0.0.0.0 --port 80 --no-show
