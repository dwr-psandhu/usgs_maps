git config --global --add safe.directory /home/data/usgs-maps
echo "pip installing this directory: $(pwd)"
SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 pip install --no-deps .
echo "starting build_usgs_cache.py"
python build_usgs_cache.py --state ca & # background build; locks cache db while running
# CARTO_API_KEY must be set in the environment (get a key at https://carto.com/basemaps/apikey)
# or basemap tiles will show CARTO's "API key required" watermark.
echo "starting panel serve usgs_maps/usgs_maps_servable.py"
panel serve usgs_maps/usgs_maps_servable.py --address 0.0.0.0 --port 80 --allow-websocket-origin="*"
