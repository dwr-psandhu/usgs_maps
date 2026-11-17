#!/bin/bash
# Entry point for the scheduled cache-build job (see deploy_cache_job.ps1).
# Runs on the shared data share and exits non-zero on failure so the job is
# marked failed. The web app (run_server.sh) only serves; it never builds.
# Extra arguments are passed to build_usgs_cache.py; the default is --state ca.
set -eu
cd "$(dirname "$0")"
git config --global --add safe.directory "$(pwd)"
SETUPTOOLS_SCM_PRETEND_VERSION=0.0.0 pip install --no-deps .
if [ "$#" -eq 0 ]; then
    set -- --state ca
fi
exec python build_usgs_cache.py "$@"
