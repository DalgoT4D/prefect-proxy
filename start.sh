#!/bin/sh

GOOGLE_APPLICATION_CREDENTIALS=../../secrets/gcp.json
uv run gunicorn proxy.main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8500