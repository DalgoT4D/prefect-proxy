#!/bin/sh

/home/ddp/prefect-proxy/venv/bin/gunicorn main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:8080