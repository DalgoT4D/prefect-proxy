# prefect-proxy

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Code coverage badge](https://img.shields.io/codecov/c/github/DalgoT4D/prefect-proxy/main.svg)](https://codecov.io/gh/DalgoT4D/prefect-proxy/branch/main)
[![DeepSource](https://app.deepsource.com/gh/DalgoT4D/prefect-proxy.svg/?label=active+issues&show_trend=true&token=2GpMBhrZhOTX8-sWY9yJWDXY)](https://app.deepsource.com/gh/DalgoT4D/prefect-proxy/?ref=repository-badge)

Since Prefect exposes an async Python interface and Django does not play well with async functions, we split the Prefect interface off into a FastAPI project

These endpoints will be called only from the Django server or from testing scripts. More project documentation can be found in [the wiki](https://github.com/DalgoT4D/prefect-proxy/wiki)


## Installation instructions

Install the Python dependencies

    pip install -r requirements.txt

## Run instructions

Start Prefect on port 4200

    prefect server start

and set `PREFECT_API_URL` in `.env` to `http://localhost:4200/api`. Change the port in this URL if you are running Prefect on a different port.

Next, start a Prefect agent

    prefect agent start -q ddp --pool default-agent-pool

The proxy server needs to listen for requests coming from Django; pick an available port and run

    gunicorn proxy.main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:<port number>

Make sure to add this port number into the `.env` for DDP_backend in the variable `PREFECT_PROXY_API_URL`.

## For developers

FastAPI endpoints are defined in `main.py`. These typically call functions in `service.py`.

Most communication with Prefect is via its SDK, but occasionally we need to make HTTP requests; these are done using

- `service.prefect_get`
- `service.prefect_post`
- `service.prefect_patch`
- `service.prefect_delete`

The Prefect API's base URL is set in `.env` via the variable `PREFECT_API_URL`.

Logs are sent to a single logfile called `prefect-proxy.log` which is written to the `LOGDIR` specified in the `.env`.

Tests are run via `pytest`:

    GOOGLE_APPLICATION_CREDENTIALS=<your/credentials.json> pytest
