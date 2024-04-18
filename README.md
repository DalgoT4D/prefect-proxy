# prefect-proxy

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Code coverage badge](https://img.shields.io/codecov/c/github/DalgoT4D/prefect-proxy/main.svg)](https://codecov.io/gh/DalgoT4D/prefect-proxy/branch/main)
[![DeepSource](https://app.deepsource.com/gh/DalgoT4D/prefect-proxy.svg/?label=active+issues&show_trend=true&token=2GpMBhrZhOTX8-sWY9yJWDXY)](https://app.deepsource.com/gh/DalgoT4D/prefect-proxy/?ref=repository-badge)

Since Prefect exposes an async Python interface and Django does not play well with async functions, we split the Prefect interface off into a FastAPI project

These endpoints will be called only from the Django server or from testing scripts. More project documentation can be found in [the wiki](https://github.com/DalgoT4D/prefect-proxy/wiki)

## Run instructions

There are two options to setup and run prefect server, proxy and the agent:

- Running all the three services manually and independently
- Running using Docker

Below are instructions on each the two options

### Running services manually

#### Instructions

Clone the [Prefect Proxy](https://github.com/DalgoT4D/prefect-proxy) repository

In the cloned repository, open the terminal and run the following commands:

- `pyenv local 3.10`

- `pyenv exec python -m venv venv`

- `source venv/bin/activate`

- `pip install --upgrade pip`

- `pip install -r requirements.txt`

- create `.env` from `.env.template`
- set the value for the `LOGDIR` in the `.env` file with the name of the directory to hold the logs. The directory will be automatically created on running the prefect proxy

#### Start Prefect Server

Start Prefect on port 4200

    prefect server start

and set `PREFECT_API_URL` in `.env` to `http://localhost:4200/api`. Change the port in this URL if you are running Prefect on a different port.

#### Start Prefect Agent

Next, on a different terminal start the Prefect agent

    prefect agent start -q ddp --pool default-agent-pool

#### Start Prefect Proxy

The proxy server needs to listen for requests coming from Django; pick an available port and run

    gunicorn proxy.main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:<port number>

Make sure to add this port number into the `.env` for DDP_backend in the variable `PREFECT_PROXY_API_URL`.

## Running using Docker

The second option is to run all the three services as docker containers. This is the easiest as it's a 1-step process.
Perform the following steps:

- change directory into the docker_compose folder
- create an `.env` file from `.env.template` in this repository
- populate all the variables in the `.env` file
- from the terminal run the docker compose command below below to start the three services

```
docker compose up -d
```

The docker compose pulls all the necessary docker images from Dockerhub. However, the prefect proxy has a Dockerfile that you can build an image locally.
To Build and use this image, run the below command from the root directory:

```
docker build --no-cache=true --build-arg BUILD_DATE=$(date -u +'%Y-%m-%dT%H:%M:%SZ') --build-arg BUILD_VERSION=0.0.1  -t prefect_proxy:latest .
```

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
