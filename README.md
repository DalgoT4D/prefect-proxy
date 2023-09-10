# prefect-proxy

[![Code coverage badge](https://img.shields.io/codecov/c/github/DalgoT4D/prefect-proxy/main.svg)](https://codecov.io/gh/DalgoT4D/prefect-proxy/branch/main)
[![DeepSource](https://app.deepsource.com/gh/DalgoT4D/prefect-proxy.svg/?label=active+issues&show_trend=true&token=2GpMBhrZhOTX8-sWY9yJWDXY)](https://app.deepsource.com/gh/DalgoT4D/prefect-proxy/?ref=repository-badge)

Since Prefect exposes an async Python interface and Django does not play well with async functions, we split the Prefect interface off into a FastAPI project

These endpoints will be called only from the Django server or from testing scripts

    pip install -r requirements.txt

    uvicorn main:app --reload --port <port number>

make sure to add this port number into the .env for DDP_backend using the variable PREFECT_PROXY_API_URL

You also need to run a celery worker against the queue called "proxy"

    celery -A main.celery worker -Q proxy
  
More project documentation can be found at https://github.com/DalgoT4D/prefect-proxy/wiki


To run tests

    pytest
