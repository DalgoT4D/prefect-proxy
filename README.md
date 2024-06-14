# prefect-proxy

[![License: AGPL v3](https://img.shields.io/badge/License-AGPL%20v3-blue.svg)](https://www.gnu.org/licenses/agpl-3.0)
[![Code coverage badge](https://img.shields.io/codecov/c/github/DalgoT4D/prefect-proxy/main.svg)](https://codecov.io/gh/DalgoT4D/prefect-proxy/branch/main)
[![DeepSource](https://app.deepsource.com/gh/DalgoT4D/prefect-proxy.svg/?label=active+issues&show_trend=true&token=2GpMBhrZhOTX8-sWY9yJWDXY)](https://app.deepsource.com/gh/DalgoT4D/prefect-proxy/?ref=repository-badge)

Since Prefect exposes an async Python interface and Django does not play well with async functions, we split the Prefect interface off into a FastAPI project

These endpoints will be called only from the Django server or from testing scripts. More project documentation can be found in [the wiki](https://github.com/DalgoT4D/prefect-proxy/wiki)

## Installation instructions

Clone the [Prefect Proxy](https://github.com/DalgoT4D/prefect-proxy) repository

In the cloned repository, run the following commands:

- `pyenv local 3.10`

- `pyenv exec python -m venv venv`

- `source venv/bin/activate`

- `pip install --upgrade pip`

- `pip install -r requirements.txt`

- create `.env` from `.env.template`
- set the value for the `LOGDIR` in the `.env` file with the name of the directory to hold the logs. The directory will be automatically created on running the prefect proxy

## Run instructions

Start Prefect on port 4200

    prefect server start

and set `PREFECT_API_URL` in `.env` to `http://localhost:4200/api`. Change the port in this URL if you are running Prefect on a different port.

Next, start Prefect worker(s). On production, Dalgo runs 3 workers on two queues `ddp` & `manual-db`. Make sure to setup the work pool from the prefect UI

    prefect worker start -q ddp --pool dalgo_work_pool

    prefect worker start -q manual-dbt --pool dalgo_work_pool

The proxy server needs to listen for requests coming from Django; pick an available port and run

    gunicorn proxy.main:app --workers 4 --worker-class uvicorn.workers.UvicornWorker --bind 0.0.0.0:<port number>

Make sure to add this port number into the `.env` for DDP_backend in the variable `PREFECT_PROXY_API_URL`.

## Dalgo Webhook configuration

All orchestration flow runs (scheduled or manual) are executed in prefect by the workers. Dalgo needs to be notified when these flow runs reach a terminal state (success or failure) to clear up resources & notify users of failures. 

Steps to create a webhook in prefect
1. Go to the prefect UI & head over to `Notifications`
2. Add a new notification of type `Custom Webhook`.
3. Set `Webhook URL` to `http://localhost:8002/webhooks/v1/notification/`. Assuming Dalgo backend runs on `http://localhost:8002`
4. Set `Method` to `POST`
5. Set the `Headers` to . The notification key here should be the one set in Dalgo backend `.env` under `PREFECT_NOTIFICATIONS_WEBHOOK_KEY`

    ```
    {"X-Notification-Key":"dev123"}
    ```
6. Set the `JSON Data` to

    ```
    {"body":"{{body}}"}
    ```
7. `Run states` that we are interested in are `Completed`, `Cancelled`, `Crashed`, `Failed`, `TimedOut`
8. Hit Save
9. Make sure this notification has the below specified message_template, if not update it. Use can use these two api of prefect `GET /api/flow_run_notification_policies/{id}` and `PATCH /api/flow_run_notification_policies/{id}`

    ```
    Flow run {flow_run_name} with id {flow_run_id} entered state {flow_run_state_name}
    ```

## Preface

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
