"""Route handlers"""

import os
import base64
import requests
from fastapi import FastAPI, HTTPException
from prefect_airbyte import AirbyteConnection
import sentry_sdk
from proxy.helpers import CustomLogger, deployment_to_json
from proxy.exception import PrefectException


from proxy.service import (
    get_airbyte_server_block,
    get_airbyte_server_block_id,
    create_airbyte_server_block,
    put_deployment_v1,
    update_deployment_entrypoint,
    post_deployment_v1,
    get_flow_runs_by_deployment_id,
    get_deployments_by_filter,
    get_flow_run_logs,
    get_flow_run_logs_v2,
    get_flow_run_tasks,
    post_deployment_flow_run,
    get_flow_runs_by_name,
    set_deployment_schedule,
    get_deployment,
    get_deployment_scheduled_flow_runs,
    get_flow_run,
    retry_flow_run,
    create_secret_block,
    get_secret_block_by_name,
    get_secret_block_contents,
    upsert_secret_block,
    delete_flow_run,
    get_long_running_flow_runs,
    get_current_prefect_version,
    update_airbyte_server_block,
    upsert_airbyte_connection_block,
    set_cancel_queued_flow_run,
    filter_late_flow_runs,
    filter_prefect_workers,
)
from proxy.schemas import (
    AirbyteServerCreate,
    AirbyteServerUpdate,
    AirbyteConnectionCreate,
    RunDbtCoreOperation,
    RunShellOperation,
    DeploymentCreate2,
    DeploymentFetch,
    FlowRunRequest,
    RetryFlowRunRequest,
    PrefectSecretBlockCreate,
    PrefectSecretBlockEdit,
    DeploymentUpdate2,
    ScheduleFlowRunRequest,
    CancelQueuedManualJob,
    FilterLateFlowRuns,
    FilterPrefectWorkers,
)

from proxy.prefect_flows_runner import dbtjob_v2_runner, shellopjob


sentry_sdk.init(
    dsn=os.getenv("SENTRY_DSN"),
    # Set traces_sample_rate to 1.0 to capture 100%
    # of transactions for performance monitoring.
    traces_sample_rate=float(os.getenv("SENTRY_TSR", "1.0")),
    # Set profiles_sample_rate to 1.0 to profile 100%
    # of sampled transactions.
    # We recommend adjusting this value in production.
    profiles_sample_rate=float(os.getenv("SENTRY_PSR", "1.0")),
)

app = FastAPI()

logger = CustomLogger("prefect-proxy")


# sentry test debug endpoint
@app.get("/sentry-debug")
async def trigger_error():
    """endpoint to test sentry"""
    division_by_zero = 1 / 0  # pylint: disable=unused-variable


# =============================================================================
def dbtrun_v1(task_config: RunDbtCoreOperation):
    """Run a dbt core flow"""

    if not isinstance(task_config, RunDbtCoreOperation):
        raise TypeError("invalid task config")
    logger.info("dbt core operation running %s", task_config.slug)

    # Ignore payload.flow_name / payload.flow_run_name — use whatever the
    # runner decorator declares (name="dbtjob_v2_runner",
    # flow_run_name="dbtjob-{task_slug}").
    try:
        result = dbtjob_v2_runner(task_config.model_dump(), task_config.slug)
        return result
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail=f"failed to run dbt core flow {task_config.slug}"
        ) from error


def shelloprun(task_config: RunShellOperation):
    """Run a shell operation flow"""
    if not isinstance(task_config, RunShellOperation):
        raise TypeError("invalid task config")

    # Ignore payload.flow_name / payload.flow_run_name — use the runner
    # decorator's (name="shellopjob", flow_run_name="shellop-{task_slug}").
    try:
        result = shellopjob(task_config.model_dump(), task_config.slug)
        return result
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to run shell operation flow") from error


# =============================================================================
@app.get("/proxy/blocks/airbyte/server/{blockname}")
async def get_airbyte_server(blockname: str):
    """Look up an Airbyte server block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    try:
        block_id = await get_airbyte_server_block_id(blockname)
    except Exception as error:
        logger.error(
            "Failed to get Airbyte server block ID for block name %s: %s",
            blockname,
            str(error),
        )
        raise HTTPException(status_code=500, detail="Internal server error") from error

    if block_id is None:
        return {"block_id": None}
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.get("/proxy/blocks/airbyte/server/block/{blockname}")
async def get_airbyte_server_block_config(blockname: str):
    """Look up an Airbyte server block by name and return block"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    try:
        block = await get_airbyte_server_block(blockname)
    except Exception as error:
        logger.error(
            "Failed to get Airbyte server block for block name %s: %s",
            blockname,
            str(error),
        )
        raise HTTPException(status_code=500, detail="Internal server error") from error

    if block is None:
        raise HTTPException(status_code=404, detail="block not found") from error
    logger.info("blockname => block : %s => %s", blockname, block)

    token_string = f"{block.username}:{block.password.get_secret_value()}"
    token_string_bytes = token_string.encode("ascii")
    base64_bytes = base64.b64encode(token_string_bytes)
    base64_string_token = base64_bytes.decode("ascii")
    return {
        "host": block.server_host,
        "port": block.server_port,
        "version": block.api_version,
        "token": base64_string_token,
    }


@app.post("/proxy/blocks/airbyte/server/")
async def post_airbyte_server(payload: AirbyteServerCreate):
    """
    create a new airbyte server block with this block name,
    raise an exception if the name is already in use
    """
    logger.info(payload)
    if not isinstance(payload, AirbyteServerCreate):
        raise TypeError("payload is invalid")
    try:
        block_id, cleaned_block_name = await create_airbyte_server_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create airbyte server block"
        ) from error
    logger.info("Created new airbyte server block with ID: %s", block_id)
    return {"block_id": block_id, "cleaned_block_name": cleaned_block_name}


@app.put("/proxy/blocks/airbyte/server/")
async def put_airbyte_server(payload: AirbyteServerUpdate):
    """
    create a new airbyte server block with this block name,
    raise an exception if the name is already in use
    """
    if not isinstance(payload, AirbyteServerUpdate):
        raise TypeError("payload is invalid")
    try:
        block_id, cleaned_block_name = await update_airbyte_server_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to update airbyte server block"
        ) from error
    logger.info("Created new airbyte server block with ID: %s", block_id)
    return {"block_id": block_id, "cleaned_block_name": cleaned_block_name}


@app.put("/proxy/blocks/airbyte/connection/")
async def put_airbyte_connection(payload: AirbyteConnectionCreate):
    """Upsert an airbyte connection block (create or overwrite)."""
    if not isinstance(payload, AirbyteConnectionCreate):
        raise TypeError("payload is invalid")
    try:
        block_id, cleaned_block_name = await upsert_airbyte_connection_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to upsert airbyte connection block"
        ) from error
    logger.info("Upserted airbyte connection block with ID: %s", block_id)
    return {"block_id": block_id, "cleaned_block_name": cleaned_block_name}


# =============================================================================
@app.get("/proxy/blocks/secret/{blockname}/contents")
async def get_secret_block_contents_route(blockname: str):
    """Return the value stored in a Secret block."""
    if not isinstance(blockname, str):
        raise TypeError("blockname is invalid")
    try:
        result = await get_secret_block_contents(blockname)
    except PrefectException as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch secret block contents"
        ) from error
    return result


@app.get("/proxy/blocks/secret/{blockname}")
async def get_secret_block(blockname: str):
    """Look up a Secret block by name. Returns {block_id, block_name} or 404
    if not found."""
    if not isinstance(blockname, str):
        raise TypeError("blockname is invalid")
    try:
        result = await get_secret_block_by_name(blockname)
    except PrefectException as error:
        raise HTTPException(status_code=404, detail=str(error)) from error
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to fetch secret block") from error
    return result


# =============================================================================
@app.post("/proxy/blocks/secret/")
async def post_secret_block(payload: PrefectSecretBlockCreate):
    """create a new prefect secret block with this block name to store a secret string"""
    if not isinstance(payload, PrefectSecretBlockCreate):
        raise TypeError("payload is invalid")
    try:
        block_id, cleaned_blockname = await create_secret_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to prefect secret block") from error
    logger.info(
        "Created new secret block with ID: %s and name: %s",
        block_id,
        cleaned_blockname,
    )
    return {"block_id": block_id, "block_name": cleaned_blockname}


# =============================================================================
@app.put("/proxy/blocks/secret/")
async def put_secret_block(payload: PrefectSecretBlockEdit):
    """create a new prefect secret block with this block name to store a secret string"""
    if not isinstance(payload, PrefectSecretBlockEdit):
        raise TypeError("payload is invalid")
    try:
        block_id, cleaned_blockname = await upsert_secret_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to prefect secret block") from error
    logger.info(
        "Edited secret block with ID: %s and name: %s",
        block_id,
        cleaned_blockname,
    )
    return {"block_id": block_id, "block_name": cleaned_blockname}


# =============================================================================
@app.delete("/delete-a-block/{block_id}")
async def delete_block(block_id):
    """we can break this up into four different deleters later if we want to"""
    if not isinstance(block_id, str):
        raise TypeError("block_id must be a string")
    root = os.getenv("PREFECT_API_URL")
    logger.info("DELETE %s/block_documents/%s", root, block_id)
    res = requests.delete(f"{root}/block_documents/{block_id}", timeout=10)
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=res.text) from error


# =============================================================================
@app.post("/proxy/v1/flows/dbtcore/run/")
def post_run_dbtcore_flow_v1(payload: RunDbtCoreOperation):
    """Prefect flow to run dbt"""
    logger.info(payload)
    if not isinstance(payload, RunDbtCoreOperation):
        raise TypeError("payload is invalid")

    logger.info("running dbtcore-run for dbt-core-op %s", payload.slug)
    try:
        result = dbtrun_v1(payload)
        logger.info(result)
        return {"status": "success", "result": result}
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/proxy/flows/shell/run/")
def post_run_shellop_flow(payload: RunShellOperation):
    """Prefect flow to run dbt"""
    logger.info(payload)
    if not isinstance(payload, RunShellOperation):
        raise TypeError("payload is invalid")

    logger.info("running shell operation")
    try:
        result = shelloprun(payload)
        logger.info(result)
        return {"status": "success", "result": result}
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/proxy/v1/deployments/")
def post_dataflow_v1(payload: DeploymentCreate2):
    """Create a deployment from an existing flow"""
    if not isinstance(payload, DeploymentCreate2):
        raise TypeError("payload is invalid")

    logger.info(payload)
    try:
        deployment = post_deployment_v1(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to create deployment") from error
    logger.info("Created new deployment: %s", deployment)
    return {"deployment": deployment}


@app.put("/proxy/v1/deployments/{deployment_id}")
def put_dataflow_v1(deployment_id, payload: DeploymentUpdate2):
    """updates a deployment"""
    if not isinstance(payload, DeploymentUpdate2):
        raise TypeError("payload is invalid")

    logger.info(payload)
    try:
        put_deployment_v1(deployment_id, payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to update the deployment") from error
    logger.info("Updated the deployment: %s", deployment_id)
    return {"success": 1}


@app.patch("/proxy/v1/deployments/{deployment_id}/entrypoint")
def patch_deployment_entrypoint(deployment_id: str, payload: dict):
    """PATCH a deployment's entrypoint. Used by the runner-flow migration script."""
    entrypoint = payload.get("entrypoint") if isinstance(payload, dict) else None
    if not isinstance(entrypoint, str) or not entrypoint:
        raise HTTPException(status_code=400, detail="entrypoint (string) is required")
    try:
        update_deployment_entrypoint(deployment_id, entrypoint)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to update deployment entrypoint"
        ) from error
    return {"success": 1}


@app.get("/proxy/v1/deployments/get_scheduled_flow_runs")
def get_dataflow_scheduled_flow_runs(deployment_id: str):
    """fetch scheduled flow-runs for a deployment"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    try:
        res = get_deployment_scheduled_flow_runs(deployment_id)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch scheduled flow-runs for deployment"
        ) from error
    return {"flow_runs": res}


@app.post("/proxy/flow_run/")
async def get_flowrun(payload: FlowRunRequest):
    """look up a flow run by name and return id if found"""
    if not isinstance(payload, FlowRunRequest):
        raise TypeError("payload is invalid")

    logger.info("flow run name=%s", payload.name)
    try:
        flow_runs = get_flow_runs_by_name(payload.name)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to fetch flow_runs by name") from error
    if flow_runs:
        if len(flow_runs) > 1:
            logger.error("multiple flow names having name %s", payload.name)
        return {"flow_run": flow_runs[0]}
    logger.error("no flow_runs having name %s", payload.name)
    raise HTTPException(status_code=400, detail="no such flow run")


@app.get("/proxy/flow_runs")
def get_flow_runs(deployment_id: str, limit: int = 0, start_time_gt: str = ""):
    """Get Flow Runs for a deployment"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    if not isinstance(limit, int):
        raise TypeError("limit must be an integer")
    if limit < 0:
        raise ValueError("limit must be positive")
    try:
        flow_runs = get_flow_runs_by_deployment_id(deployment_id, limit, start_time_gt)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch flow_runs for deployment"
        ) from error
    return {"flow_runs": flow_runs}


@app.post("/proxy/flow_runs/late")
def post_late_flow_runs(query: FilterLateFlowRuns):
    """Get Late flow Runs"""
    try:
        flow_runs = filter_late_flow_runs(query)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to fetch late flow runs") from error
    return {"flow_runs": flow_runs}


@app.get("/proxy/flow_runs/{flow_run_id}")
def get_flow_run_by_id(flow_run_id):
    """Get a flow run"""
    if not isinstance(flow_run_id, str):
        raise TypeError("Flow run id must be a string")

    try:
        flow_run = get_flow_run(flow_run_id=flow_run_id)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch flow_run " + flow_run_id
        ) from error

    return flow_run


@app.get("/proxy/flow_runs/{flow_run_id}/poll")
def get_flow_run_by_id_poll(flow_run_id):
    """Lightweight api that can be used in polling to figure out flow run state"""
    if not isinstance(flow_run_id, str):
        raise TypeError("Flow run id must be a string")

    try:
        flow_run = get_flow_run(flow_run_id=flow_run_id, update_state_from_task_runs=False)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch flow_run " + flow_run_id
        ) from error

    return flow_run


@app.delete("/proxy/flow_runs/{flow_run_id}")
def delete_deployment_flow_run(flow_run_id):
    """Get a flow run"""
    if not isinstance(flow_run_id, str):
        raise TypeError("Flow run id must be a string")

    try:
        delete_flow_run(flow_run_id=flow_run_id)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch flow_run " + flow_run_id
        ) from error

    return {"success": 1}


@app.post("/proxy/flow_runs/{flow_run_id}/retry")
def post_retry_flow_run(flow_run_id: str, payload: RetryFlowRunRequest):
    """Retry a flow run; after x mins"""
    try:
        retry_flow_run(flow_run_id=flow_run_id, minutes=payload.minutes)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch flow_run " + flow_run_id
        ) from error

    return {"success": 1}


@app.post("/proxy/deployments/filter")
def post_deployments(payload: DeploymentFetch):
    """Get deployments by various filters"""
    logger.info(payload)
    if not isinstance(payload, DeploymentFetch):
        raise TypeError("payload is invalid")
    try:
        deployments = get_deployments_by_filter(
            org_slug=payload.org_slug, deployment_ids=payload.deployment_ids
        )
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to filter deployments") from error
    logger.info("Found deployments with payload: %s", payload)
    return {"deployments": deployments}


@app.get("/proxy/flow_runs/logs/{flow_run_id}")
def get_flow_run_logs_paginated(
    flow_run_id: str,
    task_run_id: str = "",
    limit: int = 0,
    offset: int = 0,
):
    """paginate the logs from a flow run"""
    if not isinstance(flow_run_id, str):
        raise TypeError("flow_run_id must be a string")
    if not isinstance(task_run_id, str):
        raise TypeError("task_run_id must be a string")
    if not isinstance(offset, int):
        raise TypeError("offset must be an integer")
    if not isinstance(limit, int):
        raise TypeError("limit must be an integer")
    if offset < 0:
        raise ValueError("offset must be positive")
    if limit < 0:
        raise ValueError("limit must be positive")
    logger.info(
        "flow_run_id=%s, task_run_id=%s, limit=%s, offset=%s",
        flow_run_id,
        task_run_id,
        limit,
        offset,
    )
    try:
        return get_flow_run_logs(flow_run_id, task_run_id, limit, offset)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to fetch logs for flow_run") from error


@app.get("/proxy/flow_runs/v1/logs/{flow_run_id}")
def get_flow_run_logs_grouped(flow_run_id: str):
    """paginate the logs from a flow run"""
    if not isinstance(flow_run_id, str):
        raise TypeError("flow_run_id must be a string")

    try:
        return get_flow_run_logs_v2(flow_run_id)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to fetch logs for flow_run") from error


@app.get("/proxy/flow_runs/graph/{flow_run_id}")
def get_flow_run_graph(flow_run_id: str):
    """fetch the graph for a flow run"""
    if not isinstance(flow_run_id, str):
        raise TypeError("flow_run_id must be a string")

    try:
        return get_flow_run_tasks(flow_run_id)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to fetch graph for flow_run") from error


@app.get("/proxy/deployments/{deployment_id}")
def get_read_deployment(deployment_id):
    """Fetch deployment and all its details"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")

    try:
        deployment = get_deployment(deployment_id)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch deployment " + deployment_id
        ) from error

    res = deployment_to_json(deployment)

    return res


@app.delete("/proxy/deployments/{deployment_id}")
def delete_deployment(deployment_id):
    """Delete a deployment"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    logger.info("deployment_id=%s", deployment_id)

    root = os.getenv("PREFECT_API_URL")
    res = requests.delete(f"{root}/deployments/{deployment_id}", timeout=30)
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=res.text) from error
    logger.info("Deleted deployment with ID: %s", deployment_id)


@app.post("/proxy/deployments/{deployment_id}/flow_run")
async def post_create_deployment_flow_run(deployment_id, payload: dict = None):
    """Create a flow run from deployment"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    logger.info("deployment_id=%s", deployment_id)
    try:
        res = await post_deployment_flow_run(deployment_id, payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create flow_run for deployment"
        ) from error

    return res


@app.post("/proxy/deployments/{deployment_id}/flow_run/schedule")
async def post_schedule_deployment_flow_run(deployment_id, payload: ScheduleFlowRunRequest):
    """Create a flow run from deployment"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    logger.info("deployment_id=%s", deployment_id)
    try:
        res = await post_deployment_flow_run(
            deployment_id, payload.runParams, payload.scheduledTime
        )
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create flow_run for deployment"
        ) from error

    return res


@app.post("/proxy/deployments/{deployment_id}/set_schedule/{status}")
def post_deployment_set_schedule(deployment_id, status):
    """Create a flow run from deployment"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")

    if not isinstance(status, str):
        raise TypeError("status must be a string")
    if (
        (status is None)
        or (isinstance(status, str) is not True)
        or (status not in ["active", "inactive"])
    ):
        raise HTTPException(status_code=422, detail="incorrect status value")

    try:
        set_deployment_schedule(deployment_id, status)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to set schedule") from error

    return {"success": 1}


@app.get("/proxy/prefect/version")
def get_prefect_version():
    """Get Flow Runs for a deployment"""
    ver = None
    try:
        ver = get_current_prefect_version()
    except Exception as error:
        logger.exception(error)
    return ver


@app.get("/proxy/flow_runs/long-running/{nhours}")
def get_long_running_flows(nhours: int, start_time_str: str = ""):
    """Get long-running Flow Runs. the start_time, if provided, must be in ISO-8601 format"""
    flow_runs = get_long_running_flow_runs(nhours, start_time_str)
    return {"flow_runs": flow_runs}


@app.post("/proxy/flow_runs/{flow_run_id}/set_state")
def cancel_queued_flow_run(flow_run_id: str, payload: CancelQueuedManualJob):
    """Cancel a queued manual sync"""
    try:
        set_cancel_queued_flow_run(flow_run_id, payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to cancel the queued manual job"
        ) from error

    return {"success": 1}


@app.post("/proxy/workers/filter/")
def post_filter_prefect_workers(payload: FilterPrefectWorkers):
    """Fetch workers"""
    try:
        count = filter_prefect_workers(payload)
        logger.info(f"Found {count} workers")
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to fetch prefect workers") from error
    return {"count": count}
