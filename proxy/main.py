"""Route handlers"""

import os
import re
import base64
import requests
from fastapi import FastAPI, HTTPException
from prefect_airbyte import AirbyteConnection
import sentry_sdk
from proxy.helpers import CustomLogger, deployment_to_json


from proxy.service import (
    get_airbyte_server_block,
    get_airbyte_server_block_id,
    create_airbyte_server_block,
    create_dbt_core_block,
    put_deployment_v1,
    update_postgres_credentials,
    update_bigquery_credentials,
    update_target_configs_schema,
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
    get_flow_run,
    retry_flow_run,
    create_secret_block,
    upsert_secret_block,
    _create_dbt_cli_profile,
    update_dbt_cli_profile,
    get_dbt_cli_profile,
    delete_flow_run,
    get_long_running_flow_runs,
    get_current_prefect_version,
    patch_dbt_cloud_creds_block,
    get_dbt_cloud_creds_block,
    update_airbyte_server_block,
    set_cancel_queued_flow_run,
    filter_late_flow_runs,
    filter_prefect_workers,
)
from proxy.schemas import (
    AirbyteServerCreate,
    AirbyteServerUpdate,
    DbtCoreCreate,
    DbtCoreCredentialUpdate,
    DbtCoreSchemaUpdate,
    RunDbtCoreOperation,
    RunShellOperation,
    DeploymentCreate2,
    DeploymentFetch,
    FlowRunRequest,
    RetryFlowRunRequest,
    PrefectSecretBlockCreate,
    PrefectSecretBlockEdit,
    DbtCliProfileBlockCreate,
    DbtCloudCredsBlockPatch,
    DeploymentUpdate2,
    DbtCliProfileBlockUpdate,
    RunAirbyteResetConnection,
    ScheduleFlowRunRequest,
    CancelQueuedManualJob,
    FilterLateFlowRuns,
    FilterPrefectWorkers,
)
from proxy.flows import run_airbyte_connection_flow

from proxy.prefect_flows import (
    run_shell_operation_flow,
    run_dbtcore_flow_v1,
    run_airbyte_conn_reset,
)


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
def airbytesync(block_name: str, flow_name: str, flow_run_name: str):
    """Run an Airbyte Connection sync"""
    if not isinstance(block_name, str):
        raise TypeError("block_name must be a string")
    if not isinstance(flow_name, str):
        raise TypeError("flow_name must be a string")
    if not isinstance(flow_run_name, str):
        raise TypeError("flow_run_name must be a string")

    logger.info("airbytesync %s %s %s", block_name, flow_name, flow_run_name)
    flow = run_airbyte_connection_flow
    if flow_name:
        flow = flow.with_options(name=flow_name)
    if flow_run_name:
        flow = flow.with_options(flow_run_name=flow_run_name)

    try:
        logger.info("START")
        result = flow(block_name)
        logger.info("END")
        logger.info(result)
        return {"status": "success", "result": result}

    except HTTPException as error:
        logger.info("ERR-1")
        # the error message may contain "Job <num> failed."
        errormessage = error.detail
        logger.error("celery task caught exception %s", errormessage)
        pattern = re.compile(r"Job (\d+) failed.")
        match = pattern.match(errormessage)
        if match:
            airbyte_job_num = match.groups()[0]
            return {"status": "failed", "airbyte_job_num": airbyte_job_num}
        raise

    except Exception as error:
        logger.exception(error)
        raise


def dbtrun_v1(task_config: RunDbtCoreOperation):
    """Run a dbt core flow"""

    if not isinstance(task_config, RunDbtCoreOperation):
        raise TypeError("invalid task config")
    logger.info("dbt core operation running %s", task_config.slug)
    flow = run_dbtcore_flow_v1
    if task_config.flow_name:
        flow = flow.with_options(name=task_config.flow_name)
    if task_config.flow_run_name:
        flow = flow.with_options(flow_run_name=task_config.flow_run_name)

    try:
        result = flow(task_config.model_dump())
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

    flow = run_shell_operation_flow
    if task_config.flow_name:
        flow = flow.with_options(name=task_config.flow_name)
    if task_config.flow_run_name:
        flow = flow.with_options(flow_run_name=task_config.flow_run_name)

    try:
        result = flow(task_config.model_dump())
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


# =============================================================================
@app.post("/proxy/blocks/dbtcore/")
async def post_dbtcore(payload: DbtCoreCreate):
    """
    create a new dbt_core block with this block name,
    raise an exception if the name is already in use
    """
    # logger.info(payload) DO NOT LOG - CONTAINS SECRETS
    if not isinstance(payload, DbtCoreCreate):
        raise TypeError("payload is invalid")
    try:
        block_id, cleaned_blockname = await create_dbt_core_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="failed to create dbt core block") from error
    logger.info(
        "Created new dbt_core block with ID: %s and name: %s",
        block_id,
        cleaned_blockname,
    )
    return {"block_id": block_id, "block_name": cleaned_blockname}


@app.post("/proxy/blocks/dbtcli/profile/")
async def post_dbtcli_profile(payload: DbtCliProfileBlockCreate):
    """
    create a new dbt_core block with this block name,
    raise an exception if the name is already in use
    """
    # logger.info(payload) DO NOT LOG - CONTAINS SECRETS
    if not isinstance(payload, DbtCliProfileBlockCreate):
        raise TypeError("payload is invalid")
    try:
        _, block_id, cleaned_blockname = await _create_dbt_cli_profile(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create dbt cli profile block"
        ) from error
    logger.info(
        "Created new dbt cli profile block with ID: %s and name: %s",
        block_id,
        cleaned_blockname,
    )
    return {"block_id": block_id, "block_name": cleaned_blockname}


@app.put("/proxy/blocks/dbtcli/profile/")
async def put_dbtcli_profile(payload: DbtCliProfileBlockUpdate):
    """Updates the dbt cli block based on the type of warehouse"""
    if not isinstance(payload, DbtCliProfileBlockUpdate):
        raise TypeError("payload is invalid")
    try:
        _, block_id, cleaned_blockname = await update_dbt_cli_profile(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to update dbt cli profile block"
        ) from error
    logger.info(
        "Updated the dbt cli profile block with ID: %s and name: %s",
        block_id,
        cleaned_blockname,
    )
    return {"block_id": block_id, "block_name": cleaned_blockname}


@app.get("/proxy/blocks/dbtcli/profile/{cli_profile_block_name}")
async def get_dbtcli_profile(cli_profile_block_name: str):
    """Fetches the dbt cli block"""
    if not isinstance(cli_profile_block_name, str):
        raise TypeError("cli_profile_block_name is invalid")
    try:
        profile = await get_dbt_cli_profile(cli_profile_block_name)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch dbt cli profile block"
        ) from error
    return {"profile": profile}


@app.put("/proxy/blocks/dbtcore_edit/postgres/")
async def put_dbtcore_postgres(payload: DbtCoreCredentialUpdate):
    """update the credentials inside an existing dbt core op block"""
    if not isinstance(payload, DbtCoreCredentialUpdate):
        raise TypeError("payload is invalid")
    try:
        await update_postgres_credentials(payload.blockName, payload.credentials)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400,
            detail="failed to update dbt core block credentials [postgres]",
        ) from error

    logger.info("updated credentials in dbtcore block %s [postgres]", payload.blockName)
    return {"success": 1}


@app.put("/proxy/blocks/dbtcore_edit/bigquery/")
async def put_dbtcore_bigquery(payload: DbtCoreCredentialUpdate):
    """update the credentials inside an existing dbt core op block"""
    if not isinstance(payload, DbtCoreCredentialUpdate):
        raise TypeError("payload is invalid")
    try:
        await update_bigquery_credentials(payload.blockName, payload.credentials)
    except Exception as error:
        raise HTTPException(
            status_code=400,
            detail="failed to update dbt core block credentials [bigquery]",
        ) from error

    logger.info("updated credentials in dbtcore block %s [bigquery]", payload.blockName)
    return {"success": 1}


@app.put("/proxy/blocks/dbtcore_edit_schema/")
async def put_dbtcore_schema(payload: DbtCoreSchemaUpdate):
    """update the target inside an existing dbt core op block"""
    if not isinstance(payload, DbtCoreSchemaUpdate):
        raise TypeError("payload is invalid")
    try:
        await update_target_configs_schema(payload.blockName, payload.target_configs_schema)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400,
            detail="failed to update dbt core block target_configs_schema",
        ) from error

    logger.info("updated target_configs_schema in dbtcore block %s", payload.blockName)
    return {"success": 1}


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
def sync_shellop_flow(payload: RunShellOperation):
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

    if "airbyte_blocks" in res["parameters"]:
        for airbyte_block in res["parameters"]["airbyte_blocks"]:
            block = AirbyteConnection.load(airbyte_block["blockName"])
            airbyte_block["connectionId"] = block.connection_id

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


@app.post("/proxy/flows/airbyte/reset/")
async def reset_airbyte_conn_flow(payload: RunAirbyteResetConnection):
    """Prefect flow to run airbyte reset connection job"""
    logger.info(payload)
    if not isinstance(payload, RunAirbyteResetConnection):
        raise TypeError("payload is invalid")

    logger.info("running reset airbyte connection flow %s", payload.slug)
    try:
        result = await run_airbyte_conn_reset(payload)
        logger.info(result)
        return {"status": "success", "result": result}
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=str(error)) from error


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


# =============================================================================
@app.patch("/proxy/blocks/dbtcloudcreds/")
async def patch_dbt_cloud_creds(payload: DbtCloudCredsBlockPatch):
    """
    create a new DbtCloudCredentials with this block name,
    if the name already exists overwrite the new details with this name
    """
    # logger.info(payload) DO NOT LOG - CONTAINS SECRETS
    if not isinstance(payload, DbtCloudCredsBlockPatch):
        raise TypeError("payload is invalid")
    try:
        _, block_id, cleaned_blockname = await patch_dbt_cloud_creds_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to save dbt cloud credentials block"
        ) from error
    logger.info(
        "Saved new dbt cloud credentials block with ID: %s and name: %s",
        block_id,
        cleaned_blockname,
    )
    return {"block_id": block_id, "block_name": cleaned_blockname}


@app.get("/proxy/blocks/dbtcloudcreds/{block_name}")
async def get_dbt_cloud_creds(block_name: str):
    """Fetches the dbt cloud creds block"""
    if not isinstance(block_name, str):
        raise TypeError("block name is invalid")
    try:
        data = await get_dbt_cloud_creds_block(block_name)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch dbt cloud creds block"
        ) from error
    return data


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
    try:
        count = filter_prefect_workers(payload)
        logger.info(f"Found {count} workers")
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch dbt cloud creds block"
        ) from error
    return {"count": count}
