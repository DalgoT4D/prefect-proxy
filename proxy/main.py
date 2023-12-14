"""Route handlers"""
import os
import re
import requests
from fastapi import FastAPI, HTTPException, Request
from prefect_airbyte import AirbyteConnection
from proxy.helpers import CustomLogger

from proxy.service import (
    get_airbyte_server_block_id,
    create_airbyte_server_block,
    get_airbyte_connection_block_id,
    get_airbyte_connection_block,
    create_airbyte_connection_block,
    get_dbtcore_block_id,
    create_dbt_core_block,
    put_deployment,
    put_deployment_v1,
    update_postgres_credentials,
    update_bigquery_credentials,
    update_target_configs_schema,
    get_shell_block_id,
    create_shell_block,
    post_deployment,
    post_deployment_v1,
    get_flow_runs_by_deployment_id,
    get_deployments_by_filter,
    get_flow_run_logs,
    post_deployment_flow_run,
    get_flow_runs_by_name,
    post_filter_blocks,
    set_deployment_schedule,
    get_deployment,
    get_flow_run,
    create_secret_block,
    _create_dbt_cli_profile,
    _block_id,
)
from proxy.schemas import (
    AirbyteServerCreate,
    AirbyteConnectionCreate,
    DeploymentUpdate,
    PrefectShellSetup,
    DbtCoreCreate,
    DbtCoreCredentialUpdate,
    DbtCoreSchemaUpdate,
    RunFlow,
    RunDbtCoreOperation,
    RunShellOperation,
    DeploymentCreate,
    DeploymentCreate2,
    DeploymentFetch,
    FlowRunRequest,
    PrefectBlocksDelete,
    AirbyteConnectionBlocksFetch,
    PrefectSecretBlockCreate,
    DbtCliProfileBlockCreate,
    DeploymentUpdate2,
)
from proxy.flows import run_airbyte_connection_flow, run_dbtcore_flow

from proxy.prefect_flows import run_shell_operation_flow, run_dbtcore_flow_v1

from logger import setup_logger

app = FastAPI()
setup_logger()

logger = CustomLogger("prefect-proxy")


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


def dbtrun(block_name: str, flow_name: str, flow_run_name: str):
    """Run a dbt core flow"""
    if not isinstance(block_name, str):
        raise TypeError("block_name must be a string")
    if not isinstance(flow_name, str):
        raise TypeError("flow_name must be a string")
    if not isinstance(flow_run_name, str):
        raise TypeError("flow_run_name must be a string")

    logger.info("dbtrun %s %s %s", block_name, flow_name, flow_run_name)
    flow = run_dbtcore_flow
    if flow_name:
        flow = flow.with_options(name=flow_name)
    if flow_run_name:
        flow = flow.with_options(flow_run_name=flow_run_name)

    try:
        result = flow(block_name)
        return result
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to run dbt core flow"
        ) from error


def dbtrun_v1(task_config: RunDbtCoreOperation):
    """Run a dbt core flow"""

    logger.info("dbt core operation running %s", task_config.slug)
    flow = run_dbtcore_flow_v1
    if task_config.flow_name:
        flow = flow.with_options(name=task_config.flow_name)
    if task_config.flow_run_name:
        flow = flow.with_options(flow_run_name=task_config.flow_run_name)

    try:
        result = flow(task_config)
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
        result = flow(task_config)
        return result
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to run shell operation flow"
        ) from error


# =============================================================================
@app.post("/proxy/blocks/bulk/delete/")
async def post_bulk_delete_blocks(request: Request, payload: PrefectBlocksDelete):
    """Delete all airbyte connection blocks in the payload array"""
    root = os.getenv("PREFECT_API_URL")
    deleted_blockids = []
    for block_id in payload.block_ids:
        logger.info("deleting block_id : %s ", block_id)
        res = requests.delete(f"{root}/block_documents/{block_id}", timeout=10)
        try:
            res.raise_for_status()
        except requests.exceptions.HTTPError as error:
            logger.error(
                "something went wrong deleting block_id %s: %s", block_id, res.text
            )
            logger.exception(error)
            continue
        logger.info("deleted block with block_id : %s", block_id)
        deleted_blockids.append(block_id)

    return {"deleted_blockids": deleted_blockids}


# =============================================================================
@app.post("/proxy/blocks/airbyte/connection/filter")
def post_airbyte_connection_blocks(
    request: Request, payload: AirbyteConnectionBlocksFetch
):
    """Filter the prefect blocks with parameters from payload"""
    blocks = post_filter_blocks(payload.block_names)

    # we only care about the block name and its connection id
    response = []
    for blk in blocks:
        connection_id = ""
        if "data" in blk and "connection_id" in blk["data"]:
            connection_id = blk["data"]["connection_id"]
        response.append(
            {"name": blk["name"], "connectionId": connection_id, "id": blk["id"]}
        )

    return response


# =============================================================================
@app.get("/proxy/blocks/airbyte/server/{blockname}")
async def get_airbyte_server(request: Request, blockname: str):
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


@app.post("/proxy/blocks/airbyte/server/")
async def post_airbyte_server(request: Request, payload: AirbyteServerCreate):
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


# =============================================================================
@app.get("/proxy/blocks/airbyte/connection/byblockname/{blockname}")
async def get_airbyte_connection_by_blockname(request: Request, blockname):
    """look up airbyte connection block by name and return block_id"""
    block_id = await get_airbyte_connection_block_id(blockname)
    if block_id is None:
        logger.error("no airbyte connection block having name %s", blockname)
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.get("/proxy/blocks/airbyte/connection/byblockid/{blockid}")
async def get_airbyte_connection_by_blockid(request: Request, blockid):
    """look up airbyte connection block by id and return block data"""
    block = await get_airbyte_connection_block(blockid)
    if block is None:
        logger.error("no airbyte connection block having id %s", blockid)
        raise HTTPException(status_code=400, detail="no block having id " + blockid)
    logger.info("Found airbyte connection block by id: %s", block)
    return block


@app.post("/proxy/blocks/airbyte/connection/")
async def post_airbyte_connection(request: Request, payload: AirbyteConnectionCreate):
    """
    create a new airbyte connection block with this block name,
    raise an exception if the name is already in use
    """
    try:
        block_id = await create_airbyte_connection_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create airbyte connection block"
        ) from error
    logger.info("Created new airbyte connection block with ID: %s", block_id)
    return {"block_id": block_id}


# =============================================================================
@app.get("/proxy/blocks/shell/{blockname}")
async def get_shell(request: Request, blockname):
    """look up a shell operation block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    block_id = await get_shell_block_id(blockname)
    if block_id is None:
        logger.error("no shell block having name %s", blockname)
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.post("/proxy/blocks/shell/")
async def post_shell(request: Request, payload: PrefectShellSetup):
    """
    create a new shell block with this block name,
    raise an exception if the name is already in use
    """
    if not isinstance(payload, PrefectShellSetup):
        raise TypeError("payload is invalid")
    logger.info(payload)
    try:
        block_id, cleaned_blockname = await create_shell_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create shell block"
        ) from error
    logger.info("Created new shell block with ID: %s", block_id)
    return {"block_id": block_id, "block_name": cleaned_blockname}


# =============================================================================
@app.get("/proxy/blocks/dbtcore/{blockname}")
async def get_dbtcore(request: Request, blockname):
    """look up a dbt core operation block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")

    block_id = await get_dbtcore_block_id(blockname)
    if block_id is None:
        logger.error("no dbt core block having name %s", blockname)
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.post("/proxy/blocks/dbtcore/")
async def post_dbtcore(request: Request, payload: DbtCoreCreate):
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
        raise HTTPException(
            status_code=400, detail="failed to create dbt core block"
        ) from error
    logger.info(
        "Created new dbt_core block with ID: %s and name: %s",
        block_id,
        cleaned_blockname,
    )
    return {"block_id": block_id, "block_name": cleaned_blockname}


@app.post("/proxy/blocks/dbtcli/profile/")
async def post_dbtcli_profile(request: Request, payload: DbtCliProfileBlockCreate):
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


@app.put("/proxy/blocks/dbtcore_edit/postgres/")
async def put_dbtcore_postgres(request: Request, payload: DbtCoreCredentialUpdate):
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
async def put_dbtcore_bigquery(request: Request, payload: DbtCoreCredentialUpdate):
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
async def put_dbtcore_schema(request: Request, payload: DbtCoreSchemaUpdate):
    """update the target inside an existing dbt core op block"""
    if not isinstance(payload, DbtCoreSchemaUpdate):
        raise TypeError("payload is invalid")
    try:
        await update_target_configs_schema(
            payload.blockName, payload.target_configs_schema
        )
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
async def post_secret_block(request: Request, payload: PrefectSecretBlockCreate):
    """create a new prefect secret block with this block name to store a secret string"""
    if not isinstance(payload, PrefectSecretBlockCreate):
        raise TypeError("payload is invalid")
    try:
        block_id, cleaned_blockname = await create_secret_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to prefect secret block"
        ) from error
    logger.info(
        "Created new secret block with ID: %s and name: %s",
        block_id,
        cleaned_blockname,
    )
    return {"block_id": block_id, "block_name": cleaned_blockname}


# =============================================================================
@app.delete("/delete-a-block/{block_id}")
async def delete_block(request: Request, block_id):
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
@app.post("/proxy/flows/airbyte/connection/sync/")
async def sync_airbyte_connection_flow(request: Request, payload: RunFlow):
    """Prefect flow to sync an airbyte connection"""
    if not isinstance(payload, RunFlow):
        raise TypeError("payload is invalid")
    logger.info(payload)
    if payload.blockName == "":
        logger.error("received empty blockName")
        raise HTTPException(status_code=400, detail="received empty blockName")
    logger.info("Running airbyte connection sync flow")
    try:
        result = airbytesync(payload.blockName, payload.flowName, payload.flowRunName)
        logger.info(result)
        return result
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/proxy/flows/dbtcore/run/")
async def sync_dbtcore_flow(request: Request, payload: RunFlow):
    """Prefect flow to run dbt"""
    logger.info(payload)
    if not isinstance(payload, RunFlow):
        raise TypeError("payload is invalid")
    if payload.blockName == "":
        logger.error("received empty blockName")
        raise HTTPException(status_code=400, detail="received empty blockName")
    logger.info("running dbtcore-run for dbt-core-op %s", payload.blockName)
    try:
        result = dbtrun(payload.blockName, payload.flowName, payload.flowRunName)
        logger.info(result)
        return {"status": "success", "result": result}
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=str(error)) from error


@app.post("/proxy/v1/flows/dbtcore/run/")
async def sync_dbtcore_flow_v1(request: Request, payload: RunDbtCoreOperation):
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
async def sync_shellop_flow(request: Request, payload: RunShellOperation):
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


@app.post("/proxy/deployments/")
async def post_dataflow(request: Request, payload: DeploymentCreate):
    """Create a deployment from an existing flow"""
    if not isinstance(payload, DeploymentCreate):
        raise TypeError("payload is invalid")

    logger.info(payload)
    try:
        deployment = await post_deployment(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create deployment"
        ) from error
    logger.info("Created new deployment: %s", deployment)
    return {"deployment": deployment}


@app.post("/proxy/v1/deployments/")
async def post_dataflow_v1(request: Request, payload: DeploymentCreate2):
    """Create a deployment from an existing flow"""
    if not isinstance(payload, DeploymentCreate2):
        raise TypeError("payload is invalid")

    logger.info(payload)
    try:
        deployment = await post_deployment_v1(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create deployment"
        ) from error
    logger.info("Created new deployment: %s", deployment)
    return {"deployment": deployment}


@app.put("/proxy/deployments/{deployment_id}")
def put_dataflow(request: Request, deployment_id, payload: DeploymentUpdate):
    """updates a deployment"""
    if not isinstance(payload, DeploymentUpdate):
        raise TypeError("payload is invalid")

    logger.info(payload)
    try:
        put_deployment(deployment_id, payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to update the deployment"
        ) from error
    logger.info("Updated the deployment: %s", deployment_id)
    return {"success": 1}


@app.put("/proxy/v1/deployments/{deployment_id}")
def put_dataflow_v1(request: Request, deployment_id, payload: DeploymentUpdate2):
    """updates a deployment"""
    if not isinstance(payload, DeploymentUpdate2):
        raise TypeError("payload is invalid")

    logger.info(payload)
    try:
        put_deployment_v1(deployment_id, payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to update the deployment"
        ) from error
    logger.info("Updated the deployment: %s", deployment_id)
    return {"success": 1}


@app.post("/proxy/flow_run/")
async def get_flowrun(request: Request, payload: FlowRunRequest):
    """look up a flow run by name and return id if found"""
    if not isinstance(payload, FlowRunRequest):
        raise TypeError("payload is invalid")

    logger.info("flow run name=%s", payload.name)
    try:
        flow_runs = get_flow_runs_by_name(payload.name)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch flow_runs by name"
        ) from error
    if flow_runs:
        if len(flow_runs) > 1:
            logger.error("multiple flow names having name %s", payload.name)
        return {"flow_run": flow_runs[0]}
    logger.error("no flow_runs having name %s", payload.name)
    raise HTTPException(status_code=400, detail="no such flow run")


@app.get("/proxy/flow_runs")
def get_flow_runs(
    request: Request, deployment_id: str, limit: int = 0, start_time_gt: str = ""
):
    """Get Flow Runs for a deployment"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    if not isinstance(limit, int):
        raise TypeError("limit must be an integer")
    if limit < 0:
        raise ValueError("limit must be positive")
    logger.info("deployment_id=%s, limit=%s", deployment_id, limit)
    try:
        flow_runs = get_flow_runs_by_deployment_id(deployment_id, limit, start_time_gt)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch flow_runs for deployment"
        ) from error
    logger.info("Found flow runs for deployment ID: %s", deployment_id)
    return {"flow_runs": flow_runs}


@app.get("/proxy/flow_runs/{flow_run_id}")
def get_flow_run_by_id(request: Request, flow_run_id):
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

    logger.info("Found flow run wth id - %s", flow_run_id)

    return flow_run


@app.post("/proxy/deployments/filter")
def post_deployments(request: Request, payload: DeploymentFetch):
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
        raise HTTPException(
            status_code=400, detail="failed to filter deployments"
        ) from error
    logger.info("Found deployments with payload: %s", payload)
    return {"deployments": deployments}


@app.get("/proxy/flow_runs/logs/{flow_run_id}")
def get_flow_run_logs_paginated(request: Request, flow_run_id: str, offset: int = 0):
    """paginate the logs from a flow run"""
    if not isinstance(flow_run_id, str):
        raise TypeError("flow_run_id must be a string")
    if not isinstance(offset, int):
        raise TypeError("offset must be an integer")
    if offset < 0:
        raise ValueError("offset must be positive")
    logger.info("flow_run_id=%s, offset=%s", flow_run_id, offset)
    try:
        return get_flow_run_logs(flow_run_id, offset)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch logs for flow_run"
        ) from error


@app.get("/proxy/deployments/{deployment_id}")
def get_read_deployment(request: Request, deployment_id):
    """Fetch deployment and all its details"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    logger.info("deployment_id=%s", deployment_id)

    try:
        deployment = get_deployment(deployment_id)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch deployment " + deployment_id
        ) from error

    res = {
        "name": deployment["name"],
        "deploymentId": deployment["id"],
        "tags": deployment["tags"],
        "cron": deployment["schedule"]["cron"] if deployment["schedule"] else "",
        "isScheduleActive": deployment["is_schedule_active"],
        "parameters": deployment["parameters"],
    }

    if "airbyte_blocks" in res["parameters"]:
        for airbyte_block in res["parameters"]["airbyte_blocks"]:
            block = AirbyteConnection.load(airbyte_block["blockName"])
            airbyte_block["connectionId"] = block.connection_id

    return res


@app.delete("/proxy/deployments/{deployment_id}")
def delete_deployment(request: Request, deployment_id):
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
async def post_create_deployment_flow_run(request: Request, deployment_id):
    """Create a flow run from deployment"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    logger.info("deployment_id=%s", deployment_id)
    try:
        res = await post_deployment_flow_run(deployment_id)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create flow_run for deployment"
        ) from error

    return res


@app.post("/proxy/deployments/{deployment_id}/set_schedule/{status}")
def post_deployment_set_schedule(request: Request, deployment_id, status):
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
