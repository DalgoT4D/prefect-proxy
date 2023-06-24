"""Route handlers"""
import os
import re
import requests
from fastapi import FastAPI, HTTPException
from prefect_airbyte import AirbyteConnection

from proxy.service import (
    get_airbyte_server_block_id,
    create_airbyte_server_block,
    get_airbyte_connection_block_id,
    get_airbyte_connection_block,
    create_airbyte_connection_block,
    get_dbtcore_block_id,
    create_dbt_core_block,
    get_shell_block_id,
    create_shell_block,
    post_deployment,
    get_flow_runs_by_deployment_id,
    get_deployments_by_filter,
    get_flow_run_logs,
    post_deployment_flow_run,
    get_flow_runs_by_name,
    post_filter_blocks,
    set_deployment_schedule,
    get_deployment,
)
from proxy.schemas import (
    AirbyteServerCreate,
    AirbyteConnectionCreate,
    PrefectShellSetup,
    DbtCoreCreate,
    RunFlow,
    DeploymentCreate,
    DeploymentFetch,
    FlowRunRequest,
    PrefectBlocksDelete,
    AirbyteConnectionBlocksFetch,
)
from proxy.flows import run_airbyte_connection_flow, run_dbtcore_flow

from logger import setup_logger, logger

app = FastAPI()
setup_logger()


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


# =============================================================================
@app.post("/proxy/blocks/bulk/delete/")
async def post_bulk_delete_blocks(payload: PrefectBlocksDelete):
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
def post_airbyte_connection_blocks(payload: AirbyteConnectionBlocksFetch):
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
        raise HTTPException(status_code=500, detail="Internal server error")

    if block_id is None:
        return {"block_id": None}
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


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
        block_id = await create_airbyte_server_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create airbyte server block"
        ) from error
    logger.info("Created new airbyte server block with ID: %s", block_id)
    return {"block_id": block_id}


# =============================================================================
@app.get("/proxy/blocks/airbyte/connection/byblockname/{blockname}")
async def get_airbyte_connection_by_blockname(blockname):
    """look up airbyte connection block by name and return block_id"""
    block_id = await get_airbyte_connection_block_id(blockname)
    if block_id is None:
        logger.error("no airbyte connection block having name %s", blockname)
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.get("/proxy/blocks/airbyte/connection/byblockid/{blockid}")
async def get_airbyte_connection_by_blockid(blockid):
    """look up airbyte connection block by id and return block data"""
    block = await get_airbyte_connection_block(blockid)
    if block is None:
        logger.error("no airbyte connection block having id %s", blockid)
        raise HTTPException(status_code=400, detail="no block having id " + blockid)
    logger.info("Found airbyte connection block by id: %s", block)
    return block


@app.post("/proxy/blocks/airbyte/connection/")
async def post_airbyte_connection(payload: AirbyteConnectionCreate):
    """
    create a new airbyte connection block with this block name,
    raise an exception if the name is already in use
    """
    logger.info(payload)
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
async def get_shell(blockname):
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
async def post_shell(payload: PrefectShellSetup):
    """
    create a new shell block with this block name,
    raise an exception if the name is already in use
    """
    if not isinstance(payload, PrefectShellSetup):
        raise TypeError("payload is invalid")
    logger.info(payload)
    try:
        block_id = await create_shell_block(payload)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create shell block"
        ) from error
    logger.info("Created new shell block with ID: %s", block_id)
    return {"block_id": block_id}


# =============================================================================
@app.get("/proxy/blocks/dbtcore/{blockname}")
async def get_dbtcore(blockname):
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
async def post_dbtcore(payload: DbtCoreCreate):
    """
    create a new dbt_core block with this block name,
    raise an exception if the name is already in use
    """
    logger.info(payload)
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
@app.post("/proxy/flows/airbyte/connection/sync/")
async def sync_airbyte_connection_flow(payload: RunFlow):
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
async def sync_dbtcore_flow(payload: RunFlow):
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


@app.post("/proxy/deployments/")
async def post_dataflow(payload: DeploymentCreate):
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
def get_flow_runs(deployment_id: str, limit: int = 0):
    """Get Flow Runs for a deployment"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    if not isinstance(limit, int):
        raise TypeError("limit must be an integer")
    if limit < 0:
        raise ValueError("limit must be positive")
    logger.info("deployment_id=%s, limit=%s", deployment_id, limit)
    try:
        flow_runs = get_flow_runs_by_deployment_id(deployment_id, limit)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch flow_runs for deployment"
        ) from error
    logger.info("Found flow runs for deployment ID: %s", deployment_id)
    return {"flow_runs": flow_runs}


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
        raise HTTPException(
            status_code=400, detail="failed to filter deployments"
        ) from error
    logger.info("Found deployments with payload: %s", payload)
    return {"deployments": deployments}


@app.get("/proxy/flow_runs/logs/{flow_run_id}")
def get_flow_run_logs_paginated(flow_run_id: str, offset: int = 0):
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
def get_read_deployment(deployment_id):
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
        "cron": deployment["schedule"]["cron"],
        "isScheduleActive": deployment["is_schedule_active"],
        "parameters": deployment["parameters"],
    }

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
async def post_create_deployment_flow_run(deployment_id):
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
