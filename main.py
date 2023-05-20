"""Route handlers"""
import os
import requests
from fastapi import FastAPI, HTTPException
from service import (
    get_airbyte_server_block_id,
    create_airbyte_server_block,
    get_airbyte_connection_block_id,
    get_airbyte_connection_block,
    create_airbyte_connection_block,
    get_dbtcore_block_id,
    create_dbt_core_block,
    get_shell_block_id,
    create_shell_block,
    run_dbtcore_prefect_flow,
    post_deployment,
    get_flow_runs_by_deployment_id,
    run_airbyte_connection_prefect_flow,
    get_deployments_by_filter,
    get_flow_run_logs,
    post_deployment_flow_run,
)
from schemas import (
    AirbyteServerCreate,
    AirbyteConnectionCreate,
    PrefectShellSetup,
    DbtCoreCreate,
    RunFlow,
    DeploymentCreate,
    DeploymentFetch,
)
from logger import logger

app = FastAPI()


# =============================================================================
@app.get("/proxy/blocks/airbyte/server/{blockname}")
async def get_airbyte_server(blockname):
    """look up an airbyte server block by name and return block_id"""
    block_id = await get_airbyte_server_block_id(blockname)
    if block_id is None:
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.post("/proxy/blocks/airbyte/server/")
async def post_airbyte_server(payload: AirbyteServerCreate):
    """create a new airbyte server block with this block name,
    raise an exception if the name is already in use"""
    logger.info(payload)
    block_id = await create_airbyte_server_block(payload)
    logger.info("Created new airbyte server block with ID: %s", block_id)
    return {"block_id": block_id}


# =============================================================================
@app.get("/proxy/blocks/airbyte/connection/byblockname/{blockname}")
async def get_airbyte_connection_by_blockname(blockname):
    """look up airbyte connection block by name and return block_id"""
    block_id = await get_airbyte_connection_block_id(blockname)
    if block_id is None:
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.get("/proxy/blocks/airbyte/connection/byblockid/{blockid}")
async def get_airbyte_connection_by_blockid(blockid):
    """look up airbyte connection block by id and return block data"""
    block = await get_airbyte_connection_block(blockid)
    if block is None:
        raise HTTPException(status_code=400, detail="no block having id " + blockid)
    logger.info("Found airbyte connection block by id: %s", block)
    return block


@app.post("/proxy/blocks/airbyte/connection/")
async def post_airbyte_connection(payload: AirbyteConnectionCreate):
    """create a new airbyte connection block with this block name,
    raise an exception if the name is already in use"""
    logger.info(payload)
    block_id = await create_airbyte_connection_block(payload)
    logger.info("Created new airbyte connection block with ID: %s", block_id)
    return {"block_id": block_id}


# =============================================================================
@app.get("/proxy/blocks/shell/{blockname}")
async def get_shell(blockname):
    """look up a shell operation block by name and return block_id"""
    block_id = await get_shell_block_id(blockname)
    if block_id is None:
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.post("/proxy/blocks/shell/")
async def post_shell(payload: PrefectShellSetup):
    """create a new shell block with this block name,
    raise an exception if the name is already in use"""
    logger.info(payload)
    block_id = await create_shell_block(payload)
    logger.info("Created new shell block with ID: %s", block_id)
    return {"block_id": block_id}


# =============================================================================
@app.get("/proxy/blocks/dbtcore/{blockname}")
async def get_dbtcore(blockname):
    """look up a dbt core operation block by name and return block_id"""
    block_id = await get_dbtcore_block_id(blockname)
    if block_id is None:
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.post("/proxy/blocks/dbtcore/")
async def post_dbtcore(payload: DbtCoreCreate):
    """create a new dbt_core block with this block name,
    raise an exception if the name is already in use"""
    logger.info(payload)
    block_id, cleaned_blockname = await create_dbt_core_block(payload)
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
    root = os.getenv("PREFECT_API_URL")
    res = requests.delete(f"{root}/block_documents/{block_id}", timeout=30)
    res.raise_for_status()


# =============================================================================
@app.post("/proxy/flows/airbyte/connection/sync/")
async def sync_airbyte_connection_flow(payload: RunFlow):
    """Prefect flow to run airbyte connection"""
    logger.info(payload)
    if payload.blockName == "":
        raise HTTPException(status_code=400, detail="received empty blockName")
    logger.info("Running airbyte connection sync flow")
    return run_airbyte_connection_prefect_flow(payload)


@app.post("/proxy/flows/dbtcore/run/")
async def sync_dbtcore_flow(payload: RunFlow):
    """Prefect flow to run dbt"""
    logger.info(payload)
    if payload.blockName == "":
        raise HTTPException(status_code=400, detail="received empty blockName")
    return run_dbtcore_prefect_flow(payload)


@app.post("/proxy/deployments/")
async def post_dataflow(payload: DeploymentCreate):
    """Create a deployment from an existing flow"""
    logger.info(payload)
    deployment = await post_deployment(payload)
    logger.info("Created new deployment: %s", deployment)
    return {"deployment": deployment}


@app.get("/proxy/flow_runs")
def get_flow_runs(deployment_id: str, limit: int = 0):
    """Get Flow Runs for a deployment"""

    flow_runs = get_flow_runs_by_deployment_id(deployment_id, limit)
    logger.info("Found flow runs for deployment ID: %s", deployment_id)
    return {"flow_runs": flow_runs}


@app.post("/proxy/deployments/filter")
def post_deployments(payload: DeploymentFetch):
    """Get deployments by various filters"""
    logger.info(payload)
    deployments = get_deployments_by_filter(
        org_slug=payload.org_slug, deployment_ids=payload.deployment_ids
    )
    logger.info("Found deployments with payload: %s", payload)
    return {"deployments": deployments}


@app.get("/proxy/flow_runs/logs/{flow_run_id}")
def get_flow_run_logs_paginated(flow_run_id: str, offset: int = 0):
    """paginate the logs from a flow run"""
    return get_flow_run_logs(flow_run_id, offset)


@app.delete("/proxy/deployments/{deployment_id}")
def delete_deployment(deployment_id):
    """Delete a deployment"""

    root = os.getenv("PREFECT_API_URL")
    res = requests.delete(f"{root}/deployments/{deployment_id}", timeout=30)
    res.raise_for_status()
    logger.info("Deleted deployment with ID: %s", deployment_id)


@app.post("/proxy/deployments/{deployment_id}/flow_run")
async def post_create_deployment_flow_run(deployment_id):
    """Create a flow run from deployment"""

    res = await post_deployment_flow_run(deployment_id)

    return res
