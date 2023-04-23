"""Route handlers"""
import os
import requests
from fastapi import FastAPI
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
    get_deployments_by_org_slug,
)
from schemas import (
    AirbyteServerCreate,
    AirbyteConnectionCreate,
    PrefectShellSetup,
    DbtCoreCreate,
    RunFlow,
    DeploymentCreate,
)

app = FastAPI()


# =============================================================================
@app.get("/proxy/blocks/airbyte/server/{blockname}")
async def get_airbyte_server(blockname):
    """look up an airbyte server block by name and return block_id"""
    block_id = await get_airbyte_server_block_id(blockname)
    return {"block_id": block_id}


@app.post("/proxy/blocks/airbyte/server/")
async def post_airbyte_server(payload: AirbyteServerCreate):
    """create a new airbyte server block with this block name,
    raise an exception if the name is already in use"""
    block_id = await create_airbyte_server_block(payload)
    return {"block_id": block_id}


# =============================================================================
@app.get("/proxy/blocks/airbyte/connection/byblockname/{blockname}")
async def get_airbyte_connection_by_blockname(blockname):
    """look up airbyte connection block by name and return block_id"""
    block_id = await get_airbyte_connection_block_id(blockname)
    return {"block_id": block_id}


@app.get("/proxy/blocks/airbyte/connection/byblockid/{blockid}")
async def get_airbyte_connection_by_blockid(blockid):
    """look up airbyte connection block by id and return block data"""
    block = await get_airbyte_connection_block(blockid)
    return block


@app.post("/proxy/blocks/airbyte/connection/")
async def post_airbyte_connection(payload: AirbyteConnectionCreate):
    """create a new airbyte connection block with this block name,
    raise an exception if the name is already in use"""
    block_id = await create_airbyte_connection_block(payload)
    return {"block_id": block_id}


# =============================================================================
@app.get("/proxy/blocks/shell/{blockname}")
async def get_shell(blockname):
    """look up a shell operation block by name and return block_id"""
    block_id = await get_shell_block_id(blockname)
    return {"block_id": block_id}


@app.post("/proxy/blocks/shell/")
async def post_shell(payload: PrefectShellSetup):
    """create a new shell block with this block name,
    raise an exception if the name is already in use"""
    block_id = await create_shell_block(payload)
    return {"block_id": block_id}


# =============================================================================
@app.get("/proxy/blocks/dbtcore/{blockname}")
async def get_dbtcore(blockname):
    """look up a dbt core operation block by name and return block_id"""
    block_id = await get_dbtcore_block_id(blockname)
    return {"block_id": block_id}


@app.post("/proxy/blocks/dbtcore/")
async def post_dbtcore(payload: DbtCoreCreate):
    """create a new dbt_core block with this block name,
    raise an exception if the name is already in use"""
    block_id = await create_dbt_core_block(payload)
    return {"block_id": block_id}


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
    return run_airbyte_connection_prefect_flow(payload)


@app.post("/proxy/flows/dbtcore/run/")
async def sync_dbtcore_flow(payload: RunFlow):
    """Prefect flow to run dbt"""
    return run_dbtcore_prefect_flow(payload)


@app.post("/proxy/deployments/")
async def post_dataflow(payload: DeploymentCreate):
    """Create a deployment from an existing flow"""
    await post_deployment(payload)
    return {"success": "?"}


@app.get("/proxy/flow_runs")
def get_flow_runs(deployment_id: str, limit: int = 0):
    """Get Flow Runs for a deployment"""

    flow_runs = get_flow_runs_by_deployment_id(deployment_id, limit)
    return {"flow_runs": flow_runs}


@app.get("/proxy/deployments")
def get_deployments(org_slug: str):
    """Get Flow Runs for a deployment"""

    deployments = get_deployments_by_org_slug(org_slug)
    return {"deployments": deployments}
