"""Route handlers"""
import os
import requests
from fastapi import FastAPI
from service import (
    get_airbyte_server_block_id,
    create_airbyte_server_block,
    get_airbyte_connection_block_id,
    create_airbyte_connection_block,
    get_dbtcore_block_id,
    create_dbt_core_block,
    get_shell_block_id,
    create_shell_block,
    run_airbyte_connection_prefect_flow,
    run_dbtcore_prefect_flow,
)
from schemas import (
    AirbyteServerCreate,
    AirbyteConnectionCreate,
    PrefectShellSetup,
    DbtCoreCreate,
    RunFlow,
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
@app.get("/proxy/blocks/airbyte/connection/{blockname}")
async def get_airbyte_connection(blockname):
    """look up airbyte connection block by name and return block_id"""
    block_id = await get_airbyte_connection_block_id(blockname)
    return {"block_id": block_id}


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
