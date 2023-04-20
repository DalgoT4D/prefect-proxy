"""Route handlers"""
from fastapi import FastAPI
from service import (
    get_airbyte_connection_block_id,
    get_airbyte_server_block_id,
    get_dbtcore_block_id,
    get_shell_block_id,
)

app = FastAPI()


@app.get("/proxy/blocks/airbyte/server/{blockname}")
async def get_airbyte_server(blockname):
    """look up an airbyte server block by name and return block_id"""
    return await get_airbyte_server_block_id(blockname)


@app.get("/proxy/blocks/airbyte/connection/{blockname}")
async def get_airbyte_connection(blockname):
    """look up airbyte connection block by name and return block_id"""
    return await get_airbyte_connection_block_id(blockname)


@app.get("/proxy/blocks/shell/{blockname}")
async def get_shell(blockname):
    """look up a shell operation block by name and return block_id"""
    return await get_shell_block_id(blockname)


@app.get("/proxy/blocks/dbtcore/{blockname}")
async def get_dbtcore(blockname):
    """look up a dbt core operation block by name and return block_id"""
    return await get_dbtcore_block_id(blockname)
