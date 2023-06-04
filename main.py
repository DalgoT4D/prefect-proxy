"""Route handlers"""
import os
import re
import json
import requests
from fastapi import FastAPI, HTTPException, Request
from celery import Celery
from redis import Redis

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
    post_deployment,
    get_flow_runs_by_deployment_id,
    get_deployments_by_filter,
    get_flow_run_logs,
    post_deployment_flow_run,
    get_flow_runs_by_name,
)
from schemas import (
    AirbyteServerCreate,
    AirbyteConnectionCreate,
    PrefectShellSetup,
    DbtCoreCreate,
    RunFlow,
    DeploymentCreate,
    DeploymentFetch,
    FlowRunRequest,
)
from flows import run_airbyte_connection_flow, run_dbtcore_flow

from logger import logger

celery = Celery("main.celery", namespace="PROXY")
celery.conf.broker_url = "redis://localhost:6379"
celery.conf.result_backend = "redis://localhost:6379"

app = FastAPI()


# =============================================================================
def airbytesync(block_name, flow_name, flow_run_name):
    """Run an Airbyte Connection sync"""
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
        pattern = re.compile("Job (\d+) failed.")
        match = pattern.match(errormessage)
        if match:
            airbyte_job_num = match.groups()[0]
            return {"status": "failed", "airbyte_job_num": airbyte_job_num}
        else:
            raise

    except Exception as error:
        logger.exception(error)
        raise


def dbtrun(block_name, flow_name, flow_run_name):
    """Run a dbt core flow"""
    logger.info("dbtrun %s %s %s", block_name, flow_name, flow_run_name)
    flow = run_dbtcore_flow
    if flow_name:
        flow = flow.with_options(name=flow_name)
    if flow_run_name:
        flow = flow.with_options(flow_run_name=flow_run_name)
    result = flow(block_name)
    return result


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
    """create a new airbyte connection block with this block name,
    raise an exception if the name is already in use"""
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
    block_id = await get_shell_block_id(blockname)
    if block_id is None:
        logger.error("no shell block having name %s", blockname)
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.post("/proxy/blocks/shell/")
async def post_shell(payload: PrefectShellSetup):
    """create a new shell block with this block name,
    raise an exception if the name is already in use"""
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
    block_id = await get_dbtcore_block_id(blockname)
    if block_id is None:
        logger.error("no dbt core block having name %s", blockname)
        raise HTTPException(status_code=400, detail="no block having name " + blockname)
    logger.info("blockname => blockid : %s => %s", blockname, block_id)
    return {"block_id": block_id}


@app.post("/proxy/blocks/dbtcore/")
async def post_dbtcore(payload: DbtCoreCreate):
    """create a new dbt_core block with this block name,
    raise an exception if the name is already in use"""
    logger.info(payload)
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
    try:
        return get_flow_run_logs(flow_run_id, offset)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to fetch logs for flow_run"
        ) from error


@app.delete("/proxy/deployments/{deployment_id}")
def delete_deployment(deployment_id):
    """Delete a deployment"""

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

    try:
        res = await post_deployment_flow_run(deployment_id)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(
            status_code=400, detail="failed to create flow_run for deployment"
        ) from error

    return res
