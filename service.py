"""interface with prefect's python client api"""
from http import HTTPStatus
import os
import requests
from fastapi.responses import JSONResponse
from fastapi import HTTPException

from prefect.deployments import Deployment, run_deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_airbyte import AirbyteConnection, AirbyteServer

from prefect_gcp import GcpCredentials
from prefect_dbt.cli.configs import TargetConfigs
from prefect_dbt.cli.configs import BigQueryTargetConfigs
from prefect_dbt.cli.commands import DbtCoreOperation, ShellOperation
from prefect_dbt.cli import DbtCliProfile
from dotenv import load_dotenv
from logger import logger


from helpers import cleaned_name_for_dbtblock
from exception import PrefectException
from schemas import (
    AirbyteServerCreate,
    AirbyteConnectionCreate,
    PrefectShellSetup,
    DbtCoreCreate,
    DeploymentCreate,
)
from flows import (
    deployment_schedule_flow,
)

load_dotenv()

FLOW_RUN_FAILED = "FAILED"
FLOW_RUN_COMPLETED = "COMPLETED"
FLOW_RUN_SCHEDULED = "SCHEDULED"


def prefect_post(endpoint, payload):
    """POST request to prefect server"""

    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")
    
    root = os.getenv("PREFECT_API_URL")
    res = requests.post(f"{root}/{endpoint}", timeout=30, json=payload)
    logger.info(res.text)
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="Invalid input data") from error
    return res.json()


def prefect_get(endpoint):
    """GET request to prefect server"""

    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")

    root = os.getenv("PREFECT_API_URL")
    res = requests.get(f"{root}/{endpoint}", timeout=30)
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="Invalid request") from error
    return res.json()


def prefect_delete(endpoint):
    """DELETE request to prefect server"""

    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")
    
    root = os.getenv("PREFECT_API_URL")
    res = requests.delete(f"{root}/{endpoint}", timeout=30)
    try:
        res.raise_for_status()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail="Invalid request") from error
    return res.json()


def _block_id(block):
    return str(block.dict()["_block_document_id"])


# ================================================================================================
async def get_airbyte_server_block_id(blockname) -> str | None:
    """look up an airbyte server block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    try:
        block = await AirbyteServer.load(blockname)
        logger.info("found airbyte server block named %s", blockname)
        return _block_id(block)
    except ValueError:
        logger.error("no airbyte server block named %s", blockname)
        return None


async def create_airbyte_server_block(payload: AirbyteServerCreate) -> str:
    """Create airbyte server block in prefect"""

    if not isinstance(payload, AirbyteServerCreate):
        raise TypeError("payload must be an AirbyteServerCreate")

    airbyteservercblock = AirbyteServer(
        server_host=payload.serverHost,
        server_port=payload.serverPort,
        api_version=payload.apiVersion,
    )
    try:
        await airbyteservercblock.save(payload.blockName)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create airbyte server block") from error
    logger.info("created airbyte server block named %s", payload.blockName)
    return _block_id(airbyteservercblock)


def update_airbyte_server_block(blockname):
    """We don't update server blocks"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    raise PrefectException("not implemented")


def delete_airbyte_server_block(blockid):
    """Delete airbyte server block"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")
    
    logger.info("deleting airbyte server block %s", blockid)
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_airbyte_connection_block_id(blockname) -> str | None:
    """look up airbyte connection block by name and return block_id"""

    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    try:
        block = await AirbyteConnection.load(blockname)
        logger.info("found airbyte connection block named %s", blockname)
        return _block_id(block)
    except ValueError:
        logger.error("no airbyte connection block named %s", blockname)
        raise HTTPException(status_code=404, detail=f"No airbyte connection block named {blockname}")


async def get_airbyte_connection_block(blockid):
    """look up and return block data for an airbyte connection"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")
    try:
        result = prefect_get(f"block_documents/{blockid}")
        logger.info("found airbyte connection block having id %s", blockid)
        return result
    except requests.exceptions.HTTPError:
        logger.error("no airbyte connection block having id %s", blockid)
        raise HTTPException(status_code=404, detail=f"No airbyte connection block having id {blockid}")


async def create_airbyte_connection_block(
    conninfo: AirbyteConnectionCreate,
) -> str:
    """Create airbyte connection block"""
    if not isinstance(conninfo, AirbyteConnectionCreate):
        raise TypeError("conninfo must be an AirbyteConnectionCreate")
    
    logger.info(conninfo)
    try:
        serverblock = await AirbyteServer.load(conninfo.serverBlockName)
    except ValueError as exc:
        logger.exception(exc)
        raise PrefectException(
            f"could not find Airbyte Server block named {conninfo.serverBlockName}"
        ) from exc

    connection_block = AirbyteConnection(
        airbyte_server=serverblock,
        connection_id=conninfo.connectionId,
    )
    try:
        await connection_block.save(conninfo.connectionBlockName)
    except Exception as error:
        logger.exception(error)
        raise PrefectException(
            f"failed to create airbyte connection block for connection {conninfo.connectionId}"
        ) from error
    logger.info("created airbyte connection block %s", conninfo.connectionBlockName)

    return _block_id(connection_block)


def update_airbyte_connection_block(blockname):
    """We don't update connection blocks"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    
    raise PrefectException("not implemented")


def delete_airbyte_connection_block(blockid):
    """Delete airbyte connection block in prefect"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")
    
    logger.info("deleting airbyte connection block %s", blockid)
    
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_shell_block_id(blockname) -> str | None:
    """look up a shell operation block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    
    try:
        block = await ShellOperation.load(blockname)
        return _block_id(block)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"No shell operation block named {blockname}")


async def create_shell_block(shell: PrefectShellSetup):
    """Create a prefect shell block"""

    if not isinstance(shell, PrefectShellSetup):
        raise TypeError("shell must be a PrefectShellSetup")

    shell_operation_block = ShellOperation(
        commands=shell.commands, env=shell.env, working_dir=shell.workingDir
    )
    try:
        await shell_operation_block.save(shell.blockName)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create shell block") from error
    logger.info("created shell operation block %s", shell.blockName)
    return _block_id(shell_operation_block)


def delete_shell_block(blockid):
    """Delete a prefect shell block"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")
    
    logger.info("deleting shell operation block %s", blockid)
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_dbtcore_block_id(blockname) -> str | None:
    """look up a dbt core operation block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    
    try:
        block = await DbtCoreOperation.load(blockname)
        return _block_id(block)
    except ValueError:
        raise HTTPException(status_code=404, detail=f"No dbt core operation block named {blockname}")


async def _create_dbt_cli_profile(payload: DbtCoreCreate):
    """credentials are decrypted by now"""
    if not isinstance(payload, DbtCoreCreate):
        raise TypeError("payload must be a DbtCoreCreate")
    logger.info(payload)

    if payload.wtype == "postgres":
        target_configs = TargetConfigs(
            type="postgres",
            schema=payload.profile.target_configs_schema,
            extras={
                "user": payload.credentials["username"],
                "password": payload.credentials["password"],
                "dbname": payload.credentials["database"],
                "host": payload.credentials["host"],
                "port": payload.credentials["port"],
            },
        )

    elif payload.wtype == "bigquery":
        dbcredentials = GcpCredentials(service_account_info=payload.credentials)
        target_configs = BigQueryTargetConfigs(
            credentials=dbcredentials,
            schema=payload.profile.target_configs_schema,
        )
    else:
        raise PrefectException("unknown wtype: " + payload.wtype)

    dbt_cli_profile = DbtCliProfile(
        name=payload.profile.name,
        target=payload.profile.target,
        target_configs=target_configs,
    )
    # await dbt_cli_profile.save(
    #     cleaned_name_for_dbtblock(payload.profile.name), overwrite=True
    # )
    return dbt_cli_profile


async def create_dbt_core_block(payload: DbtCoreCreate):
    """Create a dbt core block in prefect"""
    if not isinstance(payload, DbtCoreCreate):
        raise TypeError("payload must be a DbtCoreCreate")
    logger.info(payload)

    dbt_cli_profile = await _create_dbt_cli_profile(payload)
    dbt_core_operation = DbtCoreOperation(
        commands=payload.commands,
        env=payload.env,
        working_dir=payload.working_dir,
        profiles_dir=payload.profiles_dir,
        project_dir=payload.project_dir,
        dbt_cli_profile=dbt_cli_profile,
    )
    cleaned_blockname = cleaned_name_for_dbtblock(payload.blockName)
    try:
        await dbt_core_operation.save(cleaned_blockname, overwrite=True)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create dbt core op block") from error

    logger.info("created dbt core operation block %s", payload.blockName)

    return _block_id(dbt_core_operation), cleaned_blockname


def delete_dbt_core_block(block_id):
    """Delete a dbt core block in prefect"""
    if not isinstance(block_id, str):
        raise TypeError("block_id must be a string")
    
    logger.info("deleting dbt core operation block %s", block_id)
    return prefect_delete(f"block_documents/{block_id}")


# ================================================================================================
async def post_deployment(payload: DeploymentCreate) -> None:
    """create a deployment from a flow and a schedule"""
    if not isinstance(payload, DeploymentCreate):
        raise TypeError("payload must be a DeploymentCreate")
    logger.info(payload)

    logger.info(payload)

    deployment = await Deployment.build_from_flow(
        flow=deployment_schedule_flow.with_options(name=payload.flow_name),
        name=payload.deployment_name,
        work_queue_name="ddp",
        tags=[payload.org_slug],
    )
    deployment.parameters = {
        "airbyte_blocks": payload.connection_blocks,
        "dbt_blocks": payload.dbt_blocks,
    }
    deployment.schedule = CronSchedule(cron=payload.cron) if payload.cron else None
    try:
        deployment_id = await deployment.apply()
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create deployment") from error
    return {"id": deployment_id, "name": deployment.name}


def get_flow_runs_by_deployment_id(deployment_id, limit):
    """Fetch flow runs of a deployment that are FAILED/COMPLETED,
    sorted by descending start time of each run"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    if not isinstance(limit, int):
        raise TypeError("limit must be an integer")
    if limit < 0:
        raise ValueError("limit must be a positive integer")
    logger.info("fetching flow runs for deployment %s", deployment_id)

    query = {
        "sort": "START_TIME_DESC",
        "deployments": {"id": {"any_": [deployment_id]}},
        "flow_runs": {
            "operator": "and_",
            "state": {"type": {"any_": [FLOW_RUN_COMPLETED, FLOW_RUN_FAILED]}},
        },
    }

    if limit > 0:
        query["limit"] = limit

    flow_runs = []

    try:
        result = prefect_post("flow_runs/filter", query)
    except Exception as error:
        logger.exception(error)
        raise PrefectException(
            f"failed to fetch flow_runs for deployment {deployment_id}"
        ) from error
    for flow_run in result:
        flow_runs.append(
            {
                "id": flow_run["id"],
                "name": flow_run["name"],
                "tags": flow_run["tags"],
                "startTime": flow_run["start_time"],
                "expectedStartTime": flow_run["expected_start_time"],
                "totalRunTime": flow_run["total_run_time"],
                "status": flow_run["state"]["type"],
            }
        )

    return flow_runs


def get_deployments_by_filter(org_slug, deployment_ids=[]):
    # pylint: disable=dangerous-default-value
    """fetch all deployments by org"""
    if not isinstance(org_slug, str):
        raise TypeError("org_slug must be a string")
    if not isinstance(deployment_ids, list):
        raise TypeError("deployment_ids must be a list")
    query = {
        "deployments": {
            "operator": "and_",
            "tags": {"all_": [org_slug]},
            "id": {"any_": deployment_ids},
        }
    }

    try:
        res = prefect_post(
            "deployments/filter",
            query,
        )
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to fetch deployments by filter") from error

    deployments = []

    for deployment in res:
        deployments.append(
            {
                "name": deployment["name"],
                "deploymentId": deployment["id"],
                "tags": deployment["tags"],
                "cron": deployment["schedule"]["cron"],
            }
        )

    return deployments


async def post_deployment_flow_run(deployment_id):
    # pylint: disable=broad-exception-caught
    """Create deployment flow run"""

    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    try:
        flow_run = await run_deployment(deployment_id, timeout=0)
        return {"flow_run_id": flow_run.id}
    except Exception as exc:
        logger.exception(exc)
        # why are we not just raising a prefect-exception here
        return JSONResponse(content={"detail": str(exc)}, status_code=500)


def parse_log(log):
    """select level, timestamp, message from ..."""

    if not isinstance(log, dict):
        raise TypeError("log must be a dict")
    return {
        "level": log["level"],
        "timestamp": log["timestamp"],
        "message": log["message"],
    }


def traverse_flow_run_graph(flow_run_id: str, flow_runs: list):
    """This recursive function will read through the graph
    and return all sub flow run ids of the parent that can potentially have logs"""

    if not isinstance(flow_run_id, str):
        raise TypeError("flow_run_id must be a string")
    if not isinstance(flow_runs, list):
        raise TypeError("flow_runs must be a list")
    flow_runs.append(flow_run_id)
    if flow_run_id is None:
        return flow_runs

    flow_graph_data = prefect_get(f"flow_runs/{flow_run_id}/graph")

    if len(flow_graph_data) == 0:
        return flow_runs

    for flow in flow_graph_data:
        if (
            "state" in flow
            and "state_details" in flow["state"]
            and flow["state"]["state_details"]["child_flow_run_id"]
        ):
            traverse_flow_run_graph(
                flow["state"]["state_details"]["child_flow_run_id"], flow_runs
            )

    return flow_runs


def get_flow_run_logs(flow_run_id: str, offset: int):
    """return logs from a flow run"""
    if not isinstance(flow_run_id, str):
        raise TypeError("flow_run_id must be a string")
    if not isinstance(offset, int):
        raise TypeError("offset must be an integer")
    flow_run_ids = traverse_flow_run_graph(flow_run_id, [])

    logs = prefect_post(
        "logs/filter",
        {
            "logs": {
                "operator": "and_",
                "flow_run_id": {"any_": flow_run_ids},
            },
            "sort": "TIMESTAMP_ASC",
            "offset": offset,
        },
    )
    return {
        "offset": offset,
        "logs": list(map(parse_log, logs)),
    }


def get_flow_runs_by_name(flow_run_name):
    """Query flow run from the name"""
    if not isinstance(flow_run_name, str):
        raise TypeError("flow_run_name must be a string")
    query = {
        "flow_runs": {"operator": "and_", "name": {"any_": [flow_run_name]}},
    }

    try:
        flow_runs = prefect_post("flow_runs/filter", query)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to fetch flow-runs by name") from error
    return flow_runs
