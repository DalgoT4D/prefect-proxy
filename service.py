"""interface with prefect's python client api"""
import os
import requests


from prefect.deployments import Deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect_airbyte import AirbyteConnection, AirbyteServer

from prefect_gcp import GcpCredentials
from prefect_dbt.cli.configs import TargetConfigs
from prefect_dbt.cli.configs import BigQueryTargetConfigs
from prefect_dbt.cli.commands import DbtCoreOperation, ShellOperation
from prefect_dbt.cli import DbtCliProfile

from dotenv import load_dotenv

from helpers import cleaned_name_for_dbtblock
from exception import PrefectException
from schemas import (
    AirbyteServerCreate,
    AirbyteConnectionCreate,
    PrefectShellSetup,
    DbtCoreCreate,
    DeploymentCreate,
    RunFlow,
)
from flows import (
    deployment_schedule_flow,
    run_airbyte_connection_flow,
    run_dbtcore_flow,
)

load_dotenv()

FLOW_RUN_FAILED = "FAILED"
FLOW_RUN_COMPLETED = "COMPLETED"
FLOW_RUN_SCHEDULED = "SCHEDULED"


def prefect_post(endpoint, payload):
    """POST request to prefect server"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.post(f"{root}/{endpoint}", timeout=30, json=payload)
    print(res.text)
    res.raise_for_status()
    return res.json()


def prefect_get(endpoint):
    """GET request to prefect server"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.get(f"{root}/{endpoint}", timeout=30)
    res.raise_for_status()
    return res.json()


def prefect_delete(endpoint):
    """DELETE request to prefect server"""
    root = os.getenv("PREFECT_API_URL")
    res = requests.delete(f"{root}/{endpoint}", timeout=30)
    res.raise_for_status()


def _block_id(block):
    return str(block.dict()["_block_document_id"])


# ================================================================================================
async def get_airbyte_server_block_id(blockname) -> str | None:
    """look up an airbyte server block by name and return block_id"""
    try:
        block = await AirbyteServer.load(blockname)
        return _block_id(block)
    except ValueError:
        return None


async def create_airbyte_server_block(payload: AirbyteServerCreate) -> str:
    """Create airbyte server block in prefect"""

    airbyteservercblock = AirbyteServer(
        server_host=payload.serverHost,
        server_port=payload.serverPort,
        api_version=payload.apiVersion,
    )
    await airbyteservercblock.save(payload.blockName)
    return _block_id(airbyteservercblock)


def update_airbyte_server_block(blockname):
    """We don't update server blocks"""
    raise PrefectException("not implemented")


def delete_airbyte_server_block(blockid):
    """Delete airbyte server block"""
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_airbyte_connection_block_id(blockname) -> str | None:
    """look up airbyte connection block by name and return block_id"""
    try:
        block = await AirbyteConnection.load(blockname)
        return _block_id(block)
    except ValueError:
        return None


async def get_airbyte_connection_block(blockid):
    """look up and return block data for an airbyte connection"""
    result = prefect_get(f"block_documents/{blockid}")
    return result


async def create_airbyte_connection_block(
    conninfo: AirbyteConnectionCreate,
) -> str:
    """Create airbyte connection block"""

    try:
        serverblock = await AirbyteServer.load(conninfo.serverBlockName)
    except ValueError as exc:
        raise PrefectException(
            f"could not find Airbyte Server block named {conninfo.serverBlockName}"
        ) from exc

    connection_block = AirbyteConnection(
        airbyte_server=serverblock,
        connection_id=conninfo.connectionId,
    )
    await connection_block.save(conninfo.connectionBlockName)

    return _block_id(connection_block)


def update_airbyte_connection_block(blockname):
    """We don't update connection blocks"""
    raise PrefectException("not implemented")


def delete_airbyte_connection_block(blockid):
    """Delete airbyte connection block in prefect"""
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_shell_block_id(blockname) -> str | None:
    """look up a shell operation block by name and return block_id"""
    try:
        block = await ShellOperation.load(blockname)
        return _block_id(block)
    except ValueError:
        return None


async def create_shell_block(shell: PrefectShellSetup):
    """Create a prefect shell block"""

    shell_operation_block = ShellOperation(
        commands=shell.commands, env=shell.env, working_dir=shell.workingDir
    )
    await shell_operation_block.save(shell.blockName)
    return _block_id(shell_operation_block)


def delete_shell_block(blockid):
    """Delete a prefect shell block"""
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_dbtcore_block_id(blockname) -> str | None:
    """look up a dbt core operation block by name and return block_id"""
    try:
        block = await DbtCoreOperation.load(blockname)
        return _block_id(block)
    except ValueError:
        return None


async def _create_dbt_cli_profile(payload: DbtCoreCreate):
    """credentials are decrypted by now"""

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
    await dbt_core_operation.save(cleaned_blockname, overwrite=True)

    return _block_id(dbt_core_operation), cleaned_blockname


def delete_dbt_core_block(block_id):
    """Delete a dbt core block in prefect"""
    return prefect_delete(f"block_documents/{block_id}")


# ================================================================================================
def run_airbyte_connection_prefect_flow(payload: RunFlow):
    """Run an Airbyte Connection sync"""

    flow = run_airbyte_connection_flow
    if payload.flowName:
        flow = flow.with_options(name=payload.flowName)
    if payload.flowRunName:
        flow = flow.with_options(flow_run_name=payload.flowRunName)
    return flow(payload)


def run_dbtcore_prefect_flow(payload: RunFlow):
    """Run a dbt core flow"""

    flow = run_dbtcore_flow
    if payload.flowName:
        flow = flow.with_options(name=payload.flowName)
    if payload.flowRunName:
        flow = flow.with_options(flow_run_name=payload.flowRunName)
    return flow(payload)


async def post_deployment(payload: DeploymentCreate) -> None:
    """create a deployment from a flow and a schedule"""
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
    deployment.schedule = CronSchedule(cron=payload.cron)
    deployment_id = await deployment.apply()
    return {"id": deployment_id, "name": deployment.name}


def get_flow_runs_by_deployment_id(deployment_id, limit):
    """Fetch flow runs of a deployment that are FAILED/COMPLETED,
    sorted by descending start time of each run"""
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

    for flow_run in prefect_post("flow_runs/filter", query):
        flow_runs.append(
            {
                "tags": flow_run["tags"],
                "startTime": flow_run["start_time"],
                "status": flow_run["state"]["type"],
            }
        )

    return flow_runs


def get_deployments_by_filter(org_slug, deployment_ids=[]):
    # pylint: disable=dangerous-default-value
    """fetch all deployments by org"""
    query = {
        "deployments": {
            "operator": "and_",
            "tags": {"all_": [org_slug]},
            "id": {"any_": deployment_ids},
        }
    }

    res = prefect_post(
        "deployments/filter",
        query,
    )

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


def parse_log(log):
    """select level, timestamp, message from ..."""
    return {
        "level": log["level"],
        "timestamp": log["timestamp"],
        "message": log["message"],
    }


def get_flow_run_logs(flow_run_id: str, offset: int):
    """return logs from a flow run"""
    logs = prefect_post(
        "logs/filter",
        {
            "logs": {
                "operator": "and_",
                "flow_run_id": {"any_": [flow_run_id]},
            },
            "sort": "TIMESTAMP_ASC",
            "offset": offset,
        },
    )
    return {
        "offset": offset,
        "logs": list(map(parse_log, logs)),
    }
