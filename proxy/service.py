"""interface with prefect's python client api"""

import os
from time import sleep
import requests
from fastapi import HTTPException

from prefect.deployments import Deployment, run_deployment
from prefect.server.schemas.schedules import CronSchedule
from prefect.server.schemas.states import Cancelled
from prefect.blocks.system import Secret
from prefect.blocks.core import Block
from prefect.client import get_client
from prefect_airbyte import AirbyteServer

from prefect_gcp import GcpCredentials
from prefect_dbt.cli.configs import TargetConfigs
from prefect_dbt.cli.configs import BigQueryTargetConfigs
from prefect_dbt.cli.commands import DbtCoreOperation
from prefect_dbt.cli import DbtCliProfile
from dotenv import load_dotenv


from proxy.helpers import CustomLogger, cleaned_name_for_prefectblock
from proxy.exception import PrefectException
from proxy.schemas import (
    AirbyteServerCreate,
    DbtCoreCreate,
    DeploymentCreate2,
    DeploymentUpdate,
    PrefectSecretBlockCreate,
    DbtCliProfileBlockCreate,
    DbtCliProfileBlockUpdate,
    DeploymentUpdate2,
)
from proxy.prefect_flows import deployment_schedule_flow_v4

load_dotenv()

FLOW_RUN_FAILED = "FAILED"
FLOW_RUN_COMPLETED = "COMPLETED"
FLOW_RUN_SCHEDULED = "SCHEDULED"


logger = CustomLogger("prefect-proxy")


def prefect_post(endpoint: str, payload: dict) -> dict:
    """POST request to prefect server"""
    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")

    root = os.getenv("PREFECT_API_URL")
    res = requests.post(f"{root}/{endpoint}", timeout=30, json=payload)

    try:
        res.raise_for_status()
        return res.json()
    except Exception as error:  # pylint:disable=broad-exception-caught
        logger.exception(error)
        # try again
        sleep(3)

    res = requests.post(f"{root}/{endpoint}", timeout=30, json=payload)
    try:
        res.raise_for_status()
        return res.json()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=res.text) from error


def prefect_patch(endpoint: str, payload: dict) -> dict:
    """POST request to prefect server"""
    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")
    if not isinstance(payload, dict):
        raise TypeError("payload must be a dictionary")

    root = os.getenv("PREFECT_API_URL")
    res = requests.patch(f"{root}/{endpoint}", timeout=30, json=payload)

    try:
        res.raise_for_status()
        # no content
        if res.status_code == 204:
            return {}
        return res.json()
    except Exception as error:  # pylint:disable=broad-exception-caught
        logger.exception(error)
        # try again
        sleep(3)

    res = requests.patch(f"{root}/{endpoint}", timeout=30, json=payload)
    try:
        res.raise_for_status()
        # no content
        if res.status_code == 204:
            return {}
        return res.json()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=res.text) from error


def prefect_get(endpoint: str) -> dict:
    """GET request to prefect server"""
    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")

    root = os.getenv("PREFECT_API_URL")
    res = requests.get(f"{root}/{endpoint}", timeout=30)
    try:
        res.raise_for_status()
        return res.json()
    except Exception as error:  # pylint:disable=broad-exception-caught
        logger.exception(error)
        # try again
        sleep(3)

    res = requests.get(f"{root}/{endpoint}", timeout=30)
    try:
        res.raise_for_status()
        return res.json()
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=res.text) from error


def prefect_delete(endpoint: str) -> dict:
    """DELETE request to prefect server"""
    if not isinstance(endpoint, str):
        raise TypeError("endpoint must be a string")

    root = os.getenv("PREFECT_API_URL")
    res = requests.delete(f"{root}/{endpoint}", timeout=30)
    try:
        res.raise_for_status()
        # no content
        if res.status_code == 204:
            return {}
        return res.json()
    except Exception as error:  # pylint:disable=broad-exception-caught
        logger.exception(error)
        sleep(3)

    res = requests.delete(f"{root}/{endpoint}", timeout=30)
    try:
        res.raise_for_status()
        # no content
        if res.status_code == 204:
            return {}
        return res.json()

    except Exception as error:  # pylint:disable=broad-exception-caught
        logger.exception(error)
        raise HTTPException(status_code=400, detail=res.text) from error


def _block_id(block: Block) -> str:
    """Get the id of block"""
    return str(block.dict()["_block_document_id"])


def _block_name(block: Block) -> str:
    """Get the name of block"""
    return str(block.dict()["_block_document_name"])


# ================================================================================================
async def get_airbyte_server_block_id(blockname: str) -> str | None:
    """look up an airbyte server block by name and return block_id"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    try:
        block = await AirbyteServer.load(blockname)
        logger.info(
            "found airbyte server block named %s",
            blockname,
        )
        return _block_id(block)
    except ValueError:
        logger.error("no airbyte server block named %s", blockname)
        return None


async def get_airbyte_server_block(blockname: str) -> dict | None:
    """look up an airbyte server block by name and return block"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    try:
        block = await AirbyteServer.load(blockname)
        logger.info(
            "found airbyte server block named %s",
            blockname,
        )
        return block
    except ValueError:
        logger.error("no airbyte server block named %s", blockname)
        return None


async def create_airbyte_server_block(payload: AirbyteServerCreate):
    """Create airbyte server block in prefect"""
    if not isinstance(payload, AirbyteServerCreate):
        raise TypeError("payload must be an AirbyteServerCreate")
    airbyteservercblock = AirbyteServer(
        server_host=payload.serverHost,
        server_port=payload.serverPort,
        api_version=payload.apiVersion,
    )
    try:
        block_name_for_save = cleaned_name_for_prefectblock(payload.blockName)
        await airbyteservercblock.save(block_name_for_save)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create airbyte server block") from error
    logger.info("created airbyte server block named %s", payload.blockName)
    return _block_id(airbyteservercblock), block_name_for_save


def update_airbyte_server_block(blockname: str):
    """We don't update server blocks"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")
    raise PrefectException("not implemented")


def delete_airbyte_server_block(blockid: str):
    """Delete airbyte server block"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")

    logger.info("deleting airbyte server block %s", blockid)
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================


def update_airbyte_connection_block(blockname: str):
    """We don't update connection blocks"""
    if not isinstance(blockname, str):
        raise TypeError("blockname must be a string")

    raise PrefectException("not implemented")


def delete_airbyte_connection_block(blockid: str) -> dict:
    """Delete airbyte connection block in prefect"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")

    logger.info("deleting airbyte connection block %s", blockid)

    return prefect_delete(f"block_documents/{blockid}")


# ===============================================================================================


def delete_shell_block(blockid: str) -> dict:
    """Delete a prefect shell block"""
    if not isinstance(blockid, str):
        raise TypeError("blockid must be a string")
    logger.info("deleting shell operation block %s", blockid)
    return prefect_delete(f"block_documents/{blockid}")


# ================================================================================================
async def get_dbt_cli_profile(cli_profile_block_name: str) -> dict:
    """look up a dbt cli profile block by name and return block_id"""
    if not isinstance(cli_profile_block_name, str):
        raise TypeError("blockname must be a string")

    try:
        block = await DbtCliProfile.load(cli_profile_block_name)
        return block.get_profile()
    except ValueError:
        # pylint: disable=raise-missing-from
        raise HTTPException(
            status_code=404,
            detail=f"No dbt cli profile block named {cli_profile_block_name}",
        )


async def _create_dbt_cli_profile(
    payload: DbtCliProfileBlockCreate | DbtCoreCreate,
) -> DbtCliProfile:
    """credentials are decrypted by now"""
    if not (
        isinstance(payload, DbtCliProfileBlockCreate)
        or isinstance(payload, DbtCoreCreate)
    ):
        raise TypeError("payload is of wrong type")
    # logger.info(payload) DO NOT LOG - CONTAINS SECRETS
    if payload.wtype == "postgres":
        extras = payload.credentials
        extras["user"] = extras["username"]
        target_configs = TargetConfigs(
            type="postgres",
            schema=payload.profile.target_configs_schema,
            extras=extras,
            allow_field_overrides=True,
        )

    elif payload.wtype == "bigquery":
        dbcredentials = GcpCredentials(service_account_info=payload.credentials)
        target_configs = BigQueryTargetConfigs(
            credentials=dbcredentials,
            schema=payload.profile.target_configs_schema,
            extras={
                "location": payload.bqlocation,
            },
        )
    else:
        raise PrefectException("unknown wtype: " + payload.wtype)

    try:
        dbt_cli_profile = DbtCliProfile(
            name=payload.profile.name,
            target=payload.profile.target_configs_schema,
            target_configs=target_configs,
        )
        cleaned_blockname = cleaned_name_for_prefectblock(
            payload.cli_profile_block_name
        )
        await dbt_cli_profile.save(
            cleaned_blockname,
            overwrite=True,
        )
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create dbt cli profile") from error

    return dbt_cli_profile, _block_id(dbt_cli_profile), cleaned_blockname


async def update_dbt_cli_profile(payload: DbtCliProfileBlockUpdate):
    """
    Update the schema, warehouse credentials or profile in cli profile block
    """
    try:
        dbtcli_block: DbtCliProfile = await DbtCliProfile.load(
            payload.cli_profile_block_name
        )
    except Exception as error:
        raise PrefectException(
            "no dbt cli profile block named " + payload.cli_profile_block_name
        ) from error

    if not isinstance(payload, DbtCliProfileBlockUpdate):
        raise TypeError("payload is of wrong type")

    try:
        # schema
        if payload.profile and payload.profile.target_configs_schema:
            dbtcli_block.target_configs.schema = payload.profile.target_configs_schema
            dbtcli_block.target = (
                payload.profile.target_configs_schema
            )  # by default output(s) target in profiles.yml will be target_configs_schema

        # target
        if payload.profile and payload.profile.target:
            dbtcli_block.target = payload.profile.target

        # profile name present in profiles.yml; should be the same as dbt_project.yml
        if payload.profile and payload.profile.name:
            dbtcli_block.name = payload.profile.name

        # update credentials
        if payload.credentials:
            if payload.wtype is None:
                raise TypeError("wtype is required")
            if payload.wtype == "postgres":
                dbtcli_block.target_configs.extras = payload.credentials
                dbtcli_block.target_configs.extras["user"] = (
                    dbtcli_block.target_configs.extras["username"]
                )

            elif payload.wtype == "bigquery":
                dbcredentials = GcpCredentials(service_account_info=payload.credentials)
                dbtcli_block.target_configs.credentials = dbcredentials
                dbtcli_block.target_configs.extras = {
                    "location": payload.bqlocation,
                }
            else:
                raise PrefectException("unknown wtype: " + payload.wtype)

        block_name = (
            cleaned_name_for_prefectblock(payload.new_block_name)
            if payload.new_block_name
            else payload.cli_profile_block_name
        )

        await dbtcli_block.save(
            name=block_name,
            overwrite=True,
        )

    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to update dbt cli profile") from error

    return dbtcli_block, _block_id(dbtcli_block), payload.cli_profile_block_name


async def create_dbt_core_block(payload: DbtCoreCreate):
    """Create a dbt core block in prefect"""
    if not isinstance(payload, DbtCoreCreate):
        raise TypeError("payload must be a DbtCoreCreate")
    # logger.info(payload) DO NOT LOG - CONTAINS SECRETS

    dbt_cli_profile, _, _ = await _create_dbt_cli_profile(payload)
    dbt_core_operation = DbtCoreOperation(
        commands=payload.commands,
        env=payload.env,
        working_dir=payload.working_dir,
        profiles_dir=payload.profiles_dir,
        project_dir=payload.project_dir,
        dbt_cli_profile=dbt_cli_profile,
    )
    cleaned_blockname = cleaned_name_for_prefectblock(payload.blockName)
    try:
        await dbt_core_operation.save(cleaned_blockname, overwrite=True)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create dbt core op block") from error

    logger.info("created dbt core operation block %s", payload.blockName)

    return _block_id(dbt_core_operation), cleaned_blockname


def delete_dbt_core_block(block_id: str) -> dict:
    """Delete a dbt core block in prefect"""
    if not isinstance(block_id, str):
        raise TypeError("block_id must be a string")

    logger.info("deleting dbt core operation block %s", block_id)
    return prefect_delete(f"block_documents/{block_id}")


async def create_secret_block(payload: PrefectSecretBlockCreate):
    """Create a prefect block of type secret"""
    try:
        secret_block = Secret(value=payload.secret)
        cleaned_blockname = cleaned_name_for_prefectblock(payload.blockName)
        await secret_block.save(cleaned_blockname, overwrite=True)
    except Exception as error:
        raise PrefectException("Could not create a secret block") from error

    return _block_id(secret_block), cleaned_blockname


async def update_postgres_credentials(dbt_blockname, new_extras):
    """updates the database credentials inside a dbt postgres block"""
    try:
        block: DbtCoreOperation = await DbtCoreOperation.load(dbt_blockname)
    except Exception as error:
        raise PrefectException("no dbt core op block named " + dbt_blockname) from error

    if block.dbt_cli_profile.target_configs.type != "postgres":
        raise TypeError("wrong blocktype")

    aliases = {
        "dbname": "database",
        "username": "user",
    }

    extras = block.dbt_cli_profile.target_configs.dict()["extras"]
    cleaned_extras = {}
    # copy existing extras over to cleaned_extras with the right keys
    for key, value in extras.items():
        cleaned_extras[aliases.get(key, key)] = value

    # copy new extras over to cleaned_extras with the right keys
    for key, value in new_extras.items():
        cleaned_extras[aliases.get(key, key)] = value

    block.dbt_cli_profile.target_configs = TargetConfigs(
        type=block.dbt_cli_profile.target_configs.type,
        schema=block.dbt_cli_profile.target_configs.dict()["schema"],
        extras=cleaned_extras,
    )

    try:
        await block.dbt_cli_profile.save(
            name=cleaned_name_for_prefectblock(block.dbt_cli_profile.name),
            overwrite=True,
        )
        await block.save(cleaned_name_for_prefectblock(dbt_blockname), overwrite=True)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to update dbt cli profile [postgres]") from error


async def update_bigquery_credentials(dbt_blockname: str, credentials: dict):
    """updates the database credentials inside a dbt bigquery block"""
    try:
        block: DbtCoreOperation = await DbtCoreOperation.load(dbt_blockname)
    except Exception as error:
        raise PrefectException("no dbt core op block named " + dbt_blockname) from error

    if block.dbt_cli_profile.target_configs.type != "bigquery":
        raise TypeError("wrong blocktype")

    dbcredentials = GcpCredentials(service_account_info=credentials)

    block.dbt_cli_profile.target_configs = BigQueryTargetConfigs(
        credentials=dbcredentials,
        schema=block.dbt_cli_profile.target_configs.dict()["schema_"],
        extras=block.dbt_cli_profile.target_configs.dict()["extras"],
    )

    try:
        await block.dbt_cli_profile.save(
            name=cleaned_name_for_prefectblock(block.dbt_cli_profile.name),
            overwrite=True,
        )
        await block.save(cleaned_name_for_prefectblock(dbt_blockname), overwrite=True)
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to update dbt cli profile [bigquery]") from error


async def update_target_configs_schema(dbt_blockname: str, target_configs_schema: str):
    """updates the target inside a dbt bigquery block"""
    try:
        block: DbtCoreOperation = await DbtCoreOperation.load(dbt_blockname)
    except Exception as error:
        raise PrefectException("no dbt core op block named " + dbt_blockname) from error

    block.dbt_cli_profile.target_configs.schema = target_configs_schema
    block.dbt_cli_profile.target = target_configs_schema

    # update the dbt command "dbt <command> --target <target>"
    if len(block.commands) > 0:
        option_index = block.commands[0].find("--target")
        if option_index > -1:
            prefix = block.commands[0][: option_index + len("--target ")]
            new_command = prefix + target_configs_schema
            block.commands[0] = new_command

    try:
        await block.dbt_cli_profile.save(
            name=cleaned_name_for_prefectblock(block.dbt_cli_profile.name),
            overwrite=True,
        )
        await block.save(cleaned_name_for_prefectblock(dbt_blockname), overwrite=True)
    except Exception as error:
        logger.exception(error)
        raise PrefectException(
            "failed to update dbt cli profile target_configs schema for "
            + dbt_blockname
        ) from error


# ================================================================================================
async def post_deployment_v1(payload: DeploymentCreate2) -> dict:
    """
    create a deployment from a flow and a schedule
    can also optionally pass in the name of the work queue and work pool
    the work pool must already exist
    work queues are created on the fly
    """
    if not isinstance(payload, DeploymentCreate2):
        raise TypeError("payload must be a DeploymentCreate")
    logger.info(payload)

    work_queue_name = payload.work_queue_name if payload.work_queue_name else "ddp"
    work_pool_name = (
        payload.work_pool_name if payload.work_pool_name else "default-agent-pool"
    )

    deployment = await Deployment.build_from_flow(
        flow=deployment_schedule_flow_v4.with_options(name=payload.flow_name),
        name=payload.deployment_name,
        work_queue_name=work_queue_name,
        work_pool_name=work_pool_name,
        tags=[payload.org_slug],
    )
    deployment.parameters = payload.deployment_params
    deployment.schedule = CronSchedule(cron=payload.cron) if payload.cron else None
    try:
        deployment_id = await deployment.apply()
    except Exception as error:
        logger.exception(error)
        raise PrefectException("failed to create deployment") from error
    return {
        "id": deployment_id,
        "name": deployment.name,
        "params": deployment.parameters,
    }


def put_deployment(deployment_id: str, payload: DeploymentUpdate) -> dict:
    """create a deployment from a flow and a schedule"""
    if not isinstance(payload, DeploymentUpdate):
        raise TypeError("payload must be a DeploymentUpdate")

    logger.info(payload)

    schedule = CronSchedule(cron=payload.cron).dict() if payload.cron else None

    payload = {
        "schedule": schedule,
        "parameters": {
            "airbyte_blocks": payload.connection_blocks,
            "dbt_blocks": payload.dbt_blocks,
        },
    }

    # res will be any empty json if success since status code is 204
    res = prefect_patch(f"deployments/{deployment_id}", payload)
    logger.info("Update deployment with ID: %s", deployment_id)
    return res


def put_deployment_v1(deployment_id: str, payload: DeploymentUpdate2) -> dict:
    """
    update a deployment's schedule / work queue / work pool / other paramters
    the work pool must already exist
    work queues are created on the fly
    """
    if not isinstance(payload, DeploymentUpdate2):
        raise TypeError("payload must be a DeploymentUpdate2")

    logger.info(payload)

    newpayload = {}

    if payload.deployment_params:
        newpayload["parameters"] = payload.deployment_params

    if payload.cron:
        newpayload["schedule"] = CronSchedule(cron=payload.cron).dict()

    if payload.work_pool_name:
        newpayload["work_pool_name"] = payload.work_pool_name

    if payload.work_queue_name:
        newpayload["work_queue_name"] = payload.work_queue_name

    # res will be any empty json if success since status code is 204
    res = prefect_patch(f"deployments/{deployment_id}", newpayload)
    logger.info("Update deployment with ID: %s", deployment_id)
    return res


def get_deployment(deployment_id: str) -> dict:
    """Fetch deployment and its details"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    res = prefect_get(f"deployments/{deployment_id}")
    logger.info("Fetched deployment with ID: %s", deployment_id)
    return res


def get_final_state_for_flow_run(flow_run_id: str):
    """fetch final state of subtasks"""
    all_ids_to_look_at = traverse_flow_run_graph(flow_run_id, [])
    query = {
        "flow_runs": {
            "operator": "and_",
            "id": {"any_": all_ids_to_look_at},
        },
    }
    result = prefect_post("task_runs/filter/", query)
    priority = ["Completed", "DBT_TEST_FAILED", "Failed", "Running", "Unknown"]
    as_numeric = list(
        map(
            lambda x: (
                priority.index(x["state"]["name"])
                if x["state"]["name"] in priority
                else 4
            ),
            result,
        )
    )
    if len(as_numeric) == 0:
        return "RUNNING"
    return priority[max(as_numeric)].upper()


def get_flow_runs_by_deployment_id(
    deployment_id: str, limit: int, start_time_gt: str
) -> list:
    """
    Fetch flow runs of a deployment that are FAILED/COMPLETED,
    sorted by descending start time of each run
    """
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
    if start_time_gt:
        query["flow_runs"]["start_time"] = {"after_": start_time_gt}

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
        # get the tasks if any and check their state names
        all_ids_to_look_at = traverse_flow_run_graph(flow_run["id"], [])
        query = {
            "flow_runs": {
                "operator": "and_",
                "id": {"any_": all_ids_to_look_at},
            },
        }
        result = prefect_post("task_runs/filter/", query)
        if "DBT_TEST_FAILED" in [x["state"]["name"] for x in result]:
            final_state_name = "DBT_TEST_FAILED"
        else:
            final_state_name = flow_run["state"]["name"]
        flow_runs.append(
            {
                "id": flow_run["id"],
                "name": flow_run["name"],
                "tags": flow_run["tags"],
                "startTime": flow_run["start_time"],
                "expectedStartTime": flow_run["expected_start_time"],
                "totalRunTime": flow_run["total_run_time"],
                "status": flow_run["state"]["type"],
                "state_name": final_state_name,
            }
        )

    return flow_runs


def get_deployments_by_filter(org_slug: str, deployment_ids=None) -> list:
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
                "cron": (
                    deployment["schedule"]["cron"] if deployment["schedule"] else None
                ),
                "isScheduleActive": deployment["is_schedule_active"],
            }
        )

    return deployments


async def post_deployment_flow_run(deployment_id: str, run_params: dict = None):
    # pylint: disable=broad-exception-caught
    """Create deployment flow run"""
    if not isinstance(deployment_id, str):
        raise TypeError("deployment_id must be a string")
    try:
        flow_run = await run_deployment(deployment_id, timeout=0, parameters=run_params)
        return {"flow_run_id": flow_run.id}
    except Exception as exc:
        logger.exception(exc)
        raise PrefectException("Failed to create deployment flow run") from exc


def parse_log(log: dict) -> dict:
    """select level, timestamp, message from ..."""
    if not isinstance(log, dict):
        raise TypeError("log must be a dict")
    return {
        "level": log["level"],
        "timestamp": log["timestamp"],
        "message": log["message"],
    }


def traverse_flow_run_graph(flow_run_id: str, flow_runs: list) -> list:
    """
    This recursive function will read through the graph
    and return all sub flow run ids of the parent that can potentially have logs
    """
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


def get_flow_run_logs(flow_run_id: str, offset: int) -> dict:
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


def get_flow_runs_by_name(flow_run_name: str) -> dict:
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
    for flow_run in flow_runs:
        flow_run["status"] = flow_run["state"]["type"]
    return flow_runs


def get_flow_run(flow_run_id: str) -> dict:
    """Get a flow run by its id"""
    try:
        flow_run = prefect_get(f"flow_runs/{flow_run_id}")
        final_state = get_final_state_for_flow_run(flow_run_id)
        if final_state != "UNKNOWN":
            flow_run["state_name"] = final_state
    except Exception as err:
        logger.exception(err)
        raise PrefectException("failed to fetch a flow-run") from err
    return flow_run


def set_deployment_schedule(deployment_id: str, status: str) -> None:
    """Set deployment schedule to active or inactive"""
    # both the apis return null below
    if status == "active":
        prefect_post(f"deployments/{deployment_id}/set_schedule_active", {})

    if status == "inactive":
        prefect_post(f"deployments/{deployment_id}/set_schedule_inactive", {})

    return None


async def cancel_flow_run(flow_run_id: str) -> dict:
    """Cancel a flow run"""
    if not isinstance(flow_run_id, str):
        raise TypeError("flow_run_id must be a string")
    try:
        async with get_client() as client:
            # set the state of the provided flow-run to cancelled
            await client.set_flow_run_state(flow_run_id=flow_run_id, state=Cancelled())
    except Exception as err:
        logger.exception(err)
        raise PrefectException("failed to cancel flow-run") from err
    return None
