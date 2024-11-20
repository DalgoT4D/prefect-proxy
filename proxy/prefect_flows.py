"""
Reusable flows
https://docs.prefect.io/2.11.3/concepts/flows/#final-state-determination
everything under here is incremented by a version compared to flows.py
"""

import os
from datetime import datetime
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.states import State, StateType
from prefect_airbyte.flows import (
    run_connection_sync,
    reset_connection,
    reset_connection_streams,
    update_connection_schema,
    clear_connection,
)
from prefect_airbyte import AirbyteConnection, AirbyteServer
from prefect_airbyte.connections import ResetStream
from prefect_dbt.cli.commands import DbtCoreOperation, ShellOperation
from prefect_dbt.cli import DbtCliProfile
from proxy.helpers import CustomLogger

logger = CustomLogger("prefect-proxy")


# django prefect block names
AIRBYTESERVER = "Airbyte Server"
AIRBYTECONNECTION = "Airbyte Connection"
SHELLOPERATION = "Shell Operation"
DBTCORE = "dbt Core Operation"


# =============================================================================
# task config for a airbyte sync operation
# {
#     type AIRBYTECONNECTION,
#     slug: str
#     airbyte_server_block:  str
#     connection_id: str
#     timeout: int
# }
@flow
def run_airbyte_connection_flow_v1(payload: dict):
    """run an airbyte sync"""
    try:
        airbyte_server_block = payload["airbyte_server_block"]
        serverblock = AirbyteServer.load(airbyte_server_block)
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=payload["connection_id"],
            timeout=payload["timeout"] or 15,
        )
        result = run_connection_sync(connection_block)
        logger.info("airbyte connection sync result=")
        logger.info(result)
        return result
    except Exception as error:  # skipcq PYL-W0703
        logger.error(str(error))  # "Job <num> failed."
        raise


# task config for a airbyte reset operation
# {
#     type AIRBYTECONNECTION,
#     slug: "airbyte-reset"
#     airbyte_server_block:  str
#     connection_id: str
#     timeout: int
# }
@flow
def run_airbyte_conn_reset(payload: dict):
    """reset an airbyte connection"""
    try:
        airbyte_server_block = payload["airbyte_server_block"]
        serverblock = AirbyteServer.load(airbyte_server_block)
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=payload["connection_id"],
            timeout=payload["timeout"] or 15,
        )
        result = reset_connection(connection_block)
        logger.info("airbyte connection reset result=")
        logger.info(result)
        return result
    except Exception as error:  # skipcq PYL-W0703
        logger.error(str(error))  # "Job <num> failed."
        raise


# task config for a airbyte reset operation
# {
#     type AIRBYTECONNECTION,
#     slug: "airbyte-clear"
#     airbyte_server_block:  str
#     connection_id: str
#     timeout: int
# }
@flow
def run_airbyte_conn_clear(payload: dict):
    """reset an airbyte connection"""
    try:
        airbyte_server_block = payload["airbyte_server_block"]
        serverblock = AirbyteServer.load(airbyte_server_block)
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=payload["connection_id"],
            timeout=payload["timeout"] or 15,
        )
        result = clear_connection(connection_block)
        logger.info("airbyte connection clear result=")
        logger.info(result)
        return result
    except Exception as error:  # skipcq PYL-W0703
        logger.error(str(error))  # "Job <num> failed."
        raise


# task config for a airbyte reset operation
# {
#     type AIRBYTECONNECTION,
#     slug: "airbyte-reset"
#     airbyte_server_block:  str
#     connection_id: str
#     timeout: int
# }
@flow
def run_airbyte_reset_streams_for_conn(payload: dict, streams: list[ResetStream]):
    """reset an airbyte connection"""
    try:
        airbyte_server_block = payload["airbyte_server_block"]
        serverblock = AirbyteServer.load(airbyte_server_block)
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=payload["connection_id"],
            timeout=payload["timeout"] or 15,
        )
        result = reset_connection_streams(connection_block, streams)
        logger.info("airbyte connection reset result=")
        logger.info(result)
        return result
    except Exception as error:  # skipcq PYL-W0703
        logger.error(str(error))  # "Job <num> failed."
        raise


@flow
def run_dbtcore_flow_v1(payload: dict):
    # pylint: disable=broad-exception-caught
    """Prefect flow to run dbt"""
    return dbtjob_v1(payload, payload["slug"])


@flow
def run_shell_operation_flow(payload: dict):
    # pylint: disable=broad-exception-caught
    """Prefect flow to run shell operation"""
    return shellopjob(payload, payload["slug"])


@flow
def run_refresh_schema_flow(payload: dict, catalog_diff: dict):
    # pylint: disable=broad-exception-caught
    # """Prefect flow to run refresh schema"""
    try:
        airbyte_server_block = payload["airbyte_server_block"]
        serverblock = AirbyteServer.load(airbyte_server_block)
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=payload["connection_id"],
            timeout=payload["timeout"] or 15,
        )
        update_connection_schema(connection_block, catalog_diff=catalog_diff)
        return True
    except Exception as error:  # skipcq PYL-W0703
        logger.error(str(error))  # "Job <num> failed."
        raise


# =============================================================================
# tasks
# task config for a dbt core operation
# {
#     type: DBTCORE,
#     slug: str
#     profiles_dir: str
#     project_dir: str
#     working_dir: str
#     env: dict
#     commands: list
#     cli_profile_block: str
#     cli_args: list = []
#     flow_name: str
#     flow_run_name: str
# }
@task(name="dbtjob_v1", task_run_name="dbtjob-{task_slug}")
def dbtjob_v1(task_config: dict, task_slug: str):  # pylint: disable=unused-argument
    # pylint: disable=broad-exception-caught
    """
    each dbt op will run as a task within the parent flow
    errors are propagated to the flow except those from "dbt test"
    """

    # load the cli block first
    cli_profile_block = DbtCliProfile.load(task_config["cli_profile_block"])

    extras = cli_profile_block.target_configs.extras

    with sshtunnel.SSHTunnelForwarder(
        ("15.206.94.142", 22),
        remote_bind_address=(extras["host"], extras["port"]),
        # ...and credentials
        ssh_pkey="/Users/fatchat/tech4dev/ec2-instances/staging/ddp.pem",
        ssh_username="ddp",
        # ssh_password=conn_info.get("ssh_password"),
        # ssh_private_key_password=conn_info.get("ssh_private_key_password"),
    ) as tunnel:

        extras["host"] = tunnel.local_bind_address[0]
        extras["port"] = tunnel.local_bind_address[1]
        cli_profile_block.target_configs.extras = extras

        dbt_op: DbtCoreOperation = DbtCoreOperation(
            commands=task_config["commands"],
            env=task_config["env"],
            working_dir=task_config["working_dir"],
            profiles_dir=task_config["profiles_dir"],
            project_dir=task_config["project_dir"],
            dbt_cli_profile=cli_profile_block,
        )
        logger.info("running dbtjob with DBT_TEST_FAILED update")

        if os.path.exists(dbt_op.profiles_dir / "profiles.yml"):
            os.unlink(dbt_op.profiles_dir / "profiles.yml")

        try:
            return dbt_op.run()
        except Exception:  # skipcq PYL-W0703
            if task_config["slug"] == "dbt-test":
                return State(
                    type=StateType.COMPLETED,
                    name="DBT_TEST_FAILED",
                    message="WARNING: dbt test failed",
                )

            raise


# =============================================================================
# task config for a shell operation
# {
#     type: SHELLOPERATION,
#     slug: str,
#     commands: [],
#     env: {},
#     workingDir: ""
# }
@task(name="shellopjob", task_run_name="shellop-{task_slug}")
def shellopjob(task_config: dict, task_slug: str):  # pylint: disable=unused-argument
    # pylint: disable=broad-exception-caught
    """loads and runs the shell operation"""

    if task_config["slug"] == "git-pull":  # DDP_backend:constants.TASK_GITPULL
        secret_block_name = task_config["env"]["secret-git-pull-url-block"]
        git_repo_endpoint = ""
        if secret_block_name and len(secret_block_name) > 0:
            secret_blk = Secret.load(secret_block_name)
            git_repo_endpoint = secret_blk.get()

        commands = task_config["commands"]
        updated_cmds = [f"{cmd} {git_repo_endpoint}" for cmd in commands]
        task_config["commands"] = updated_cmds

    elif task_config["slug"] == "generate-edr":  # DDP_backend:constants.TASK_GENERATE_EDR
        # commands = ["edr send-report --bucket-file-path reports/{orgname}.TODAYS_DATE.html --profiles-dir elementary_profiles"]
        # env = {"PATH": /path/to/dbt/venv, "shell": "/bin/bash"}
        secret_block_aws_access_key = "edr-aws-access-key"
        aws_access_key = Secret.load(secret_block_aws_access_key).get()
        secret_block_aws_access_secret = "edr-aws-access-secret"
        aws_access_secret = Secret.load(secret_block_aws_access_secret).get()
        secret_block_s3_bucket = "edr-s3-bucket"
        edr_s3_bucket = Secret.load(secret_block_s3_bucket).get()
        # object key for the report
        todays_date = datetime.today().strftime("%Y-%m-%d")
        task_config["commands"][0] = task_config["commands"][0].replace("TODAYS_DATE", todays_date)
        task_config["commands"][
            0
        ] += f" --aws-access-key-id {aws_access_key} --aws-secret-access-key {aws_access_secret} --s3-bucket-name {edr_s3_bucket}"

    shell_op = ShellOperation(
        commands=task_config["commands"],
        env=task_config["env"],
        working_dir=task_config["working_dir"],
        shell=(task_config["env"]["shell"] if "shell" in task_config["env"] else "/bin/bash"),
    )
    return shell_op.run()


# =============================================================================
# deployment_parmas:
# {
#     config: {
#         tasks: [
#             {
#                 "type": DBTCORE,
#                 "slug": "dbt-run", # coming from django master task table
#                 "seq": 1,
#                 "commands": [],
#                 "env": {},
#                 "working_dir": ",
#                 "profiles_dir": "",
#                 "project_dir": "",
#                 "cli_profile_block": "",
#                 "cli_args": [],
#             }
#         ]
#     }
# }
@flow
def deployment_schedule_flow_v4(config: dict, dbt_blocks: list = [], airbyte_blocks: list = []):
    # pylint: disable=broad-exception-caught
    """modification so dbt test failures are not propagated as flow failures"""
    config["tasks"].sort(key=lambda blk: blk["seq"])

    try:
        for task_config in config["tasks"]:
            if task_config["type"] == DBTCORE:
                dbtjob_v1(task_config, task_config["slug"])

            elif task_config["type"] == SHELLOPERATION:
                shellopjob(task_config, task_config["slug"])

            elif task_config["type"] == AIRBYTECONNECTION:
                if task_config["slug"] == "airbyte-reset":
                    if task_config.get("streams") and len(task_config["streams"]) > 0:
                        # run reset for streams
                        streams = [ResetStream(**stream) for stream in task_config["streams"]]
                        run_airbyte_reset_streams_for_conn(task_config, streams)
                    else:
                        # run full reset of all streams
                        run_airbyte_conn_reset(task_config)
                elif task_config["slug"] == "airbyte-sync":
                    run_airbyte_connection_flow_v1(task_config)

                elif task_config["slug"] == "airbyte-clear":
                    run_airbyte_conn_clear(task_config)

                elif task_config["slug"] == "update-schema":
                    run_refresh_schema_flow(
                        task_config, catalog_diff=task_config.get("catalog_diff", {})
                    )
            else:
                raise Exception(f"Unknown task type: {task_config['type']}")

    except Exception as error:  # skipcq PYL-W0703
        logger.exception(error)
        raise
