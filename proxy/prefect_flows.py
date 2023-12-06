"""
Reusable flows
https://docs.prefect.io/2.11.3/concepts/flows/#final-state-determination
everything under here is incremented by a version compared to flows.py
"""

import os
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.states import State, StateType
from prefect_airbyte.flows import run_connection_sync
from prefect_airbyte import AirbyteConnection, AirbyteServer
from prefect_dbt.cli.commands import DbtCoreOperation, ShellOperation
from proxy.helpers import CustomLogger, command_from_dbt_blockname
from prefect_dbt.cli import DbtCliProfile

logger = CustomLogger("prefect-proxy")


# django prefect block names
AIRBYTESERVER = "Airbyte Server"
AIRBYTECONNECTION = "Airbyte Connection"
SHELLOPERATION = "Shell Operation"
DBTCORE = "dbt Core Operation"


""" task config for a airbyte sync operation
{
    type AIRBYTECONNECTION,
    slug: str
    airbyte_server_block:  str
    connection_id: str
    timeout: int
}
"""


@flow
def run_airbyte_connection_flow_v1(config: dict):
    # pylint: disable=broad-exception-caught
    """Prefect flow to run airbyte connection"""
    try:
        airbyte_server_block = config["airbyte_server_block"]
        serverblock = AirbyteServer.load(airbyte_server_block)
        connection_block = AirbyteConnection(
            airbyte_server=serverblock,
            connection_id=config["connection_id"],
            timeout=config["timeout"] or 15,
        )
        result = run_connection_sync(connection_block)
        logger.info("airbyte connection sync result=")
        logger.info(result)
        return result
    except Exception as error:  # skipcq PYL-W0703
        logger.error(str(error))  # "Job <num> failed."
        raise


@flow
def run_dbtcore_flow_v1(payload: dict):
    # pylint: disable=broad-exception-caught
    """Prefect flow to run dbt"""
    return dbtjob_v1(payload)


@flow
def run_shell_operation_flow(payload: dict):
    # pylint: disable=broad-exception-caught
    """Prefect flow to run shell operation"""
    return shellopjob(payload)


# =============================================================================

""" task config for a dbt core operation
{
    type: DBTCORE,
    slug: str
    profiles_dir: str
    project_dir: str
    working_dir: str
    env: dict
    commands: list
    cli_profile_block: str
    cli_args: list = []
    flow_name: str
    flow_run_name: str
}
"""


@task(name="dbtjob_v1")
def dbtjob_v1(task_config: dict):
    # pylint: disable=broad-exception-caught
    """
    each dbt op will run as a task within the parent flow
    errors are propagated to the flow except those from "dbt test"
    """

    # load the cli block first
    cli_profile_block = DbtCliProfile.load(task_config["cli_profile_block"])

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
                message=f"WARNING: dbt test failed",
            )

        raise


""" task config for a shell operation
{
    type: SHELLOPERATION,
    commands: [],
    env: {},
    workingDir: ""
}
"""


@task(name="shellopjob", task_run_name="shellop")
def shellopjob(task_config: dict):
    # pylint: disable=broad-exception-caught
    """loads and runs the shell operation"""

    if task_config["slug"] == "git-pull":
        secret_block_name = task_config["env"]["secret-git-pull-url-block"]
        git_repo_endpoint = ""
        if secret_block_name and len(secret_block_name) > 0:
            secret_blk = Secret.load(secret_block_name)
            git_repo_endpoint = secret_blk.get()

        commands = task_config["commands"]
        updated_cmds = []
        for cmd in commands:
            updated_cmds.append(f"{cmd} {git_repo_endpoint}")
        task_config["commands"] = updated_cmds

    shell_op = ShellOperation(
        commands=task_config["commands"],
        env=task_config["env"],
        working_dir=task_config["working_dir"],
    )
    return shell_op.run()


# =============================================================================

"""
deployment_parmas:
{
    config: {
        tasks: [
            {
                "type": DBTCORE,
                "slug": "dbt-run", # coming from django master task table
                "seq": 1,
                "commands": [],
                "env": {},
                "working_dir": ",
                "profiles_dir": "",
                "project_dir": "",
                "cli_profile_block": "",
                "cli_args": [],
            }
        ]
    }
}
"""


@flow
def deployment_schedule_flow_v4(config: dict):
    # pylint: disable=broad-exception-caught
    """modification so dbt test failures are not propagated as flow failures"""
    config["tasks"].sort(key=lambda blk: blk["seq"])

    try:
        for task_config in config["tasks"]:
            if task_config["type"] == DBTCORE:
                dbtjob_v1(task_config)
            elif task_config["type"] == SHELLOPERATION:
                shellopjob(task_config)
            elif task_config["type"] == AIRBYTECONNECTION:
                run_airbyte_connection_flow_v1(task_config)
            else:
                raise Exception(f"Unknown task type: {task_config['type']}")

    except Exception as error:  # skipcq PYL-W0703
        logger.exception(error)
        raise
