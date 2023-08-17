"""
Reusable flows
https://docs.prefect.io/2.11.3/concepts/flows/#final-state-determination
"""

import os
from prefect import flow, task
from prefect.blocks.system import Secret
from prefect.states import State
from prefect_airbyte.flows import run_connection_sync
from prefect_airbyte import AirbyteConnection
from prefect_dbt.cli.commands import DbtCoreOperation, ShellOperation
from proxy.helpers import CustomLogger

logger = CustomLogger("prefect-proxy")


# django prefect block names
AIRBYTESERVER = "Airbyte Server"
AIRBYTECONNECTION = "Airbyte Connection"
SHELLOPERATION = "Shell Operation"
DBTCORE = "dbt Core Operation"


@flow
def run_airbyte_connection_flow(block_name: str):
    # pylint: disable=broad-exception-caught
    """Prefect flow to run airbyte connection"""
    try:
        airbyte_connection: AirbyteConnection = AirbyteConnection.load(block_name)
        result = run_connection_sync(airbyte_connection)
        logger.info("airbyte connection sync result=")
        logger.info(result)
        return result
    except Exception as error:  # skipcq PYL-W0703
        logger.error(str(error))  # "Job <num> failed."
        raise


@flow
def run_dbtcore_flow(block_name: str):
    # pylint: disable=broad-exception-caught
    """Prefect flow to run dbt"""
    try:
        dbt_op: DbtCoreOperation = DbtCoreOperation.load(block_name)
        if os.path.exists(dbt_op.profiles_dir / "profiles.yml"):
            os.unlink(dbt_op.profiles_dir / "profiles.yml")
        return dbt_op.run()
    except Exception as error:  # skipcq PYL-W0703
        logger.exception(error)
        raise


# =============================================================================
# ==== deprecated soon after 2023-08-06
@flow
def deployment_schedule_flow(airbyte_blocks: list, dbt_blocks: list):
    # pylint: disable=broad-exception-caught
    """A general flow function that will help us create deployments"""
    # sort the dbt blocks by seq
    dbt_blocks.sort(key=lambda blk: blk["seq"])

    # sort the airbyte blocks by seq
    airbyte_blocks.sort(key=lambda blk: blk["seq"])

    # run airbyte blocks
    for block in airbyte_blocks:
        airbyte_connection = AirbyteConnection.load(block["blockName"])
        try:
            run_connection_sync(airbyte_connection)
        except Exception as error:  # skipcq PYL-W0703
            logger.exception(error)

    # run dbt blocks
    for block in dbt_blocks:
        if block["blockType"] == SHELLOPERATION:
            shell_op = ShellOperation.load(block["blockName"])

            try:
                # fetch the secret block having the git oauth token-based url to pull code from
                # private repos
                # the key "secret-git-pull-url-block" will always be present. Value will be empty
                # string if no token was submitted by user
                secret_block_name = shell_op.env["secret-git-pull-url-block"]
                git_repo_endpoint = ""
                if secret_block_name and len(secret_block_name) > 0:
                    secret_blk = Secret.load(secret_block_name)
                    git_repo_endpoint = secret_blk.get()

                # update the commands to account for the token
                commands = shell_op.commands
                updated_cmds = []
                for cmd in commands:
                    updated_cmds.append(f"{cmd} {git_repo_endpoint}")
                shell_op.commands = updated_cmds

                # run the shell command(s)
                shell_op.run()
            except Exception as error:  # skipcq PYL-W0703
                logger.exception(error)

        elif block["blockType"] == DBTCORE:
            dbt_op = DbtCoreOperation.load(block["blockName"])
            if os.path.exists(dbt_op.profiles_dir / "profiles.yml"):
                os.unlink(dbt_op.profiles_dir / "profiles.yml")
            try:
                dbt_op.run()
            except Exception as error:  # skipcq PYL-W0703
                logger.exception(error)


# =============================================================================
@task(name="gitpulljob")
def gitpulljob(shell_op_name: str):
    # pylint: disable=broad-exception-caught
    """loads and runs the git-pull shell operation"""
    shell_op: ShellOperation = ShellOperation.load(shell_op_name)

    # fetch the secret block having the git oauth token-based url to pull code
    #  from private repos
    # the key "secret-git-pull-url-block" will always be present. Value will be
    #  empty string if no token was submitted by user
    secret_block_name = shell_op.env["secret-git-pull-url-block"]
    git_repo_endpoint = ""
    if secret_block_name and len(secret_block_name) > 0:
        secret_blk = Secret.load(secret_block_name)
        git_repo_endpoint = secret_blk.get()

    # update the commands to account for the token
    commands = shell_op.commands
    updated_cmds = []
    for cmd in commands:
        updated_cmds.append(f"{cmd} {git_repo_endpoint}")
    shell_op.commands = updated_cmds

    # run the shell command(s)
    return shell_op.run()


@task(name="dbtjob")
def dbtjob(dbt_op_name: str):
    # pylint: disable=broad-exception-caught
    """
    each dbt op will run as a task within the parent flow
    errors are propagated to the flow except those from "dbt test"
    """
    dbt_op: DbtCoreOperation = DbtCoreOperation.load(dbt_op_name)

    if os.path.exists(dbt_op.profiles_dir / "profiles.yml"):
        os.unlink(dbt_op.profiles_dir / "profiles.yml")

    try:
        return dbt_op.run()
    except Exception:  # skipcq PYL-W0703
        if dbt_op_name.endswith("-test"):
            return State(type="COMPLETED", message=f"WARNING: {dbt_op_name} failed")

        raise


@flow
def deployment_schedule_flow_v2(airbyte_blocks: list, dbt_blocks: list):
    # pylint: disable=broad-exception-caught
    """modification so dbt test failures are not propagated as flow failures"""
    # sort the airbyte blocks by seq
    airbyte_blocks.sort(key=lambda blk: blk["seq"])

    # sort the dbt blocks by seq
    dbt_blocks.sort(key=lambda blk: blk["seq"])

    try:
        # run airbyte blocks, fail if sync fails
        for block in airbyte_blocks:
            airbyte_connection = AirbyteConnection.load(block["blockName"])
            run_connection_sync(airbyte_connection)

        # run dbt blocks, fail on block failure unless the failing block is a dbt-test
        for block in dbt_blocks:
            if block["blockType"] == SHELLOPERATION:
                gitpulljob(block["blockName"])

            elif block["blockType"] == DBTCORE:
                dbtjob(block["blockName"])

    except Exception as error:  # skipcq PYL-W0703
        logger.exception(error)
        raise
