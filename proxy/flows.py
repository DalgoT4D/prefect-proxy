"""Reusable flows"""

import os
from prefect import flow
from prefect_airbyte.flows import run_connection_sync
from prefect_airbyte import AirbyteConnection
from prefect_dbt.cli.commands import DbtCoreOperation, ShellOperation
from prefect.blocks.system import Secret
from logger import logger


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
        airbyte_connection = AirbyteConnection.load(block_name)
        result = run_connection_sync(airbyte_connection)  # has three tasks
        logger.info("airbyte connection sync result=")
        logger.info(result)
        return result
    except Exception as error:  # pylint: disable=broad-exception-caught
        # logger.exception(error)
        logger.error(str(error))  # "Job <num> failed."
        # raise HTTPException(status_code=400, detail=str(error)) from error


@flow
def run_dbtcore_flow(block_name: str):
    # pylint: disable=broad-exception-caught
    """Prefect flow to run dbt"""
    try:
        dbt_op = DbtCoreOperation.load(block_name)
        if os.path.exists(dbt_op.profiles_dir / "profiles.yml"):
            os.unlink(dbt_op.profiles_dir / "profiles.yml")
        return dbt_op.run()
    except Exception as error:
        logger.exception(error)
        # raise HTTPException(status_code=400, detail=str(error)) from error


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
        except Exception as error:
            logger.exception(error)
            # raise HTTPException(status_code=400, detail=str(error)) from error

    # run dbt blocks
    for block in dbt_blocks:
        if block["blockType"] == SHELLOPERATION:
            shell_op = ShellOperation.load(block["blockName"])

            # fetch the secret block having the git oauth token-based url to pull code from private repos
            # the key "secret-git-pull-url-block" will always be present. Value will be empty if no token was submitted by user
            secret_block_name = shell_op.env["secret-git-pull-url-block"]
            secret_blk = Secret.load(secret_block_name)
            url = secret_blk.get()

            # update the commands to account for the token
            commands = shell_op.commands
            updated_cmds = []
            for cmd in commands:
                updated_cmds.append(f"{cmd} {url}")
            shell_op.commands = updated_cmds

            # run the shell command(s)
            shell_op.run()

            continue

        if block["blockType"] == DBTCORE:
            dbt_op = DbtCoreOperation.load(block["blockName"])
            if os.path.exists(dbt_op.profiles_dir / "profiles.yml"):
                os.unlink(dbt_op.profiles_dir / "profiles.yml")
            try:
                dbt_op.run()
            except Exception as error:
                logger.exception(error)
                # raise HTTPException(status_code=400, detail=str(error)) from error
