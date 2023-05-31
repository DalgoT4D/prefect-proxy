"""Reusable flows"""

import os
from fastapi import HTTPException
from prefect import flow
from prefect_airbyte.flows import run_connection_sync
from prefect_airbyte import AirbyteConnection
from prefect_dbt.cli.commands import DbtCoreOperation
from logger import logger


@flow
def run_airbyte_connection_flow(block_name: str):
    """Prefect flow to run airbyte connection"""
    try:
        airbyte_connection = AirbyteConnection.load(block_name)
        return run_connection_sync(airbyte_connection)
    except Exception as error:
        # logger.exception(error)
        logger.error("FAILED, see next line")
        logger.error(str(error))
        raise HTTPException(status_code=400, detail=str(error)) from error


@flow
def run_dbtcore_flow(block_name: str):
    """Prefect flow to run dbt"""
    try:
        dbt_op = DbtCoreOperation.load(block_name)
        if os.path.exists(dbt_op.profiles_dir / "profiles.yml"):
            os.unlink(dbt_op.profiles_dir / "profiles.yml")
        return dbt_op.run()
    except Exception as error:
        # logger.exception(error)
        logger.error("FAILED")
        raise HTTPException(status_code=400, detail=str(error)) from error


@flow
def deployment_schedule_flow(airbyte_blocks: list, dbt_blocks: list):
    """A general flow function that will help us create deployments"""

    # sort the dbt blocks by seq
    dbt_blocks.sort(key=lambda blk: blk["seq"])

    # sort the airbyte blocks by seq
    airbyte_blocks.sort(key=lambda blk: blk["seq"])

    # run airbyte blocks
    for block in airbyte_blocks:
        airbyte_connection = AirbyteConnection.load(block["blockName"])
        try:
            return run_connection_sync(airbyte_connection)
        except Exception as error:
            logger.exception(error)
            raise HTTPException(status_code=400, detail=str(error)) from error

    # run dbt blocks
    for block in dbt_blocks:
        dbt_op = DbtCoreOperation.load(block["blockName"])
        try:
            return dbt_op.run()
        except Exception as error:
            logger.exception(error)
            raise HTTPException(status_code=400, detail=str(error)) from error
