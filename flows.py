"""Reusable flows"""

from fastapi import HTTPException
from prefect import flow
from prefect_airbyte.flows import run_connection_sync
from prefect_airbyte import AirbyteConnection
from prefect_dbt.cli.commands import DbtCoreOperation
from schemas import RunFlow
from logger import logger


@flow
def run_airbyte_connection_flow(payload: RunFlow):
    """Prefect flow to run airbyte connection"""
    airbyte_connection = AirbyteConnection.load(payload.blockName)
    try:
        return run_connection_sync(airbyte_connection)
    except Exception as error:
        logger.exception(error)
        raise HTTPException(status_code=400, detail=str(error)) from error


@flow
def run_dbtcore_flow(payload: RunFlow):
    """Prefect flow to run dbt"""
    dbt_op = DbtCoreOperation.load(payload.blockName)
    try:
        return dbt_op.run()
    except Exception as error:
        logger.exception(error)
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
