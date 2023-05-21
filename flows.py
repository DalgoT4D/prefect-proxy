"""Reusable flows"""

import os
from prefect import flow
from prefect_airbyte.flows import run_connection_sync
from prefect_airbyte import AirbyteConnection
from prefect_dbt.cli.commands import DbtCoreOperation
from schemas import RunFlow


@flow
def run_airbyte_connection_flow(payload: RunFlow):
    """Prefect flow to run airbyte connection"""
    airbyte_connection = AirbyteConnection.load(payload.blockName)
    return run_connection_sync(airbyte_connection)


@flow
def run_dbtcore_flow(payload: RunFlow):
    """Prefect flow to run dbt"""
    dbt_op = DbtCoreOperation.load(payload.blockName)
    if os.path.exists(dbt_op.profiles_dir / "profiles.yml"):
        os.unlink(dbt_op.profiles_dir / "profiles.yml")
    return dbt_op.run()


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
        run_connection_sync(airbyte_connection)

    # run dbt blocks
    for block in dbt_blocks:
        dbt_op = DbtCoreOperation.load(block["blockName"])
        dbt_op.run()
