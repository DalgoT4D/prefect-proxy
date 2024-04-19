import os
import pytest
from pydantic import ValidationError
from proxy.main import delete_deployment
from proxy.schemas import (
    AirbyteConnectionBlockResponse,
    AirbyteConnectionCreate,
    AirbyteServerBlockResponse,
    AirbyteServerCreate,
    DbtCoreBlockResponse,
    DbtCoreCreate,
    DeploymentCreate,
    FlowRunsResponse,
    PostDeploymentResponse,
)
from proxy.service import (
    create_airbyte_server_block,
    create_dbt_core_block,
    delete_airbyte_connection_block,
    delete_airbyte_server_block,
    delete_dbt_core_block,
    get_airbyte_server_block_id,
    get_flow_runs_by_deployment_id,
)


class TestAirbyteServer:
    block_id = None

    @pytest.mark.asyncio
    async def test_create_airbyte_server_block(self):
        payload = {
            "serverHost": "localhost",
            "serverPort": 8000,
            "apiVersion": "v1",
            "blockName": "airbyte1",
        }
        try:
            validated_payload = AirbyteServerCreate(**payload)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

        try:
            res = await create_airbyte_server_block(validated_payload)
            AirbyteServerBlockResponse(block_id=res)
            TestAirbyteServer.block_id = res
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    @pytest.mark.asyncio
    async def test_get_airbyte_server_block_id(self):
        try:
            res = await get_airbyte_server_block_id(blockname="airbyte1")
            AirbyteServerBlockResponse(block_id=res)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")


class TestAirbyteConnection:
    block_id = None


    def test_delete_airbyte_connection_block(self):
        try:
            delete_airbyte_server_block(blockid=TestAirbyteServer.block_id)
            delete_airbyte_connection_block(blockid=TestAirbyteConnection.block_id)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")


class TestDbtConnection:
    @pytest.mark.asyncio
    async def test_create_dbt_core_block(self):
        payload = {
            "blockName": "test",
            "profile": {
                "name": "shri_dbt",
                "target": "dev",
                "target_configs_schema": "dev",
            },
            "wtype": "postgres",
            "credentials": {
                "host": os.getenv("DB_HOST"),
                "port": 5432,
                "database": os.getenv("DB_NAME"),
                "username": os.getenv("DB_USER"),
                "ssl": False,
                "password": os.getenv("DB_PASSWORD"),
            },
            "commands": ["dbt list"],
            "working_dir": "/tmp",
            "env": {"key": "value"},
            "profiles_dir": "/dir",
            "project_dir": "/dir",
        }
        try:
            validated_payload = DbtCoreCreate(**payload)
        except ValidationError as e:
            raise ValueError(f"Payload validation failed: {e.errors()}")

        try:
            res = await create_dbt_core_block(validated_payload)
            DbtCoreBlockResponse(block_id=res)
            TestDbtConnection.block_id = res
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")

    def test_delete_dbt_core_block(self):
        try:
            delete_dbt_core_block(block_id=TestDbtConnection.block_id)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")


class TestFlowDeployment:

    def test_get_flow_runs_by_deployment_id(self):
        deployment_id = TestFlowDeployment.deployment_id
        limit = 10

        try:
            res = get_flow_runs_by_deployment_id(deployment_id, limit, "")
            FlowRunsResponse(flow_runs=res)
        except Exception as e:
            raise ValueError(f"Test failed: {e}")

    def test_delete_deployment(self):
        deployment_id = TestFlowDeployment.deployment_id
        try:
            delete_deployment(deployment_id)
        except Exception as e:
            raise ValueError(f"Test failed: {e}")
