import os
import pytest
from pydantic import ValidationError
from proxy.main import delete_deployment
from proxy.schemas import (
    AirbyteConnectionBlockResponse,
    AirbyteConnectionCreate,
    AirbyteServerBlockResponse,
    AirbyteServerCreate,
    FlowRunsResponse,
    PostDeploymentResponse,
)
from proxy.service import (
    create_airbyte_server_block,
    delete_airbyte_connection_block,
    delete_airbyte_server_block,
    get_airbyte_server_block_id,
    get_flow_runs_by_deployment_id,
)


@pytest.mark.skip(reason="Integration test")
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


@pytest.mark.skip(reason="Integration test")
class TestAirbyteConnection:
    block_id = None

    def test_delete_airbyte_connection_block(self):
        try:
            delete_airbyte_server_block(blockid=TestAirbyteServer.block_id)
            delete_airbyte_connection_block(blockid=TestAirbyteConnection.block_id)
        except ValidationError as e:
            raise ValueError(f"Response validation failed: {e.errors()}")


@pytest.mark.skip(reason="Integration test")
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
