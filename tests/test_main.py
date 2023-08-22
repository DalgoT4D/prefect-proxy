from unittest.mock import Mock, AsyncMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from proxy.main import (
    airbytesync,
    app,
    dbtrun,
    delete_block,
    delete_deployment,
    get_airbyte_connection_by_blockid,
    get_airbyte_connection_by_blockname,
    get_airbyte_server,
    get_dbtcore,
    get_flow_run_logs_paginated,
    get_flow_runs,
    get_flowrun,
    get_read_deployment,
    get_shell,
    post_airbyte_connection,
    post_airbyte_connection_blocks,
    post_airbyte_server,
    post_bulk_delete_blocks,
    post_create_deployment_flow_run,
    post_dataflow,
    post_dbtcore,
    put_dbtcore_postgres,
    put_dbtcore_bigquery,
    put_dbtcore_schema,
    put_dataflow,
    get_flow_run_by_id,
    post_secret_block,
    post_deployment_set_schedule,
    post_deployments,
    post_shell,
    sync_airbyte_connection_flow,
    sync_dbtcore_flow,
)

from proxy.schemas import (
    AirbyteConnectionBlocksFetch,
    AirbyteConnectionCreate,
    AirbyteServerCreate,
    DbtCoreCreate,
    DbtCoreCredentialUpdate,
    DbtProfileCreate,
    DbtCoreSchemaUpdate,
    PrefectSecretBlockCreate,
    DeploymentCreate,
    DeploymentUpdate,
    DeploymentFetch,
    FlowRunRequest,
    PrefectBlocksDelete,
    PrefectShellSetup,
    RunFlow,
)

app = FastAPI()
client = TestClient(app)


def test_airbytesync_success():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    inner_result = {"status": "success", "result": "example_result"}
    expected_result = {"status": "success", "result": inner_result}

    with patch(
        "proxy.main.run_airbyte_connection_flow"
    ) as mock_run_airbyte_connection_flow:
        with patch.object(
            mock_run_airbyte_connection_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: inner_result
            result = airbytesync(block_name, flow_name, flow_run_name)
            assert result == expected_result


def test_airbytesync_failure():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    inner_result = {"status": "failed", "result": "example_failure_result"}
    expected_result = {"status": "success", "result": inner_result}

    with patch(
        "proxy.main.run_airbyte_connection_flow"
    ) as mock_run_airbyte_connection_flow:
        with patch.object(
            mock_run_airbyte_connection_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: inner_result
            result = airbytesync(block_name, flow_name, flow_run_name)
            assert result == expected_result


def test_airbyte_sync_with_invalid_block_name():
    invalid_block_name = None
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"

    with pytest.raises(Exception):
        airbytesync(invalid_block_name, flow_name, flow_run_name)


def test_airbyte_sync_invalid_flow_name():
    block_name = "example_block"
    invalid_flow_name = None
    flow_run_name = "example_flow_run"

    with pytest.raises(Exception):
        airbytesync(block_name, invalid_flow_name, flow_run_name)


def test_airbyte_sync_invalid_flow_run_name():
    block_name = "example_block"
    flow_name = "example_flow"
    invalid_flow_run_name = None

    with pytest.raises(Exception):
        airbytesync(block_name, flow_name, invalid_flow_run_name)


def test_dbtrun_success():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    expected_result = {"result": "example_result"}

    with patch("proxy.main.run_dbtcore_flow") as mock_run_dbtcore_flow:
        with patch.object(
            mock_run_dbtcore_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: expected_result
            result = dbtrun(block_name, flow_name, flow_run_name)
            assert result == expected_result


def test_dbtrun_failure():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    expected_result = {"result": "example_failure_result", "status": "failed"}

    with patch("proxy.main.run_dbtcore_flow") as mock_run_dbtcore_flow:
        with patch.object(
            mock_run_dbtcore_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: expected_result
            result = dbtrun(block_name, flow_name, flow_run_name)
            assert result == expected_result


def test_run_dbt_exception():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    expected_result = {"result": "example_failure_result", "status": "failed"}

    def mock_with_options(*args, **kwargs):
        raise HTTPException(status_code=400, detail="Job 12345 failed.")

    with patch("proxy.main.run_dbtcore_flow.with_options", new=mock_with_options):
        try:
            result = dbtrun(block_name, flow_name, flow_run_name)
            assert result == expected_result
        except HTTPException as e:
            assert e.status_code == 400
            assert e.detail == "Job 12345 failed."


def test_dbtrun_invalid_block_name():
    block_name = None
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"

    with pytest.raises(TypeError):
        dbtrun(block_name, flow_name, flow_run_name)


def test_dbtrun_invalid_flow_name():
    block_name = "example_block"
    flow_name = None
    flow_run_name = "example_flow_run"

    with pytest.raises(TypeError):
        dbtrun(block_name, flow_name, flow_run_name)


def test_dbtrun_invalid_flow_run_name():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = None

    with pytest.raises(TypeError):
        dbtrun(block_name, flow_name, flow_run_name)


@pytest.mark.asyncio
async def test_post_bulk_delete_blocks_success():
    payload = PrefectBlocksDelete(block_ids=["12345", "67890"])
    request = client.request("POST", "/")
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.return_value = None
        response = await post_bulk_delete_blocks(request, payload)
        assert response == {"deleted_blockids": ["12345", "67890"]}


def test_post_airbyte_connection_blocks_success():
    payload = AirbyteConnectionBlocksFetch(block_names=["test_block"])
    request = client.request("POST", "/")
    with patch(
        "proxy.main.post_filter_blocks",
        return_value=[
            {"name": "test_block", "data": {"connection_id": "12345"}, "id": "67890"}
        ],
    ):
        response = post_airbyte_connection_blocks(request, payload)
        assert response == [
            {"name": "test_block", "connectionId": "12345", "id": "67890"}
        ]


@pytest.mark.asyncio
async def test_get_airbyte_server_success():
    request = client.request("POST", "/")
    with patch("proxy.main.get_airbyte_server_block_id", return_value="12345"):
        response = await get_airbyte_server(request, "test_block")
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_get_airbyte_server_failure():
    request = client.request("GET", "/")
    with patch("proxy.main.get_airbyte_server_block_id") as mock_get_block_id:
        mock_get_block_id.side_effect = Exception("Test error")
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_server(request, "test_block")
        assert excinfo.value.status_code == 500
        assert excinfo.value.detail == "Internal server error"


@pytest.mark.asyncio
async def test_get_airbyte_server_invalid_block_name():
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await get_airbyte_server(request, None)
    assert excinfo.value.args[0] == "blockname must be a string"


@pytest.mark.asyncio
async def test_post_airbyte_server_success():
    payload = AirbyteServerCreate(
        blockName="testserver",
        serverHost="http://test-server.com",
        serverPort=8000,
        apiVersion="v1",
    )
    request = client.request("POST", "/")
    with patch("proxy.main.create_airbyte_server_block", return_value=("12345", "testserver")):
        response = await post_airbyte_server(request, payload)
        assert response == {"block_id": "12345", "cleaned_block_name": "testserver"}


@pytest.mark.asyncio
async def test_post_airbyte_server_failure():
    payload = AirbyteServerCreate(
        blockName="testserver",
        serverHost="http://test-server.com",
        serverPort=8000,
        apiVersion="v1",
    )
    request = client.request("POST", "/")
    with patch(
        "proxy.main.create_airbyte_server_block", side_effect=Exception("test error")
    ):
        with pytest.raises(HTTPException) as excinfo:
            await post_airbyte_server(request, payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create airbyte server block"


@pytest.mark.asyncio
async def test_post_airbyte_server_with_invalid_payload():
    payload = None
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await post_airbyte_server(request, payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockname_success():
    request = client.request("POST", "/")
    with patch("proxy.main.get_airbyte_connection_block_id", return_value="12345"):
        response = await get_airbyte_connection_by_blockname(request, "test_block")
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockname_failure():
    request = client.request("POST", "/")
    with patch("proxy.main.get_airbyte_connection_block_id", return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_connection_by_blockname(request, "test_block")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no block having name test_block"


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockname_invalid_blockname():
    request = client.request("POST", "/")
    with patch(
        "proxy.main.get_airbyte_connection_block_id"
    ) as get_airbyte_connection_block_id_mock:
        get_airbyte_connection_block_id_mock.return_value = None
        blockname = "invalid_blockname"
        with pytest.raises(HTTPException) as exc_info:
            await get_airbyte_connection_by_blockname(request, blockname)
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "no block having name " + blockname


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockid_success():
    block_data = {"id": "12345", "name": "test_block", "url": "http://test-block.com"}
    request = client.request("POST", "/")
    with patch("proxy.main.get_airbyte_connection_block", return_value=block_data):
        response = await get_airbyte_connection_by_blockid(request, "12345")
        assert response == block_data


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockid_failure():
    request = client.request("POST", "/")
    with patch("proxy.main.get_airbyte_connection_block", return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_connection_by_blockid(request, "12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no block having id 12345"


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockid_invalid_blockid():
    request = client.request("POST", "/")
    with patch(
        "proxy.main.get_airbyte_connection_block"
    ) as get_airbyte_connection_block_mock:
        get_airbyte_connection_block_mock.return_value = None
        blockid = "invalid_blockid"
        with pytest.raises(HTTPException) as exc_info:
            await get_airbyte_connection_by_blockid(request, blockid)
        assert exc_info.value.status_code == 400
        assert exc_info.value.detail == "no block having id " + blockid


@pytest.mark.asyncio
async def test_post_airbyte_connection_success():
    payload = AirbyteConnectionCreate(
        serverBlockName="testserver",
        connectionId="12345",
        connectionBlockName="test_connection",
    )
    request = client.request("POST", "/")
    with patch("proxy.main.create_airbyte_connection_block", return_value="12345"):
        response = await post_airbyte_connection(request, payload)
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_post_airbyte_connection_failure():
    request = client.request("POST", "/")
    payload = AirbyteConnectionCreate(
        serverBlockName="testserver",
        connectionId="12345",
        connectionBlockName="test_connection",
    )
    with patch(
        "proxy.main.create_airbyte_connection_block",
        side_effect=Exception("test error"),
    ):
        with pytest.raises(HTTPException) as excinfo:
            await post_airbyte_connection(request, payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create airbyte connection block"


@pytest.mark.asyncio
async def test_post_airbyte_connection_with_invalid_payload():
    request = client.request("POST", "/")
    payload = AirbyteConnectionCreate(
        serverBlockName="testserver",
        connectionId="12345",
        connectionBlockName="test_connection",
    )
    with pytest.raises(HTTPException) as excinfo:
        await post_airbyte_connection(request, payload)
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "failed to create airbyte connection block"


@pytest.mark.asyncio
async def test_get_shell_success():
    request = client.request("POST", "/")
    with patch("proxy.main.get_shell_block_id", return_value="12345"):
        response = await get_shell(request, "test_block")
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_get_shell_failure():
    request = client.request("POST", "/")
    with patch("proxy.main.get_shell_block_id", return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_shell(request, "test_block")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no block having name test_block"


@pytest.mark.asyncio
async def test_get_shell_invalid_blockname():
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await get_shell(request, None)
    assert excinfo.value.args[0] == "blockname must be a string"


@pytest.mark.asyncio
async def test_post_shell_success():
    request = client.request("POST", "/")
    payload = PrefectShellSetup(
        blockName="test_shell",
        commands=['echo "Hello, World!"'],
        workingDir="test_dir",
        env={"TEST_ENV": "test_value"},
    )
    with patch("proxy.main.create_shell_block", return_value=("12345", "test_shell")):
        response = await post_shell(request, payload)
        assert response == {"block_id": "12345", "block_name": "test_shell"}


@pytest.mark.asyncio
async def test_post_shell_failure():
    payload = PrefectShellSetup(
        blockName="test_shell",
        commands=['echo "Hello, World!"'],
        workingDir="test_dir",
        env={"TEST_ENV": "test_value"},
    )
    request = client.request("POST", "/")
    with patch("proxy.main.create_shell_block", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            await post_shell(request, payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create shell block"


@pytest.mark.asyncio
async def test_post_shell_invalid_payload():
    payload = None
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await post_shell(request, payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_get_dbtcore_success():
    request = client.request("POST", "/")
    with patch("proxy.main.get_dbtcore_block_id", return_value="12345"):
        response = await get_dbtcore(request, "test_block")
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_get_dbtcore_failure():
    request = client.request("POST", "/")
    with patch("proxy.main.get_dbtcore_block_id", return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_dbtcore(request, "test_block")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no block having name test_block"


@pytest.mark.asyncio
async def test_get_dbtcore_invalid_blockname():
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await get_dbtcore(request, None)
    assert excinfo.value.args[0] == "blockname must be a string"


@pytest.mark.asyncio
async def test_post_dbtcore_success():
    request = client.request("POST", "/")
    payload = DbtCoreCreate(
        blockName="test_dbt",
        profile=DbtProfileCreate(
            name="test_profile",
            target="test_target",
            target_configs_schema="test_schema",
        ),
        wtype="postgres",
        credentials={"TEST_CREDS": "test_creds"},
        commands=['echo "Hello, World!"'],
        env={"TEST_ENV": "test_value"},
        working_dir="test_dir",
        profiles_dir="test_profiles_dir",
        project_dir="test_project_dir",
    )
    with patch(
        "proxy.main.create_dbt_core_block", return_value=("12345", "test_dbt_cleaned")
    ):
        response = await post_dbtcore(request, payload)
        assert response == {"block_id": "12345", "block_name": "test_dbt_cleaned"}


@pytest.mark.asyncio
async def test_post_dbtcore_failure():
    request = client.request("POST", "/")
    payload = DbtCoreCreate(
        blockName="test_dbt",
        profile=DbtProfileCreate(
            name="test_profile",
            target="test_target",
            target_configs_schema="test_schema",
        ),
        wtype="postgres",
        credentials={"TEST_CREDS": "test_creds"},
        commands=['echo "Hello, World!"'],
        env={"TEST_ENV": "test_value"},
        working_dir="test_dir",
        profiles_dir="test_profiles_dir",
        project_dir="test_project_dir",
    )
    with patch("proxy.main.create_dbt_core_block", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            await post_dbtcore(request, payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create dbt core block"


@pytest.mark.asyncio
async def test_post_dbtcore_invalid_payload():
    payload = None
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await post_dbtcore(request, payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_put_dbtcore_postgres_badparams():
    request = Mock()
    with pytest.raises(TypeError) as excinfo:
        await put_dbtcore_postgres(request, 1)
    assert str(excinfo.value) == "payload is invalid"


@pytest.mark.asyncio
@patch("proxy.main.update_postgres_credentials")
async def test_put_dbtcore_postgres_failure(mock_update: AsyncMock):
    request = Mock()
    payload = DbtCoreCredentialUpdate(
        blockName="block-name", credentials={"cred-key": "cred-val"}
    )
    mock_update.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await put_dbtcore_postgres(request, payload)
    assert (
        excinfo.value.detail == "failed to update dbt core block credentials [postgres]"
    )


@pytest.mark.asyncio
@patch("proxy.main.update_postgres_credentials")
async def test_put_dbtcore_postgres_success(mock_update: AsyncMock):
    request = Mock()
    payload = DbtCoreCredentialUpdate(
        blockName="block-name", credentials={"cred-key": "cred-val"}
    )
    response = await put_dbtcore_postgres(request, payload)
    assert response == {"success": 1}


@pytest.mark.asyncio
async def test_put_dbtcore_bigquery_badparams():
    request = Mock()
    with pytest.raises(TypeError) as excinfo:
        await put_dbtcore_bigquery(request, 1)
    assert str(excinfo.value) == "payload is invalid"


@pytest.mark.asyncio
@patch("proxy.main.update_bigquery_credentials")
async def test_put_dbtcore_bigquery_failure(mock_update: AsyncMock):
    request = Mock()
    payload = DbtCoreCredentialUpdate(
        blockName="block-name", credentials={"cred-key": "cred-val"}
    )
    mock_update.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await put_dbtcore_bigquery(request, payload)
    assert (
        excinfo.value.detail == "failed to update dbt core block credentials [bigquery]"
    )


@pytest.mark.asyncio
@patch("proxy.main.update_bigquery_credentials")
async def test_put_dbtcore_bigquery_success(mock_update: AsyncMock):
    request = Mock()
    payload = DbtCoreCredentialUpdate(
        blockName="block-name", credentials={"cred-key": "cred-val"}
    )
    response = await put_dbtcore_bigquery(request, payload)
    assert response == {"success": 1}


@pytest.mark.asyncio
async def test_put_dbtcore_schema_badparams():
    request = Mock()
    with pytest.raises(TypeError) as excinfo:
        await put_dbtcore_schema(request, 1)
    assert str(excinfo.value) == "payload is invalid"


@pytest.mark.asyncio
@patch("proxy.main.update_target_configs_schema")
async def test_put_dbtcore_schema_failure(mock_update: AsyncMock):
    request = Mock()
    payload = DbtCoreSchemaUpdate(
        blockName="block-name", target_configs_schema="target"
    )
    mock_update.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await put_dbtcore_schema(request, payload)
    assert (
        excinfo.value.detail == "failed to update dbt core block target_configs_schema"
    )


@pytest.mark.asyncio
@patch("proxy.main.update_target_configs_schema")
async def test_put_dbtcore_schema_success(mock_update: AsyncMock):
    request = Mock()
    payload = DbtCoreSchemaUpdate(
        blockName="block-name", target_configs_schema="target"
    )
    response = await put_dbtcore_schema(request, payload)
    assert response == {"success": 1}


@pytest.mark.asyncio
async def test_post_secret_block_badparams():
    request = Mock()
    with pytest.raises(TypeError) as excinfo:
        await post_secret_block(request, 1)
    assert str(excinfo.value) == "payload is invalid"


@pytest.mark.asyncio
@patch("proxy.main.create_secret_block")
async def test_post_secret_block_failure(mock_create: AsyncMock):
    request = Mock()
    payload = PrefectSecretBlockCreate(blockName="block-name", secret="secret")
    mock_create.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await post_secret_block(request, payload)
    assert excinfo.value.detail == "failed to prefect secret block"


@pytest.mark.asyncio
@patch("proxy.main.create_secret_block")
async def test_post_secret_block_success(mock_create: AsyncMock):
    request = Mock()
    payload = PrefectSecretBlockCreate(blockName="block-name", secret="secret")
    mock_create.return_value = ("block_id", "cleaned_blockname")
    response = await post_secret_block(request, payload)
    assert response == {"block_id": "block_id", "block_name": "cleaned_blockname"}


@pytest.mark.asyncio
async def test_delete_block_success():
    request = client.request("POST", "/")
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.status_code = 204
        response = await delete_block(request, "12345")
        assert response is None


@pytest.mark.asyncio
async def test_delete_block_failure():
    request = client.request("POST", "/")
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.side_effect = Exception("test error")
        mock_delete.return_value.text = "test error"
        with pytest.raises(HTTPException) as excinfo:
            await delete_block(request, "12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "test error"


@pytest.mark.asyncio
async def test_delete_block_invalid_blockid():
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await delete_block(request, None)
    assert excinfo.value.args[0] == "block_id must be a string"


@pytest.mark.asyncio
async def test_sync_airbyte_connection_flow_success():
    request = client.request("POST", "/")
    payload = RunFlow(
        blockName="test_block", flowName="test_flow", flowRunName="test_flow_run"
    )
    with patch("proxy.main.airbytesync", return_value="test result"):
        response = await sync_airbyte_connection_flow(request, payload)
        assert response == "test result"


@pytest.mark.asyncio
async def test_sync_airbyte_connection_flow_failure():
    payload = RunFlow(blockName="", flowName="test_flow", flowRunName="test_flow_run")
    request = client.request("POST", "/")
    with pytest.raises(HTTPException) as excinfo:
        await sync_airbyte_connection_flow(request, payload)
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "received empty blockName"


@pytest.mark.asyncio
async def test_sync_airbyte_connection_flow_invalid_payload():
    payload = None
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await sync_airbyte_connection_flow(request, payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_sync_dbtcore_flow_success():
    payload = RunFlow(
        blockName="test_block", flowName="test_flow", flowRunName="test_flow_run"
    )
    request = client.request("POST", "/")
    with patch("proxy.main.dbtrun", return_value="test result"):
        response = await sync_dbtcore_flow(request, payload)
        assert response == {"status": "success", "result": "test result"}


@pytest.mark.asyncio
async def test_sync_dbtcore_flow_failure():
    payload = RunFlow(blockName="", flowName="test_flow", flowRunName="test_flow_run")
    request = client.request("POST", "/")
    with pytest.raises(HTTPException) as excinfo:
        await sync_dbtcore_flow(request, payload)
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "received empty blockName"


@pytest.mark.asyncio
async def test_sync_dbtcore_flow_invalid_payload():
    payload = None
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await sync_dbtcore_flow(request, payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_post_dataflow_success():
    payload = DeploymentCreate(
        flow_name="test_block",
        deployment_name="FULL",
        org_slug="test_org",
        connection_blocks=[{"name": "test_block"}],
        dbt_blocks=[{"name": "test_block"}],
        cron="* * * * *",
    )
    request = client.request("POST", "/")
    with patch("proxy.main.post_deployment", return_value={"id": "67890"}):
        response = await post_dataflow(request, payload)
        assert response == {"deployment": {"id": "67890"}}


@pytest.mark.asyncio
async def test_post_dataflow_failure():
    request = client.request("POST", "/")
    payload = DeploymentCreate(
        flow_name="test_block",
        deployment_name="FULL",
        org_slug="test_org",
        connection_blocks=[{"name": "test_block"}],
        dbt_blocks=[{"name": "test_block"}],
        cron="* * * * *",
    )
    with patch("proxy.main.post_deployment", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            await post_dataflow(request, payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create deployment"


@pytest.mark.asyncio
async def test_post_dataflow_invalid_payload():
    payload = None
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await post_dataflow(request, payload)
    assert excinfo.value.args[0] == "payload is invalid"


def test_put_dataflow_badparams():
    request = Mock()
    with pytest.raises(TypeError) as excinfo:
        put_dataflow(request, "deployment-id", 123)
    assert str(excinfo.value) == "payload is invalid"


@patch("proxy.main.put_deployment")
def test_put_data_flow_failure(mock_put: Mock):
    request = Mock()
    payload = DeploymentUpdate(cron="* * * * *")
    mock_put.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        put_dataflow(request, "deployment-id", payload)
    assert excinfo.value.detail == "failed to update the deployment"


@patch("proxy.main.put_deployment")
def test_put_data_flow_success(mock_put: Mock):
    request = Mock()
    payload = DeploymentUpdate(cron="* * * * *")
    response = put_dataflow(request, "deployment-id", payload)
    assert response == {"success": 1}


def test_get_flow_run_by_id_badparams():
    request = Mock()
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_by_id(request, 123)
    assert str(excinfo.value) == "Flow run id must be a string"


@patch("proxy.main.get_flow_run")
def test_put_data_flow_failure(mock_get: Mock):
    request = Mock()
    mock_get.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        get_flow_run_by_id(request, "f-run")
    assert excinfo.value.detail == "failed to fetch flow_run f-run"


@patch("proxy.main.get_flow_run")
def test_put_data_flow_success(mock_get: Mock):
    request = Mock()
    mock_get.return_value = "flow-run"
    response = get_flow_run_by_id(request, "f-run")
    assert response == "flow-run"


@pytest.mark.asyncio
async def test_get_flowrun_success():
    payload = FlowRunRequest(name="test_flow_run")
    request = client.request("GET", "/")
    with patch("proxy.main.get_flow_runs_by_name", return_value=[{"id": "12345"}]):
        response = await get_flowrun(request, payload)
        assert response == {"flow_run": {"id": "12345"}}


@pytest.mark.asyncio
async def test_get_flowrun_failure():
    payload = FlowRunRequest(name="test_flow_run")
    request = client.request("GET", "/")
    with patch("proxy.main.get_flow_runs_by_name", return_value=[]):
        with pytest.raises(HTTPException) as excinfo:
            await get_flowrun(request, payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no such flow run"


@pytest.mark.asyncio
async def test_get_flowrun_invalid_payload():
    payload = None
    request = client.request("GET", "/")
    with pytest.raises(TypeError) as excinfo:
        await get_flowrun(request, payload)
    assert excinfo.value.args[0] == "payload is invalid"


def test_get_flow_runs_success():
    request = client.request("GET", "/")
    with patch(
        "proxy.main.get_flow_runs_by_deployment_id", return_value=[{"id": "12345"}]
    ):
        response = get_flow_runs(request, "67890")
        assert response == {"flow_runs": [{"id": "12345"}]}


def test_get_flow_runs_failure():
    request = client.request("GET", "/")
    with patch(
        "proxy.main.get_flow_runs_by_deployment_id", side_effect=Exception("test error")
    ):
        with pytest.raises(HTTPException) as excinfo:
            get_flow_runs(request, "67890")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to fetch flow_runs for deployment"


def test_get_flow_runs_invalid_deployment_id():
    request = client.request("GET", "/")
    with pytest.raises(TypeError) as excinfo:
        get_flow_runs(request, None, 0)
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_get_flow_runs_with_invalid_limit():
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        get_flow_runs(request, "67890", None)
    assert excinfo.value.args[0] == "limit must be an integer"


def test_get_flow_runs_limit_less_than_zero():
    request = client.request("GET", "/")
    with pytest.raises(ValueError) as excinfo:
        get_flow_runs(request, "67890", -1)
    assert excinfo.value.args[0] == "limit must be positive"


def test_post_deployments_success():
    payload = DeploymentFetch(org_slug="test_org", deployment_ids=["12345"])
    request = client.request("POST", "/")
    with patch("proxy.main.get_deployments_by_filter", return_value=[{"id": "12345"}]):
        response = post_deployments(request, payload)
        assert response == {"deployments": [{"id": "12345"}]}


def test_post_deployments_failure():
    payload = DeploymentFetch(org_slug="test_org", deployment_ids=["12345"])
    request = client.request("POST", "/")
    with patch(
        "proxy.main.get_deployments_by_filter", side_effect=Exception("test error")
    ):
        with pytest.raises(HTTPException) as excinfo:
            post_deployments(request, payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to filter deployments"


def test_post_deployments_invalid_payload():
    payload = None
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        post_deployments(request, payload)
    assert excinfo.value.args[0] == "payload is invalid"


def test_get_flow_run_logs_paginated_success():
    request = client.request("GET", "/")
    with patch("proxy.main.get_flow_run_logs", return_value="test logs"):
        response = get_flow_run_logs_paginated(request, "12345")
        assert response == "test logs"


def test_get_flow_run_logs_paginated_failure():
    request = client.request("GET", "/")
    with patch("proxy.main.get_flow_run_logs", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            get_flow_run_logs_paginated(request, "12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to fetch logs for flow_run"


def test_get_flow_run_logs_paginated_invalid_flow_run_id():
    request = client.request("GET", "/")
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated(request, None, 0)
    assert excinfo.value.args[0] == "flow_run_id must be a string"


def test_get_flow_run_logs_paginated_invalid_offset():
    request = client.request("GET", "/")
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated(request, "12345", None)
    assert excinfo.value.args[0] == "offset must be an integer"


def test_get_flow_run_logs_paginated_offset_less_than_zero():
    request = client.request("GET", "/")
    with pytest.raises(ValueError) as excinfo:
        get_flow_run_logs_paginated(request, "12345", -1)
    assert excinfo.value.args[0] == "offset must be positive"


def test_get_read_deployment_success():
    deployment_id = "test-deployment-id"
    mock_deployment_data = {
        "name": "test-deployment",
        "id": deployment_id,
        "tags": ["tag1", "tag2"],
        "schedule": {"cron": "* * * * *"},
        "is_schedule_active": True,
        "parameters": {"airbyte_blocks": []},
    }
    request = client.request("GET", "/")
    with patch("proxy.main.get_deployment") as mock_get_deployment:
        mock_get_deployment.return_value = mock_deployment_data
        response = get_read_deployment(request, deployment_id)
    assert response["deploymentId"] == deployment_id


def test_get_read_deployment_failure():
    request = client.request("GET", "/")
    with patch("proxy.main.get_deployment", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            deployment_id = "test-deployment-id"
            get_read_deployment(request, deployment_id)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to fetch deployment " + deployment_id


def test_get_read_deployment_invalid_deployment_id():
    request = client.request("GET", "/")
    with pytest.raises(TypeError) as excinfo:
        get_read_deployment(request, None)
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_delete_deployment_success():
    request = client.request("POST", "/")
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.return_value = None
        response = delete_deployment(request, "12345")
        assert response is None


def test_delete_deployment_failure():
    request = client.request("POST", "/")
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.side_effect = Exception("test error")
        mock_delete.return_value.text = "test error"
        with pytest.raises(HTTPException) as excinfo:
            delete_deployment(request, "12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "test error"


def test_delete_deployment_invalid_deployment_id():
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        delete_deployment(request, None)
    assert excinfo.value.args[0] == "deployment_id must be a string"


@pytest.mark.asyncio
async def test_post_create_deployment_flow_run_success():
    request = client.request("POST", "/")
    with patch("proxy.main.post_deployment_flow_run", return_value="test result"):
        response = await post_create_deployment_flow_run(request, "12345")
        assert response == "test result"


@pytest.mark.asyncio
async def test_post_create_deployment_flow_run_failure():
    request = client.request("POST", "/")
    with patch(
        "proxy.main.post_deployment_flow_run", side_effect=Exception("test error")
    ):
        with pytest.raises(HTTPException) as excinfo:
            await post_create_deployment_flow_run(request, "12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create flow_run for deployment"


@pytest.mark.asyncio
async def test_post_create_deployment_flow_run_invalid_deployment_id():
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await post_create_deployment_flow_run(request, None)
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_post_deployment_set_schedule_success():
    request = client.request("POST", "/")
    with patch("proxy.main.set_deployment_schedule"):
        response = post_deployment_set_schedule(request, "12345", "active")
        assert response == {"success": 1}


def test_post_deployment_set_schedule_failure():
    request = client.request("POST", "/")
    with pytest.raises(HTTPException) as excinfo:
        post_deployment_set_schedule(request, "12345", "invalid_status")
    assert excinfo.value.status_code == 422
    assert excinfo.value.detail == "incorrect status value"


def test_post_deployment_set_schedule_invalid_deployment_id():
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        post_deployment_set_schedule(request, None, "active")
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_post_deployment_set_schedule_invalid_status():
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        post_deployment_set_schedule(request, "12345", None)
    assert excinfo.value.args[0] == "status must be a string"
