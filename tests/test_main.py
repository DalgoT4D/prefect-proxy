from unittest.mock import Mock, AsyncMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from proxy.main import (
    airbytesync,
    app,
    dbtrun_v1,
    shelloprun,
    delete_block,
    delete_deployment,
    get_airbyte_server,
    get_dbtcli_profile,
    get_flow_run_logs_paginated,
    get_flow_runs,
    get_flowrun,
    get_read_deployment,
    post_airbyte_server,
    post_create_deployment_flow_run,
    post_dbtcore,
    post_dbtcli_profile,
    put_dbtcli_profile,
    put_dbtcore_postgres,
    put_dbtcore_bigquery,
    put_dbtcore_schema,
    get_flow_run_by_id,
    post_secret_block,
    put_secret_block,
    post_deployment_set_schedule,
    post_deployments,
    sync_shellop_flow,
    sync_dbtcore_flow_v1,
    post_dataflow_v1,
    put_dataflow_v1,
)

from proxy.schemas import (
    AirbyteServerCreate,
    DbtCoreCreate,
    DbtCoreCredentialUpdate,
    DbtProfileCreate,
    DbtCoreSchemaUpdate,
    RunDbtCoreOperation,
    PrefectSecretBlockCreate,
    PrefectSecretBlockEdit,
    DbtCliProfileBlockCreate,
    DbtCliProfileBlockUpdate,
    DeploymentFetch,
    FlowRunRequest,
    RunShellOperation,
    DeploymentCreate2,
    DeploymentUpdate2,
)

app = FastAPI()
client = TestClient(app)


def test_airbytesync_success():
    block_name = "example_block"
    flow_name = "example_flow"
    flow_run_name = "example_flow_run"
    inner_result = {"status": "success", "result": "example_result"}
    expected_result = {"status": "success", "result": inner_result}

    with patch("proxy.main.run_airbyte_connection_flow") as mock_run_airbyte_connection_flow:
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

    with patch("proxy.main.run_airbyte_connection_flow") as mock_run_airbyte_connection_flow:
        with patch.object(
            mock_run_airbyte_connection_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: inner_result
            result = airbytesync(block_name, flow_name, flow_run_name)
            assert result == expected_result


def test_airbytesync_http_exception():
    block_name = "example_block"
    flow_name = ""
    flow_run_name = ""

    with patch("proxy.main.run_airbyte_connection_flow") as mock_run_airbyte_connection_flow:
        mock_run_airbyte_connection_flow.side_effect = HTTPException(
            status_code=400, detail="Job 12345 failed."
        )
        result = airbytesync(block_name, flow_name, flow_run_name)
        assert result == {"status": "failed", "airbyte_job_num": "12345"}


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


def test_dbtrun_v1():
    """tests dbtrun_v1"""
    task_config = RunDbtCoreOperation(
        flow_name="",
        flow_run_name="",
        type="TYPE",
        slug="SLUG",
        profiles_dir=".",
        project_dir=".",
        working_dir=".",
        env={},
        commands=[],
        cli_profile_block="block-name",
    )
    with patch("proxy.main.run_dbtcore_flow_v1") as mock_run_dbtcore_flow_v1:
        mock_run_dbtcore_flow_v1.return_value = {"result": "example_result"}
        result = dbtrun_v1(task_config)
        assert result == {"result": "example_result"}
        mock_run_dbtcore_flow_v1.assert_called_once_with(task_config)


def test_shelloprun_success():
    expected_result = {"result": "example_result", "status": "success"}
    task_config = RunShellOperation(
        type="Shell operation",
        slug="git-pull",
        commands=["echo test"],
        working_dir="/tmp",
        env={},
        flow_name="example_flow",
        flow_run_name="example_flow_run",
    )

    with patch("proxy.main.run_shell_operation_flow") as mock_run_shell_operation_flow:
        with patch.object(
            mock_run_shell_operation_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: expected_result
            result = shelloprun(task_config)
            assert result == expected_result


def test_shelloprun_failure():
    expected_result = {"result": "example_result", "status": "failed"}
    task_config = RunShellOperation(
        type="Shell operation",
        slug="git-pull",
        commands=["echo test"],
        working_dir="/tmp",
        env={},
        flow_name="example_flow",
        flow_run_name="example_flow_run",
    )

    with patch("proxy.main.run_shell_operation_flow") as mock_run_shell_operation_flow:
        with patch.object(
            mock_run_shell_operation_flow.with_options.return_value, "with_options"
        ) as mock_with_options:
            mock_with_options.return_value = lambda x: expected_result
            result = shelloprun(task_config)
            assert result == expected_result


def test_airbyte_sync_invalid_payload_type():
    task_config = None

    with pytest.raises(TypeError):
        shelloprun(task_config)


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
    with patch("proxy.main.create_airbyte_server_block", side_effect=Exception("test error")):
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
        cli_profile_block_name="test_cli_profile",
        commands=['echo "Hello, World!"'],
        env={"TEST_ENV": "test_value"},
        working_dir="test_dir",
        profiles_dir="test_profiles_dir",
        project_dir="test_project_dir",
    )
    with patch("proxy.main.create_dbt_core_block", return_value=("12345", "test_dbt_cleaned")):
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
        cli_profile_block_name="test_cli_profile",
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
    payload = DbtCoreCredentialUpdate(blockName="block-name", credentials={"cred-key": "cred-val"})
    mock_update.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await put_dbtcore_postgres(request, payload)
    assert excinfo.value.detail == "failed to update dbt core block credentials [postgres]"


@pytest.mark.asyncio
@patch("proxy.main._create_dbt_cli_profile")
async def test_post_dbtcli_profile_success(mock__create_dbt_cli_profile: AsyncMock):
    """tests post_dbtcli_profile"""
    request = Mock()
    payload = DbtCliProfileBlockCreate(
        cli_profile_block_name="block-name",
        profile={"name": "NAME", "target_configs_schema": "SCHEMA"},
        wtype="postgres",
        credentials={},
    )

    mock__create_dbt_cli_profile.return_value = ("ignore", "block-id", "block-name")
    result = await post_dbtcli_profile(request, payload)
    assert result == {"block_id": "block-id", "block_name": "block-name"}


@pytest.mark.asyncio
@patch("proxy.main._create_dbt_cli_profile")
async def test_post_dbtcli_profile_raise(mock__create_dbt_cli_profile: AsyncMock):
    """tests post_dbtcli_profile"""
    request = Mock()
    payload = DbtCliProfileBlockCreate(
        cli_profile_block_name="block-name",
        profile={"name": "NAME", "target_configs_schema": "SCHEMA"},
        wtype="postgres",
        credentials={},
    )
    mock__create_dbt_cli_profile.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await post_dbtcli_profile(request, payload)
    assert excinfo.value.detail == "failed to create dbt cli profile block"


@pytest.mark.asyncio
@patch("proxy.main.update_dbt_cli_profile")
async def test_put_dbtcli_profile_success(mock_update_dbt_cli_profile: AsyncMock):
    """tests put_dbtcli_profile"""
    request = Mock()
    payload = DbtCliProfileBlockUpdate(
        cli_profile_block_name="block-name",
        profile={"name": "NAME", "target_configs_schema": "SCHEMA"},
        wtype="postgres",
        credentials={},
    )

    mock_update_dbt_cli_profile.return_value = ("ignore", "block-id", "block-name")
    result = await put_dbtcli_profile(request, payload)
    assert result == {"block_id": "block-id", "block_name": "block-name"}


@pytest.mark.asyncio
@patch("proxy.main.update_dbt_cli_profile")
async def test_put_dbtcli_profile_raise(mock_update_dbt_cli_profile: AsyncMock):
    """tests put_dbtcli_profile"""
    request = Mock()
    payload = DbtCliProfileBlockUpdate(
        cli_profile_block_name="block-name",
        profile={"name": "NAME", "target_configs_schema": "SCHEMA"},
        wtype="postgres",
        credentials={},
    )
    mock_update_dbt_cli_profile.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await put_dbtcli_profile(request, payload)
    assert excinfo.value.detail == "failed to update dbt cli profile block"


@pytest.mark.asyncio
async def test_get_dbt_cli_profile_failure_1():
    """Tests get_dbt_cli_profile"""
    request = Mock()
    with pytest.raises(TypeError) as excinfo:
        await get_dbtcli_profile(request, None)

    assert str(excinfo.value) == "cli_profile_block_name is invalid"


@pytest.mark.asyncio
@patch("proxy.main.get_dbt_cli_profile")
async def test_get_dbt_cli_profile_success(mock_get_dbt_cli_profile: AsyncMock):
    """Tests get_dbt_cli_profile"""
    request = Mock()
    mock_get_dbt_cli_profile.return_value = {"key": "value"}
    result = await get_dbtcli_profile(request, "block-name")
    assert result["profile"]["key"] == "value"


@pytest.mark.asyncio
@patch("proxy.main.get_dbt_cli_profile")
async def test_get_dbt_cli_profile_failure_2(mock_get_dbt_cli_profile: AsyncMock):
    """Tests get_dbt_cli_profile"""
    request = Mock()
    mock_get_dbt_cli_profile.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        result = await get_dbtcli_profile(request, "block-name")
    assert excinfo.value.detail == "failed to fetch dbt cli profile block"


@pytest.mark.asyncio
@patch("proxy.main.update_postgres_credentials")
async def test_put_dbtcore_postgres_success(mock_update: AsyncMock):
    request = Mock()
    payload = DbtCoreCredentialUpdate(blockName="block-name", credentials={"cred-key": "cred-val"})
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
    payload = DbtCoreCredentialUpdate(blockName="block-name", credentials={"cred-key": "cred-val"})
    mock_update.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await put_dbtcore_bigquery(request, payload)
    assert excinfo.value.detail == "failed to update dbt core block credentials [bigquery]"


@pytest.mark.asyncio
@patch("proxy.main.update_bigquery_credentials")
async def test_put_dbtcore_bigquery_success(mock_update: AsyncMock):
    request = Mock()
    payload = DbtCoreCredentialUpdate(blockName="block-name", credentials={"cred-key": "cred-val"})
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
    payload = DbtCoreSchemaUpdate(blockName="block-name", target_configs_schema="target")
    mock_update.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await put_dbtcore_schema(request, payload)
    assert excinfo.value.detail == "failed to update dbt core block target_configs_schema"


@pytest.mark.asyncio
@patch("proxy.main.update_target_configs_schema")
async def test_put_dbtcore_schema_success(mock_update: AsyncMock):
    request = Mock()
    payload = DbtCoreSchemaUpdate(blockName="block-name", target_configs_schema="target")
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
@patch("proxy.main.upsert_secret_block")
async def test_put_secret_block_failure(mock_edit: AsyncMock):
    request = Mock()
    payload = PrefectSecretBlockEdit(blockName="block-name", secret="secret")
    mock_edit.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await put_secret_block(request, payload)
    assert excinfo.value.detail == "failed to prefect secret block"


@pytest.mark.asyncio
@patch("proxy.main.upsert_secret_block")
async def test_put_secret_block_success(mock_edit: AsyncMock):
    request = Mock()
    payload = PrefectSecretBlockEdit(blockName="block-name", secret="secret")
    mock_edit.return_value = ("block_id", "cleaned_blockname")
    response = await put_secret_block(request, payload)
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
async def test_sync_shellop_flow_success():
    payload = RunShellOperation(
        type="Shell operation",
        slug="test-op",
        commands=['echo "Hello, World!"'],
        working_dir="test_dir",
        env={"key": "test_value"},
        flow_name="shell_test_flow",
        flow_run_name="shell_test_flow",
    )
    request = client.request("POST", "/")
    with patch("proxy.main.shelloprun", return_value="test result"):
        response = await sync_shellop_flow(request, payload)
        assert response == {"status": "success", "result": "test result"}


@pytest.mark.asyncio
async def test_sync_shellop_flow_invalid_payload():
    payload = None
    request = client.request("POST", "/")
    with pytest.raises(TypeError) as excinfo:
        await sync_shellop_flow(request, payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_sync_dbtcore_flow_v1():
    """tests sync_dbtcore_flow_v1"""
    request = Mock()
    payload = RunDbtCoreOperation(
        slug="slug",
        type="dbtrun",
        profiles_dir=".",
        project_dir=".",
        working_dir=".",
        env={},
        commands=[],
        cli_profile_block="block",
        flow_name="",
        flow_run_name="",
    )
    with patch("proxy.main.dbtrun_v1") as mock_dbtrun_v1:
        mock_dbtrun_v1.return_value = "test result"
        result = await sync_dbtcore_flow_v1(request, payload)
        assert result == {"status": "success", "result": "test result"}


def test_put_dataflow_v1_raises():
    """put_dataflow_v1 raises http exception"""
    request = Mock()
    with patch("proxy.main.put_deployment_v1") as mock_put_dataflow_v1:
        mock_put_dataflow_v1.side_effect = Exception()
        payload = DeploymentUpdate2(deployment_params={})
        with pytest.raises(HTTPException) as excinfo:
            put_dataflow_v1(request, "deployment-id", payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to update the deployment"


def test_put_dataflow_v1_success():
    """put_dataflow_v1 raises http exception"""
    request = Mock()
    with patch("proxy.main.put_deployment_v1"):
        payload = DeploymentUpdate2(deployment_params={})
        result = put_dataflow_v1(request, "deployment-id", payload)
        assert result == {"success": 1}


def test_get_flow_run_by_id_badparams():
    request = Mock()
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_by_id(request, 123)
    assert str(excinfo.value) == "Flow run id must be a string"


@patch("proxy.main.get_flow_run")
def test_get_flow_run_by_id_failure(mock_get: Mock):
    request = Mock()
    mock_get.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        get_flow_run_by_id(request, "f-run")
    assert excinfo.value.detail == "failed to fetch flow_run f-run"


@patch("proxy.main.get_flow_run")
def test_get_flow_run_by_id_success(mock_get: Mock):
    request = Mock()
    mock_get.return_value = {
        "id": "12345",
        "state": {"type": "COMPLETED"},
        "status": "COMPLETED",
    }
    response = get_flow_run_by_id(request, "f-run")
    assert response == {
        "id": "12345",
        "state": {"type": "COMPLETED"},
        "status": "COMPLETED",
    }


@pytest.mark.asyncio
async def test_get_flowrun_success():
    payload = FlowRunRequest(name="test_flow_run")
    request = client.request("GET", "/")
    with patch(
        "proxy.main.get_flow_runs_by_name",
        return_value=[{"id": "12345", "state": {"type": "COMPLETED"}, "status": "COMPLETED"}],
    ):
        response = await get_flowrun(request, payload)
        assert response == {
            "flow_run": {
                "id": "12345",
                "state": {"type": "COMPLETED"},
                "status": "COMPLETED",
            }
        }


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
        "proxy.main.get_flow_runs_by_deployment_id",
        return_value=[{"id": "12345", "state": {"type": "COMPLETED"}}],
    ):
        response = get_flow_runs(request, "67890")
        assert response == {"flow_runs": [{"id": "12345", "state": {"type": "COMPLETED"}}]}


def test_get_flow_runs_failure():
    request = client.request("GET", "/")
    with patch("proxy.main.get_flow_runs_by_deployment_id", side_effect=Exception("test error")):
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
    with patch("proxy.main.get_deployments_by_filter", side_effect=Exception("test error")):
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
        get_flow_run_logs_paginated(request, None)
    assert excinfo.value.args[0] == "flow_run_id must be a string"


def test_get_flow_run_logs_paginated_invalid_task_run_id():
    request = client.request("GET", "/")
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated(request, "12345", None)
    assert excinfo.value.args[0] == "task_run_id must be a string"


def test_get_flow_run_logs_paginated_invalid_offset():
    request = client.request("GET", "/")
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated(request, "12345", "12345", 0, None)
    assert excinfo.value.args[0] == "offset must be an integer"


def test_get_flow_run_logs_paginated_invalid_limit():
    request = client.request("GET", "/")
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated(request, "12345", "12345", None)
    assert excinfo.value.args[0] == "limit must be an integer"


def test_get_flow_run_logs_paginated_offset_less_than_zero():
    request = client.request("GET", "/")
    with pytest.raises(ValueError) as excinfo:
        get_flow_run_logs_paginated(request, "12345", "12345", 0, -1)
    assert excinfo.value.args[0] == "offset must be positive"


def test_get_flow_run_logs_paginated_offset_less_than_zero():
    request = client.request("GET", "/")
    with pytest.raises(ValueError) as excinfo:
        get_flow_run_logs_paginated(request, "12345", "12345", -1)
    assert excinfo.value.args[0] == "limit must be positive"


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
    with patch("proxy.main.post_deployment_flow_run", side_effect=Exception("test error")):
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


@pytest.mark.asyncio
@patch("proxy.main.post_deployment_v1")
async def test_post_dataflow_v1_success(mock_post_deployment_v1: AsyncMock):
    """tests post_dataflow_v1"""
    request = Mock()
    payload = DeploymentCreate2(
        flow_name="", deployment_name="", org_slug="org", deployment_params={}
    )

    mock_post_deployment_v1.return_value = {"id": "12345"}

    result = post_dataflow_v1(request, payload)
    assert result == {"deployment": {"id": "12345"}}


@pytest.mark.asyncio
@patch("proxy.main.post_deployment_v1")
async def test_post_dataflow_v1_failure(mock_post_deployment_v1: AsyncMock):
    """tests post_dataflow_v1"""
    request = Mock()
    payload = DeploymentCreate2(
        flow_name="", deployment_name="", org_slug="org", deployment_params={}
    )

    mock_post_deployment_v1.side_effect = Exception()
    with pytest.raises(HTTPException) as excinfo:
        await post_dataflow_v1(request, payload)
    assert excinfo.value.detail == "failed to create deployment"
