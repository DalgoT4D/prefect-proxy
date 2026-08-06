import os
from unittest.mock import Mock, AsyncMock, patch

import pytest
from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient

from proxy.main import (
    app,
    dbtrun_v1,
    shelloprun,
    delete_block,
    delete_deployment,
    get_airbyte_server,
    get_flow_run_logs_paginated,
    get_flow_runs,
    get_flowrun,
    get_read_deployment,
    post_airbyte_server,
    put_airbyte_server,
    put_airbyte_connection,
    post_create_deployment_flow_run,
    get_flow_run_by_id,
    post_secret_block,
    put_secret_block,
    post_deployment_set_schedule,
    post_deployments,
    post_run_shellop_flow,
    post_run_dbtcore_flow_v1,
    post_dataflow_v1,
    put_dataflow_v1,
    get_dataflow_scheduled_flow_runs,
    get_long_running_flows,
)

from proxy.schemas import (
    AirbyteServerCreate,
    AirbyteServerUpdate,
    AirbyteConnectionCreate,
    RunDbtCoreOperation,
    PrefectSecretBlockCreate,
    PrefectSecretBlockEdit,
    DeploymentFetch,
    FlowRunRequest,
    RunShellOperation,
    DeploymentCreate2,
    DeploymentUpdate2,
)

app = FastAPI()
client = TestClient(app)


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
    with patch("proxy.main.dbtjob_v2_runner") as mock_dbtjob_v2_runner:
        mock_dbtjob_v2_runner.return_value = {"result": "example_result"}
        result = dbtrun_v1(task_config)
        assert result == {"result": "example_result"}
        # Runner is invoked with the serialized payload + slug — the runner
        # decorator provides its own flow_name/flow_run_name so the caller's
        # values are intentionally ignored.
        mock_dbtjob_v2_runner.assert_called_once_with(task_config.model_dump(), task_config.slug)


def test_dbtrun_v1_rejects_invalid_payload():
    """dbtrun_v1 must reject anything that isn't a RunDbtCoreOperation — this
    is the endpoint's first line of defense against malformed input."""
    with pytest.raises(TypeError):
        dbtrun_v1({"not": "a valid payload"})


def test_dbtrun_v1_wraps_runner_failure():
    """When the underlying runner raises, dbtrun_v1 must surface it as a
    400 HTTPException so the API returns a client error rather than a 500."""
    task_config = RunDbtCoreOperation(
        flow_name="",
        flow_run_name="",
        type="TYPE",
        slug="dbt-run",
        profiles_dir=".",
        project_dir=".",
        working_dir=".",
        env={},
        commands=[],
        cli_profile_block="block-name",
    )
    with patch("proxy.main.dbtjob_v2_runner", side_effect=RuntimeError("boom")):
        with pytest.raises(HTTPException) as exc_info:
            dbtrun_v1(task_config)
        assert exc_info.value.status_code == 400
        assert "dbt-run" in exc_info.value.detail


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

    with patch("proxy.main.shellopjob") as mock_shellopjob:
        mock_shellopjob.return_value = expected_result
        result = shelloprun(task_config)
        assert result == expected_result
        mock_shellopjob.assert_called_once_with(task_config.model_dump(), task_config.slug)


def test_shelloprun_failure():
    """When shellopjob raises, shelloprun must convert it to a 400 HTTPException."""
    task_config = RunShellOperation(
        type="Shell operation",
        slug="git-pull",
        commands=["echo test"],
        working_dir="/tmp",
        env={},
        flow_name="example_flow",
        flow_run_name="example_flow_run",
    )

    with patch("proxy.main.shellopjob", side_effect=RuntimeError("boom")):
        with pytest.raises(HTTPException) as exc_info:
            shelloprun(task_config)
        assert exc_info.value.status_code == 400


def test_shelloprun_rejects_invalid_payload():
    """shelloprun must reject anything that isn't a RunShellOperation."""
    with pytest.raises(TypeError):
        shelloprun({"not": "a valid payload"})


def test_airbyte_sync_invalid_payload_type():
    task_config = None

    with pytest.raises(TypeError):
        shelloprun(task_config)


@pytest.mark.asyncio
async def test_get_airbyte_server_success():
    with patch("proxy.main.get_airbyte_server_block_id", return_value="12345"):
        response = await get_airbyte_server("test_block")
        assert response == {"block_id": "12345"}


@pytest.mark.asyncio
async def test_get_airbyte_server_failure():
    with patch("proxy.main.get_airbyte_server_block_id") as mock_get_block_id:
        mock_get_block_id.side_effect = Exception("Test error")
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_server("test_block")
        assert excinfo.value.status_code == 500
        assert excinfo.value.detail == "Internal server error"


@pytest.mark.asyncio
async def test_get_airbyte_server_invalid_block_name():
    with pytest.raises(TypeError) as excinfo:
        await get_airbyte_server(None)
    assert excinfo.value.args[0] == "blockname must be a string"


@pytest.mark.asyncio
async def test_post_airbyte_server_success():
    payload = AirbyteServerCreate(
        blockName="testserver",
        serverHost="http://test-server.com",
        serverPort="8000",
        apiVersion="v1",
    )
    with patch("proxy.main.create_airbyte_server_block", return_value=("12345", "testserver")):
        response = await post_airbyte_server(payload)
        assert response == {"block_id": "12345", "cleaned_block_name": "testserver"}


@pytest.mark.asyncio
async def test_post_airbyte_server_failure():
    payload = AirbyteServerCreate(
        blockName="testserver",
        serverHost="http://test-server.com",
        serverPort="8000",
        apiVersion="v1",
    )
    with patch("proxy.main.create_airbyte_server_block", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            await post_airbyte_server(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create airbyte server block"


@pytest.mark.asyncio
async def test_post_airbyte_server_with_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await post_airbyte_server(payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_put_airbyte_server_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await put_airbyte_server(payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_put_airbyte_server_exception():
    payload = AirbyteServerUpdate(
        blockName="testserver",
        serverHost="http://test-server.com",
        serverPort="8000",
        apiVersion="v1",
    )
    with patch("proxy.main.update_airbyte_server_block", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            await put_airbyte_server(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to update airbyte server block"


@pytest.mark.asyncio
async def test_put_airbyte_server_success():
    payload = AirbyteServerUpdate(
        blockName="testserver",
        serverHost="http://test-server.com",
        serverPort="8000",
        apiVersion="v1",
    )
    with patch("proxy.main.update_airbyte_server_block", return_value=("12345", "testserver")):
        response = await put_airbyte_server(payload)
        assert response == {"block_id": "12345", "cleaned_block_name": "testserver"}


@pytest.mark.asyncio
async def test_put_airbyte_connection_invalid_payload():
    with pytest.raises(TypeError) as excinfo:
        await put_airbyte_connection(None)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
async def test_put_airbyte_connection_exception():
    payload = AirbyteConnectionCreate(
        serverBlockName="server-block",
        connectionId="conn-uuid",
        connectionBlockName="conn-uuid",
    )
    with patch("proxy.main.upsert_airbyte_connection_block", side_effect=Exception("boom")):
        with pytest.raises(HTTPException) as excinfo:
            await put_airbyte_connection(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to upsert airbyte connection block"


@pytest.mark.asyncio
async def test_put_airbyte_connection_success():
    payload = AirbyteConnectionCreate(
        serverBlockName="server-block",
        connectionId="conn-uuid",
        connectionBlockName="conn-uuid",
        connectionName="my-conn",
        extra={"env": {}, "post_sync_ops": []},
    )
    with patch(
        "proxy.main.upsert_airbyte_connection_block",
        return_value=("blockid-123", "conn-uuid"),
    ):
        response = await put_airbyte_connection(payload)
        assert response == {"block_id": "blockid-123", "cleaned_block_name": "conn-uuid"}



@pytest.mark.asyncio
async def test_post_secret_block_badparams():
    with pytest.raises(TypeError) as excinfo:
        await post_secret_block(1)
    assert str(excinfo.value) == "payload is invalid"


@pytest.mark.asyncio
@patch("proxy.main.create_secret_block")
async def test_post_secret_block_failure(mock_create: AsyncMock):
    payload = PrefectSecretBlockCreate(blockName="block-name", secret="secret")
    mock_create.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await post_secret_block(payload)
    assert excinfo.value.detail == "failed to prefect secret block"


@pytest.mark.asyncio
@patch("proxy.main.create_secret_block")
async def test_post_secret_block_success(mock_create: AsyncMock):
    payload = PrefectSecretBlockCreate(blockName="block-name", secret="secret")
    mock_create.return_value = ("block_id", "cleaned_blockname")
    response = await post_secret_block(payload)
    assert response == {"block_id": "block_id", "block_name": "cleaned_blockname"}


@pytest.mark.asyncio
@patch("proxy.main.upsert_secret_block")
async def test_put_secret_block_failure(mock_edit: AsyncMock):
    payload = PrefectSecretBlockEdit(blockName="block-name", secret="secret")
    mock_edit.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        await put_secret_block(payload)
    assert excinfo.value.detail == "failed to prefect secret block"


@pytest.mark.asyncio
@patch("proxy.main.upsert_secret_block")
async def test_put_secret_block_success(mock_edit: AsyncMock):
    payload = PrefectSecretBlockEdit(blockName="block-name", secret="secret")
    mock_edit.return_value = ("block_id", "cleaned_blockname")
    response = await put_secret_block(payload)
    assert response == {"block_id": "block_id", "block_name": "cleaned_blockname"}


@pytest.mark.asyncio
async def test_delete_block_success():
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.status_code = 204
        response = await delete_block("12345")
        assert response is None


@pytest.mark.asyncio
async def test_delete_block_failure():
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.side_effect = Exception("test error")
        mock_delete.return_value.text = "test error"
        with pytest.raises(HTTPException) as excinfo:
            await delete_block("12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "test error"


@pytest.mark.asyncio
async def test_delete_block_invalid_blockid():
    with pytest.raises(TypeError) as excinfo:
        await delete_block(None)
    assert excinfo.value.args[0] == "block_id must be a string"


@pytest.mark.asyncio
def test_post_run_shellop_flow_success():
    payload = RunShellOperation(
        type="Shell operation",
        slug="test-op",
        commands=['echo "Hello, World!"'],
        working_dir="test_dir",
        env={"key": "test_value"},
        flow_name="shell_test_flow",
        flow_run_name="shell_test_flow",
    )
    with patch("proxy.main.shelloprun", return_value="test result"):
        response = post_run_shellop_flow(payload)
        assert response == {"status": "success", "result": "test result"}


@pytest.mark.asyncio
def test_post_run_shellop_flow_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        post_run_shellop_flow(payload)
    assert excinfo.value.args[0] == "payload is invalid"


@pytest.mark.asyncio
def test_post_run_dbtcore_flow_v1():
    """tests post_run_dbtcore_flow_v1"""
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
        result = post_run_dbtcore_flow_v1(payload)
        assert result == {"status": "success", "result": "test result"}


def test_put_dataflow_v1_raises():
    """put_dataflow_v1 raises http exception"""
    with patch("proxy.main.put_deployment_v1") as mock_put_dataflow_v1:
        mock_put_dataflow_v1.side_effect = Exception()
        payload = DeploymentUpdate2(deployment_params={})
        with pytest.raises(HTTPException) as excinfo:
            put_dataflow_v1("deployment-id", payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to update the deployment"


def test_put_dataflow_v1_success():
    """put_dataflow_v1 raises http exception"""
    with patch("proxy.main.put_deployment_v1"):
        payload = DeploymentUpdate2(deployment_params={})
        result = put_dataflow_v1("deployment-id", payload)
        assert result == {"success": 1}


def test_get_dataflow_scheduled_flow_runs():
    with patch(
        "proxy.main.get_deployment_scheduled_flow_runs"
    ) as mock_get_deployment_scheduled_flow_runs:
        mock_get_deployment_scheduled_flow_runs.return_value = [{"id": "flow_run_id"}]
        result = get_dataflow_scheduled_flow_runs("deployment-id")
        assert result == {"flow_runs": [{"id": "flow_run_id"}]}


def test_get_flow_run_by_id_badparams():
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_by_id(123)
    assert str(excinfo.value) == "Flow run id must be a string"


@patch("proxy.main.get_flow_run")
def test_get_flow_run_by_id_failure(mock_get: Mock):
    mock_get.side_effect = Exception("exception")
    with pytest.raises(HTTPException) as excinfo:
        get_flow_run_by_id("f-run")
    assert excinfo.value.detail == "failed to fetch flow_run f-run"


@patch("proxy.main.get_flow_run")
def test_get_flow_run_by_id_success(mock_get: Mock):
    mock_get.return_value = {
        "id": "12345",
        "state": {"type": "COMPLETED"},
        "status": "COMPLETED",
    }
    response = get_flow_run_by_id("f-run")
    assert response == {
        "id": "12345",
        "state": {"type": "COMPLETED"},
        "status": "COMPLETED",
    }


@pytest.mark.asyncio
async def test_get_flowrun_success():
    payload = FlowRunRequest(name="test_flow_run")
    with patch(
        "proxy.main.get_flow_runs_by_name",
        return_value=[{"id": "12345", "state": {"type": "COMPLETED"}, "status": "COMPLETED"}],
    ):
        response = await get_flowrun(payload)
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
    with patch("proxy.main.get_flow_runs_by_name", return_value=[]):
        with pytest.raises(HTTPException) as excinfo:
            await get_flowrun(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "no such flow run"


@pytest.mark.asyncio
async def test_get_flowrun_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        await get_flowrun(payload)
    assert excinfo.value.args[0] == "payload is invalid"


def test_get_flow_runs_success():
    with patch(
        "proxy.main.get_flow_runs_by_deployment_id",
        return_value=[{"id": "12345", "state": {"type": "COMPLETED"}}],
    ):
        response = get_flow_runs("67890")
        assert response == {"flow_runs": [{"id": "12345", "state": {"type": "COMPLETED"}}]}


def test_get_flow_runs_failure():
    with patch("proxy.main.get_flow_runs_by_deployment_id", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            get_flow_runs("67890")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to fetch flow_runs for deployment"


def test_get_flow_runs_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        get_flow_runs(None, 0)
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_get_flow_runs_with_invalid_limit():
    with pytest.raises(TypeError) as excinfo:
        get_flow_runs("67890", None)
    assert excinfo.value.args[0] == "limit must be an integer"


def test_get_flow_runs_limit_less_than_zero():
    with pytest.raises(ValueError) as excinfo:
        get_flow_runs("67890", -1)
    assert excinfo.value.args[0] == "limit must be positive"


def test_post_deployments_success():
    payload = DeploymentFetch(org_slug="test_org", deployment_ids=["12345"])
    with patch("proxy.main.get_deployments_by_filter", return_value=[{"id": "12345"}]):
        response = post_deployments(payload)
        assert response == {"deployments": [{"id": "12345"}]}


def test_post_deployments_failure():
    payload = DeploymentFetch(org_slug="test_org", deployment_ids=["12345"])
    with patch("proxy.main.get_deployments_by_filter", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            post_deployments(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to filter deployments"


def test_post_deployments_invalid_payload():
    payload = None
    with pytest.raises(TypeError) as excinfo:
        post_deployments(payload)
    assert excinfo.value.args[0] == "payload is invalid"


def test_get_flow_run_logs_paginated_success():
    with patch("proxy.main.get_flow_run_logs", return_value="test logs"):
        response = get_flow_run_logs_paginated("12345")
        assert response == "test logs"


def test_get_flow_run_logs_paginated_failure():
    with patch("proxy.main.get_flow_run_logs", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            get_flow_run_logs_paginated("12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to fetch logs for flow_run"


def test_get_flow_run_logs_paginated_invalid_flow_run_id():
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated(None)
    assert excinfo.value.args[0] == "flow_run_id must be a string"


def test_get_flow_run_logs_paginated_invalid_task_run_id():
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated("12345", None)
    assert excinfo.value.args[0] == "task_run_id must be a string"


def test_get_flow_run_logs_paginated_invalid_offset():
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated("12345", "12345", 0, None)
    assert excinfo.value.args[0] == "offset must be an integer"


def test_get_flow_run_logs_paginated_invalid_limit():
    with pytest.raises(TypeError) as excinfo:
        get_flow_run_logs_paginated("12345", "12345", None)
    assert excinfo.value.args[0] == "limit must be an integer"


def test_get_flow_run_logs_paginated_offset_less_than_zero():
    with pytest.raises(ValueError) as excinfo:
        get_flow_run_logs_paginated("12345", "12345", 0, -1)
    assert excinfo.value.args[0] == "offset must be positive"


def test_get_flow_run_logs_paginated_offset_less_than_zero():
    with pytest.raises(ValueError) as excinfo:
        get_flow_run_logs_paginated("12345", "12345", -1)
    assert excinfo.value.args[0] == "limit must be positive"


def test_get_read_deployment_success():
    deployment_id = "test-deployment-id"
    mock_deployment_data = {
        "name": "test-deployment",
        "id": deployment_id,
        "tags": ["tag1", "tag2"],
        "schedule": {"cron": "* * * * *"},
        "parameters": {"config": []},
    }
    with patch("proxy.main.get_deployment") as mock_get_deployment:
        mock_get_deployment.return_value = mock_deployment_data
        response = get_read_deployment(deployment_id)
    assert response["deploymentId"] == deployment_id


def test_get_read_deployment_failure():
    with patch("proxy.main.get_deployment", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            deployment_id = "test-deployment-id"
            get_read_deployment(deployment_id)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to fetch deployment " + deployment_id


def test_get_read_deployment_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        get_read_deployment(None)
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_delete_deployment_success():
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.return_value = None
        response = delete_deployment("12345")
        assert response is None


def test_delete_deployment_failure():
    with patch("proxy.main.requests.delete") as mock_delete:
        mock_delete.return_value.raise_for_status.side_effect = Exception("test error")
        mock_delete.return_value.text = "test error"
        with pytest.raises(HTTPException) as excinfo:
            delete_deployment("12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "test error"


def test_delete_deployment_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        delete_deployment(None)
    assert excinfo.value.args[0] == "deployment_id must be a string"


@pytest.mark.asyncio
async def test_post_create_deployment_flow_run_success():
    with patch("proxy.main.post_deployment_flow_run", return_value="test result"):
        response = await post_create_deployment_flow_run("12345")
        assert response == "test result"


@pytest.mark.asyncio
async def test_post_create_deployment_flow_run_failure():
    with patch("proxy.main.post_deployment_flow_run", side_effect=Exception("test error")):
        with pytest.raises(HTTPException) as excinfo:
            await post_create_deployment_flow_run("12345")
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == "failed to create flow_run for deployment"


@pytest.mark.asyncio
async def test_post_create_deployment_flow_run_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        await post_create_deployment_flow_run(None)
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_post_deployment_set_schedule_success():
    with patch("proxy.main.set_deployment_schedule"):
        response = post_deployment_set_schedule("12345", "active")
        assert response == {"success": 1}


def test_post_deployment_set_schedule_failure():
    with pytest.raises(HTTPException) as excinfo:
        post_deployment_set_schedule("12345", "invalid_status")
    assert excinfo.value.status_code == 422
    assert excinfo.value.detail == "incorrect status value"


def test_post_deployment_set_schedule_invalid_deployment_id():
    with pytest.raises(TypeError) as excinfo:
        post_deployment_set_schedule(None, "active")
    assert excinfo.value.args[0] == "deployment_id must be a string"


def test_post_deployment_set_schedule_invalid_status():
    with pytest.raises(TypeError) as excinfo:
        post_deployment_set_schedule("12345", None)
    assert excinfo.value.args[0] == "status must be a string"


@pytest.mark.asyncio
@patch("proxy.main.post_deployment_v1")
async def test_post_dataflow_v1_success(mock_post_deployment_v1: AsyncMock):
    """tests post_dataflow_v1"""
    payload = DeploymentCreate2(
        flow_name="", deployment_name="", org_slug="org", deployment_params={}
    )

    mock_post_deployment_v1.return_value = {"id": "12345"}

    result = post_dataflow_v1(payload)
    assert result == {"deployment": {"id": "12345"}}

