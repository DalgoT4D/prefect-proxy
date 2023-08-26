import asyncio
import tempfile
from unittest.mock import AsyncMock, patch, Mock

import pytest
import requests
from fastapi import HTTPException

from proxy.exception import PrefectException
from proxy.schemas import (
    AirbyteConnectionCreate,
    AirbyteServerCreate,
    DbtCoreCreate,
    DbtProfileCreate,
    PrefectShellSetup,
    DeploymentCreate,
    DeploymentUpdate,
    PrefectSecretBlockCreate,
)
from proxy.service import (
    _create_dbt_cli_profile,
    create_airbyte_connection_block,
    create_airbyte_server_block,
    create_dbt_core_block,
    create_shell_block,
    delete_airbyte_connection_block,
    delete_airbyte_server_block,
    delete_dbt_core_block,
    delete_shell_block,
    get_airbyte_connection_block,
    get_airbyte_connection_block_id,
    get_airbyte_server_block_id,
    get_dbtcore_block_id,
    get_deployments_by_filter,
    get_flow_run,
    get_flow_run_logs,
    get_flow_runs_by_deployment_id,
    get_flow_runs_by_name,
    get_shell_block_id,
    parse_log,
    prefect_delete,
    prefect_get,
    prefect_post,
    prefect_patch,
    set_deployment_schedule,
    traverse_flow_run_graph,
    post_filter_blocks,
    update_airbyte_server_block,
    update_airbyte_connection_block,
    update_postgres_credentials,
    update_bigquery_credentials,
    update_target_configs_schema,
    post_deployment,
    put_deployment,
    get_deployment,
    CronSchedule,
    post_deployment_flow_run,
    create_secret_block,
)


def test_prefect_post_invalid_endpoint():
    with pytest.raises(TypeError) as excinfo:
        prefect_post(123, {})
    assert str(excinfo.value) == "endpoint must be a string"


def test_prefect_post_invalid_payload():
    with pytest.raises(TypeError) as excinfo:
        prefect_post("test_endpoint", "invalid_payload")
    assert str(excinfo.value) == "payload must be a dictionary"


@patch("requests.post")
@patch("os.getenv")
def test_prefect_post_success(mock_getenv, mock_post):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response._content = b'{"key": "value"}'
    mock_post.return_value = mock_response

    endpoint = "test_endpoint"
    payload = {"test_key": "test_value"}
    response = prefect_post(endpoint, payload)

    assert response == {"key": "value"}
    mock_post.assert_called_once_with(
        "http://localhost/test_endpoint", timeout=30, json=payload
    )


@patch("requests.post")
@patch("os.getenv")
def test_prefect_post_failure(mock_getenv, mock_post):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 400
    mock_response._content = b"Invalid input data"
    mock_post.return_value = mock_response

    endpoint = "test_endpoint"
    payload = {"test_key": "test_value"}

    with pytest.raises(HTTPException) as excinfo:
        prefect_post(endpoint, payload)

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Invalid input data"


def test_prefect_patch_invalid_endpoint():
    with pytest.raises(TypeError) as excinfo:
        prefect_patch(123, {})
    assert str(excinfo.value) == "endpoint must be a string"


def test_prefect_patch_invalid_payload():
    with pytest.raises(TypeError) as excinfo:
        prefect_patch("test_endpoint", "invalid_payload")
    assert str(excinfo.value) == "payload must be a dictionary"


@patch("requests.patch")
@patch("os.getenv")
def test_prefect_patch_success(mock_getenv, mock_patch):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response._content = b'{"key": "value"}'
    mock_patch.return_value = mock_response

    endpoint = "test_endpoint"
    payload = {"test_key": "test_value"}
    response = prefect_patch(endpoint, payload)

    assert response == {"key": "value"}
    mock_patch.assert_called_once_with(
        "http://localhost/test_endpoint", timeout=30, json=payload
    )


@patch("requests.patch")
@patch("os.getenv")
def test_prefect_patch_success_204(mock_getenv, mock_patch):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 204
    mock_patch.return_value = mock_response

    endpoint = "test_endpoint"
    payload = {"test_key": "test_value"}
    response = prefect_patch(endpoint, payload)

    assert response == {}
    mock_patch.assert_called_once_with(
        "http://localhost/test_endpoint", timeout=30, json=payload
    )


@patch("requests.patch")
@patch("os.getenv")
def test_prefect_patch_failure(mock_getenv, mock_patch):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 400
    mock_response._content = b"Invalid input data"
    mock_patch.return_value = mock_response

    endpoint = "test_endpoint"
    payload = {"test_key": "test_value"}

    with pytest.raises(HTTPException) as excinfo:
        prefect_patch(endpoint, payload)

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Invalid input data"


def test_prefect_get_invalid_endpoint():
    with pytest.raises(TypeError) as excinfo:
        prefect_get(123)
    assert str(excinfo.value) == "endpoint must be a string"


@patch("requests.get")
@patch("os.getenv")
def test_prefect_get_failure(mock_getenv, mock_get):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 400
    mock_response._content = b"Invalid request"
    mock_get.return_value = mock_response

    endpoint = "test_endpoint"

    with pytest.raises(HTTPException) as excinfo:
        prefect_get(endpoint)

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Invalid request"


@patch("requests.get")
@patch("os.getenv")
def test_prefect_get_success(mock_getenv, mock_get):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response._content = b'{"key": "value"}'
    mock_get.return_value = mock_response

    endpoint = "test_endpoint"
    response = prefect_get(endpoint)

    assert response == {"key": "value"}
    mock_get.assert_called_once_with("http://localhost/test_endpoint", timeout=30)


def test_prefect_delete_invalid_endpoint():
    with pytest.raises(TypeError) as excinfo:
        prefect_delete(123)
    assert str(excinfo.value) == "endpoint must be a string"


@patch("requests.delete")
@patch("os.getenv")
def test_prefect_delete_failure(mock_getenv, mock_delete):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 400
    mock_response._content = b"Invalid request"
    mock_delete.return_value = mock_response

    endpoint = "test_endpoint"

    with pytest.raises(HTTPException) as excinfo:
        prefect_delete(endpoint)

    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Invalid request"


@patch("requests.delete")
def test_prefect_delete_success(mock_delete):
    mock_response = requests.Response()
    mock_response.status_code = 200
    mock_response._content = b'{"key": "value"}'
    mock_delete.return_value = mock_response

    endpoint = "test_endpoint"
    response = prefect_delete(endpoint)

    assert response == {"key": "value"}


@patch("requests.delete")
def test_prefect_delete_success_204(mock_delete):
    mock_response = requests.Response()
    mock_response.status_code = 204
    mock_delete.return_value = mock_response

    endpoint = "test_endpoint"
    response = prefect_delete(endpoint)

    assert response == {}


@patch("proxy.service.prefect_post")
def test_post_filter_blocks_failure(mock_prefect_post):
    block_names = ["block_one", "block_two"]
    mock_prefect_post.side_effect = PrefectException("failed to filter blocks")
    with pytest.raises(PrefectException) as excinfo:
        post_filter_blocks(block_names)

    assert str(excinfo.value) == "failed to filter blocks"


@patch("proxy.service.prefect_post")
def test_post_filter_blocks_success(mock_prefect_post):
    block_names = ["block_one", "block_two"]
    post_filter_blocks(block_names)
    mock_prefect_post.assert_called_once_with(
        "block_documents/filter",
        {
            "block_documents": {
                "operator": "and_",
                "name": {"any_": block_names},
            }
        },
    )


class MockBlock:
    def dict(self):
        return {"_block_document_id": "expected_block_id"}


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer.load", new_callable=AsyncMock)
async def test_get_airbyte_server_block_id_valid_blockname(mock_load):
    mock_load.return_value = MockBlock()
    blockname = "valid_blockname"
    result = await get_airbyte_server_block_id(blockname)
    assert result == "expected_block_id"
    mock_load.assert_called_once_with(blockname)


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer.load", new_callable=AsyncMock)
async def test_get_airbyte_server_block_id_invalid_blockname(mock_load):
    mock_load.side_effect = ValueError(
        "no airbyte server block named invalid_blockname"
    )
    blockname = "invalid_blockname"
    result = await get_airbyte_server_block_id(blockname)
    assert result is None
    mock_load.assert_called_once_with(blockname)


@pytest.mark.asyncio
async def test_get_airbyte_server_block_id_non_string_blockname():
    with pytest.raises(TypeError):
        await get_airbyte_server_block_id(123)


class MockAirbyteServer:
    def __init__(self, server_host, server_port, api_version):
        pass

    async def save(self, block_name):
        pass

    def dict(self):
        return {"_block_document_id": "expected_block_id"}


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer", new=MockAirbyteServer)
async def test_create_airbyte_server_block():
    payload = AirbyteServerCreate(
        blockName="test_block",
        serverHost="test_host",
        serverPort=1234,
        apiVersion="test_version",
    )
    result = await create_airbyte_server_block(payload)
    assert result == ("expected_block_id", "testblock")


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer", new=MockAirbyteServer)
async def test_create_airbyte_server_block_failure():
    payload = AirbyteServerCreate(
        blockName="test_block",
        serverHost="test_host",
        serverPort=1234,
        apiVersion="test_version",
    )
    with patch("proxy.service.AirbyteServer.save", new_callable=AsyncMock) as mock_save:
        mock_save.side_effect = Exception("failed to create airbyte server block")
        with pytest.raises(Exception) as excinfo:
            await create_airbyte_server_block(payload)
        assert str(excinfo.value) == "failed to create airbyte server block"


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer", new=MockAirbyteServer)
async def test_create_airbyte_server_block_invalid_payload():
    payload = "invalid_payload"
    with pytest.raises(TypeError) as excinfo:
        await create_airbyte_server_block(payload)
    assert str(excinfo.value) == "payload must be an AirbyteServerCreate"


@patch("proxy.service.prefect_delete")
def test_delete_airbyte_server_block(mock_prefect_delete):
    blockid = "test_blockid"
    delete_airbyte_server_block(blockid)
    mock_prefect_delete.assert_called_once_with(f"block_documents/{blockid}")


def test_delete_airbyte_server_block_invalid_blockid():
    blockid = 1234
    with pytest.raises(TypeError) as excinfo:
        delete_airbyte_server_block(blockid)
    assert str(excinfo.value) == "blockid must be a string"


# =================================================================================================
def test_update_airbyte_server_block_must_be_string():
    with pytest.raises(TypeError) as excinfo:
        update_airbyte_server_block(123)
    assert str(excinfo.value) == "blockname must be a string"


def test_update_airbyte_server_block_not_implemented():
    with pytest.raises(PrefectException) as excinfo:
        update_airbyte_server_block("blockname")
    assert str(excinfo.value) == "not implemented"


# =================================================================================================


@pytest.mark.asyncio
@patch("proxy.service.AirbyteConnection.load", new_callable=AsyncMock)
async def test_get_airbyte_connection_block_id_valid_blockname(mock_load):
    class MockBlock:
        def dict(self):
            return {"_block_document_id": "expected_block_id"}

    mock_load.return_value = MockBlock()
    blockname = "valid_blockname"
    result = await get_airbyte_connection_block_id(blockname)
    assert result == "expected_block_id"
    mock_load.assert_called_once_with(blockname)


@pytest.mark.asyncio
@patch("proxy.service.AirbyteConnection.load", new_callable=AsyncMock)
async def test_get_airbyte_connection_block_id_invalid_blockname(mock_load):
    mock_load.side_effect = ValueError(
        "no airbyte connection block named invalid_blockname"
    )
    blockname = "invalid_blockname"
    with pytest.raises(HTTPException) as excinfo:
        await get_airbyte_connection_block_id(blockname)
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == f"No airbyte connection block named {blockname}"
    mock_load.assert_called_once_with(blockname)


@pytest.mark.asyncio
async def test_get_airbyte_connection_block_id_non_string_blockname():
    blockname = 1234
    with pytest.raises(TypeError) as excinfo:
        await get_airbyte_connection_block_id(blockname)
    assert str(excinfo.value) == "blockname must be a string"


# =================================================================================================


@pytest.mark.asyncio
async def test_get_airbyte_connection_block_id_non_string_blockid():
    blockid = 1234
    with pytest.raises(TypeError) as excinfo:
        await get_airbyte_connection_block(blockid)
    assert str(excinfo.value) == "blockid must be a string"


@pytest.mark.asyncio
@patch("proxy.service.prefect_get")
async def test_get_airbyte_connection_block_valid_blockid(mock_prefect_get):
    mock_prefect_get.return_value = {"key": "value"}
    blockid = "valid_blockid"
    result = await get_airbyte_connection_block(blockid)
    assert result == {"key": "value"}
    mock_prefect_get.assert_called_once_with(f"block_documents/{blockid}")


@pytest.mark.asyncio
@patch("proxy.service.prefect_get")
async def test_get_airbyte_connection_block_invalid_blockid(mock_prefect_get):
    mock_prefect_get.side_effect = requests.exceptions.HTTPError()
    blockid = "invalid_blockid"
    with pytest.raises(HTTPException) as excinfo:
        await get_airbyte_connection_block(blockid)
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == f"No airbyte connection block having id {blockid}"
    mock_prefect_get.assert_called_once_with(f"block_documents/{blockid}")


# =================================================================================================


class MockAirbyteServer:
    def __init__(self, server_host, server_port, api_version):
        pass

    async def save(self, block_name):
        pass

    def dict(self):
        return {"_block_document_id": "expected_server_block_id"}


class MockAirbyteConnection:
    def __init__(self, airbyte_server, connection_id, timeout):
        self.airbyte_server = airbyte_server
        self.connection_id = connection_id
        self.timeout = timeout

    async def save(self, block_name):
        if self.connection_id == "test_error_connection_id":
            raise Exception("test error")

    def dict(self):
        return {"_block_document_id": "expected_connection_block_id"}


@pytest.mark.asyncio
@patch("proxy.service.AirbyteConnection", new=MockAirbyteConnection)
@patch("proxy.service.AirbyteServer.load", new_callable=AsyncMock)
async def test_create_airbyte_connection_block(mock_load):
    mock_load.return_value = MockAirbyteServer(None, None, None)
    conninfo = AirbyteConnectionCreate(
        serverBlockName="test_server_block",
        connectionBlockName="test_connection_block",
        connectionId="test_connection_id",
    )
    result = await create_airbyte_connection_block(conninfo)
    assert result == "expected_connection_block_id"
    mock_load.assert_called_once_with("test_server_block")


@pytest.mark.asyncio
@patch("proxy.service.AirbyteConnection", new=MockAirbyteConnection)
@patch("proxy.service.AirbyteServer.load", new_callable=AsyncMock)
async def test_create_airbyte_connection_block_save_error(mock_load):
    mock_load.return_value = MockAirbyteServer(None, None, None)
    conninfo = AirbyteConnectionCreate(
        serverBlockName="test_server_block",
        connectionBlockName="test_connection_block",
        connectionId="test_error_connection_id",
    )
    with pytest.raises(PrefectException) as excinfo:
        await create_airbyte_connection_block(conninfo)
    assert (
        str(excinfo.value)
        == f"failed to create airbyte connection block for connection {conninfo.connectionId}"
    )


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer.load", new_callable=AsyncMock)
async def test_create_airbyte_connection_block_invalid_server_block(mock_load):
    mock_load.side_effect = ValueError(
        "no airbyte server block named invalid_server_block"
    )
    conninfo = AirbyteConnectionCreate(
        serverBlockName="invalid_server_block",
        connectionBlockName="test_connection_block",
        connectionId="test_connection_id",
    )
    with pytest.raises(PrefectException) as excinfo:
        await create_airbyte_connection_block(conninfo)
    assert (
        str(excinfo.value)
        == "could not find Airbyte Server block named invalid_server_block"
    )
    mock_load.assert_called_once_with("invalid_server_block")


@pytest.mark.asyncio
async def test_create_airbyte_connection_block_invalid_conninfo():
    conninfo = "invalid_conninfo"
    with pytest.raises(TypeError) as excinfo:
        await create_airbyte_connection_block(conninfo)
    assert str(excinfo.value) == "conninfo must be an AirbyteConnectionCreate"


# =================================================================================================
# =================================================================================================
def test_update_airbyte_connection_block_must_be_string():
    with pytest.raises(TypeError) as excinfo:
        update_airbyte_connection_block(123)
    assert str(excinfo.value) == "blockname must be a string"


def test_update_airbyte_connection_block_not_implemented():
    with pytest.raises(PrefectException) as excinfo:
        update_airbyte_connection_block("blockname")
    assert str(excinfo.value) == "not implemented"


# =================================================================================================


@patch("proxy.service.prefect_delete")
def test_delete_airbyte_connection_block(mock_prefect_delete):
    blockid = "test_blockid"
    delete_airbyte_connection_block(blockid)
    mock_prefect_delete.assert_called_once_with(f"block_documents/{blockid}")


def test_delete_airbyte_connection_block_non_string_blockid():
    blockid = 1234
    with pytest.raises(TypeError) as excinfo:
        delete_airbyte_connection_block(blockid)
    assert str(excinfo.value) == "blockid must be a string"


# =================================================================================================


class MockShellOperation:
    def __init__(self, commands, env, working_dir):
        pass

    async def save(self, block_name, overwrite=False):
        pass

    def dict(self):
        return {"_block_document_id": "expected_block_id"}


@pytest.mark.asyncio
@patch("proxy.service.ShellOperation.load", new_callable=AsyncMock)
async def test_get_shell_block_id_valid_blockname(mock_load):
    mock_load.return_value = MockShellOperation(None, None, None)
    blockname = "valid_blockname"
    result = await get_shell_block_id(blockname)
    assert result == "expected_block_id"
    mock_load.assert_called_once_with(blockname)


@pytest.mark.asyncio
@patch("proxy.service.ShellOperation.load", new_callable=AsyncMock)
async def test_get_shell_block_id_invalid_blockname(mock_load):
    mock_load.side_effect = ValueError(
        "no shell operation block named invalid_blockname"
    )
    blockname = "invalid_blockname"
    with pytest.raises(HTTPException) as excinfo:
        await get_shell_block_id(blockname)
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == f"No shell operation block named {blockname}"
    mock_load.assert_called_once_with(blockname)


@pytest.mark.asyncio
async def test_get_shell_block_id_invalid_blockname_type():
    blockname = 123
    with pytest.raises(TypeError) as excinfo:
        await get_shell_block_id(blockname)
    assert str(excinfo.value) == "blockname must be a string"


@pytest.mark.asyncio
@patch("proxy.service.ShellOperation", new=MockShellOperation)
@patch("proxy.service.ShellOperation.load", new_callable=AsyncMock)
async def test_create_shell_block(mock_load):
    mock_load.return_value = MockShellOperation(None, None, None)
    shell = PrefectShellSetup(
        blockName="test_block_name",
        commands=["test_command"],
        env={"test_key": "test_value"},
        workingDir="test_working_dir",
    )
    result = await create_shell_block(shell)
    assert result[0] == "expected_block_id"


@pytest.mark.asyncio
async def test_create_shell_block_invalid_shell():
    shell = "invalid_shell"
    with pytest.raises(TypeError) as excinfo:
        await create_shell_block(shell)
    assert str(excinfo.value) == "shell must be a PrefectShellSetup"


@pytest.mark.asyncio
# @patch("proxy.service.ShellOperation", new=MockShellOperation)
@patch("proxy.service.ShellOperation.save", new_callable=AsyncMock)
async def test_create_shell_block_failure(mock_save):
    mock_save.side_effect = Exception("save failed")

    shell = PrefectShellSetup(
        blockName="test_block_name",
        commands=["test_command"],
        env={"test_key": "test_value"},
        workingDir="/tmp",
    )

    with pytest.raises(PrefectException) as excinfo:
        await create_shell_block(shell)
    assert str(excinfo.value) == "failed to create shell block"


@patch("proxy.service.prefect_delete")
def test_delete_shell_block(mock_prefect_delete):
    blockid = "test_blockid"
    delete_shell_block(blockid)
    mock_prefect_delete.assert_called_once_with(f"block_documents/{blockid}")


def test_delete_shell_block_non_string_blockid():
    blockid = 1234
    with pytest.raises(TypeError) as excinfo:
        delete_shell_block(blockid)
    assert str(excinfo.value) == "blockid must be a string"


# =================================================================================================


class MockBlock:
    def dict(self):
        return {"_block_document_id": "expected_block_id"}


@pytest.mark.asyncio
async def test_get_dbtcore_block_id_success():
    mock_block = MockBlock()

    with patch(
        "proxy.service.DbtCoreOperation.load", new_callable=AsyncMock
    ) as mock_load:
        mock_load.return_value = mock_block
        result = await get_dbtcore_block_id("test_block_name")
        assert result == "expected_block_id"


@pytest.mark.asyncio
@patch("proxy.service.DbtCoreOperation.load", new_callable=AsyncMock)
async def test_get_dbtcore_block_id_failure(mock_load):
    mock_load.side_effect = ValueError("load failed")

    with pytest.raises(HTTPException) as excinfo:
        await get_dbtcore_block_id("test_block_name")
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == "No dbt core operation block named test_block_name"


@pytest.mark.asyncio
async def test_get_dbtcore_block_id_invalid_blockname():
    with pytest.raises(TypeError) as excinfo:
        await get_dbtcore_block_id(123)
    assert str(excinfo.value) == "blockname must be a string"


# @pytest.mark.asyncio
# @patch("proxy.service.DbtCliProfile.save", new_callable=AsyncMock)
# async def test_create_dbt_cli_profile(mock_save):
#     payload = DbtCoreCreate(
#         blockName="test_block_name",
#         profile=DbtProfileCreate(
#             name="test_name",
#             target_configs_schema="test_outputs_path",
#         ),
#         wtype="postgres",
#         credentials={
#             "username": "test_username",
#             "password": "test_password",
#             "database": "test_database",
#             "host": "test_host",
#             "port": "test_port",
#         },
#         commands=["test_command"],
#         env={"test_key": "test_value"},
#         working_dir="test_working_dir",
#         profiles_dir="test_profiles_dir",
#         project_dir="test_project_dir",
#     )

#     result = await _create_dbt_cli_profile(payload)

#     assert result.name == payload.profile.name
#     assert result.target == payload.profile.target_configs_schema


@pytest.mark.asyncio
async def test_create_dbt_cli_profile_failure():
    # Create a DbtCoreCreate object with an invalid wtype value
    payload = DbtCoreCreate(
        blockName="test_block_name",
        profile=DbtProfileCreate(
            name="test_name",
            target_configs_schema="test_outputs_path",
        ),
        wtype="invalid_wtype",  # Use an invalid wtype value
        credentials={
            "username": "test_username",
            "password": "test_password",
            "database": "test_database",
            "host": "test_host",
            "port": "test_port",
        },
        commands=["test_command"],
        env={"test_key": "test_value"},
        working_dir="test_working_dir",
        profiles_dir="test_profiles_dir",
        project_dir="test_project_dir",
    )

    # Call the function with the payload and assert that it raises a PrefectException
    with pytest.raises(PrefectException) as excinfo:
        await _create_dbt_cli_profile(payload)

    # Assert that the exception message is as expected
    assert str(excinfo.value) == "unknown wtype: invalid_wtype"


@pytest.mark.asyncio
async def test_create_dbt_cli_profile_with_invalid_payload():
    payload = "invalid_payload"

    with pytest.raises(TypeError) as excinfo:
        await _create_dbt_cli_profile(payload)

    assert str(excinfo.value) == "payload must be a DbtCoreCreate"


@pytest.mark.asyncio
@patch("proxy.service.DbtCliProfile.save", new_callable=AsyncMock)
async def test_create_dbt_cli_profile_exception(mock_save):
    mock_save.side_effect = Exception("test exception")

    payload = DbtCoreCreate(
        blockName="test_block_name",
        profile=DbtProfileCreate(
            name="test_name",
            target_configs_schema="test_outputs_path",
        ),
        wtype="postgres",
        credentials={
            "username": "test_username",
            "password": "test_password",
            "database": "test_database",
            "host": "test_host",
            "port": "test_port",
        },
        commands=["test_command"],
        env={"test_key": "test_value"},
        working_dir="test_working_dir",
        profiles_dir="test_profiles_dir",
        project_dir="test_project_dir",
    )

    with pytest.raises(PrefectException) as excinfo:
        await _create_dbt_cli_profile(payload)

    assert str(excinfo.value) == "failed to create dbt cli profile"


@pytest.mark.asyncio
@patch("proxy.service.DbtCliProfile.save", new_callable=AsyncMock)
@patch("proxy.service.DbtCoreOperation.__init__", return_value=None)
@patch("proxy.service.DbtCoreOperation.save", new_callable=AsyncMock)
@patch(
    "proxy.service._block_id", return_value=("test_block_id", "test_cleaned_blockname")
)
async def test_create_dbt_core_block_success(
    mock_block_id, mock_dbtcoreoperation_save, mock_dbtcoreoperation_init, mock_save
):
    with tempfile.TemporaryDirectory() as tempdir:
        payload = DbtCoreCreate(
            blockName="test_block_name",
            profile=DbtProfileCreate(
                name="test_name",
                target_configs_schema="test_outputs_path",
            ),
            wtype="postgres",
            credentials={
                "username": "test_username",
                "password": "test_password",
                "database": "test_database",
                "host": "test_host",
                "port": "test_port",
            },
            commands=["run"],
            env={"test_key": "test_value"},
            working_dir=tempdir,
            profiles_dir="test_profiles_dir",
            project_dir="test_project_dir",
        )

        result = await create_dbt_core_block(payload)

        assert result == (("test_block_id", "test_cleaned_blockname"), "testblockname")


@pytest.mark.asyncio
async def test_create_dbt_core_block_failure():
    payload = "invalid_payload"

    with pytest.raises(TypeError) as excinfo:
        await create_dbt_core_block(payload)

    assert str(excinfo.value) == "payload must be a DbtCoreCreate"


@pytest.mark.asyncio
@patch("proxy.service.DbtCliProfile.save", new_callable=AsyncMock)
@patch("proxy.service.DbtCoreOperation.__init__", return_value=None)
@patch("proxy.service.DbtCoreOperation.save", side_effect=Exception("Test error"))
@patch(
    "proxy.service._block_id", return_value=("test_block_id", "test_cleaned_blockname")
)
async def test_create_dbt_core_block_exception(
    mock_block_id, mock_dbtcoreoperation_save, mock_dbtcoreoperation_init, mock_save
):
    with tempfile.TemporaryDirectory() as tempdir:
        payload = DbtCoreCreate(
            blockName="test_block_name",
            profile=DbtProfileCreate(
                name="test_name",
                target_configs_schema="test_outputs_path",
            ),
            wtype="postgres",
            credentials={
                "username": "test_username",
                "password": "test_password",
                "database": "test_database",
                "host": "test_host",
                "port": "test_port",
            },
            commands=["run"],
            env={"test_key": "test_value"},
            working_dir=tempdir,
            profiles_dir="test_profiles_dir",
            project_dir="test_project_dir",
        )

        with pytest.raises(PrefectException) as exc_info:
            await create_dbt_core_block(payload)

        assert str(exc_info.value) == "failed to create dbt core op block"


@pytest.mark.asyncio
@patch("proxy.service.prefect_delete")
async def test_delete_dbt_core_block_success(mock_prefect_delete):
    block_id = "test_block_id"
    mock_prefect_delete.return_value = "Deletion successful"

    # Simulate asynchronous behavior by using asyncio.sleep
    await asyncio.sleep(0)

    result = delete_dbt_core_block(block_id)

    assert result == "Deletion successful"
    mock_prefect_delete.assert_called_once_with("block_documents/test_block_id")


@pytest.mark.asyncio
async def test_delete_dbt_core_block_type_error():
    block_id = 123

    with pytest.raises(TypeError) as exc_info:
        await delete_dbt_core_block(block_id)

    assert str(exc_info.value) == "block_id must be a string"


@pytest.mark.asyncio
@patch("proxy.service.Secret.save", new_callable=AsyncMock)
async def test_create_secret_block(mock_save: AsyncMock):
    mock_save.side_effect = Exception("exception thrown")
    payload = PrefectSecretBlockCreate(secret="my-secret", blockName="my-blockname")
    with pytest.raises(PrefectException) as excinfo:
        await create_secret_block(payload)
    assert str(excinfo.value) == "Could not create a secret block"


@pytest.mark.asyncio
@patch(
    "proxy.service.DbtCoreOperation.load",
    AsyncMock(side_effect=Exception()),
)
async def test_update_postgres_credentials_wrong_name():
    with pytest.raises(PrefectException) as excinfo:
        await update_postgres_credentials("dne", {})
    assert str(excinfo.value) == "no dbt core op block named dne"


@pytest.mark.asyncio
@patch(
    "proxy.service.DbtCoreOperation.load",
    AsyncMock(
        return_value=Mock(
            dbt_cli_profile=Mock(target_configs=Mock(type="not-postgres"))
        )
    ),
)
async def test_update_postgres_credentials_wrong_blocktype():
    with pytest.raises(TypeError) as excinfo:
        await update_postgres_credentials("blockname", {})
    assert str(excinfo.value) == "wrong blocktype"


@pytest.mark.asyncio
@patch("proxy.service.DbtCoreOperation.load", new_callable=AsyncMock)
async def test_update_postgres_credentials_success(mock_load):
    dbt_coreop_block = Mock(
        dbt_cli_profile=Mock(
            target_configs=Mock(
                type="postgres",
                dict=Mock(
                    return_value={
                        "extras": {
                            "host": "old_host",
                            "database": "old_database",
                            "user": "old_user",
                            "password": "old_password",
                        },
                        "schema": "old_schema",
                    }
                ),
            ),
            save=AsyncMock(),
        ),
        save=AsyncMock(),
    )
    dbt_coreop_block.dbt_cli_profile.name = "block-name"
    mock_load.return_value = dbt_coreop_block

    await update_postgres_credentials(
        "block-name", {"host": "new_host", "dbname": "new_database"}
    )

    dbt_coreop_block.dbt_cli_profile.save.assert_called_once_with(
        name="block-name", overwrite=True
    )
    dbt_coreop_block.save.assert_called_once_with("block-name", overwrite=True)

    assert dbt_coreop_block.dbt_cli_profile.target_configs.type == "postgres"
    assert dbt_coreop_block.dbt_cli_profile.target_configs.extras == {
        "host": "new_host",
        "database": "new_database",
        "user": "old_user",
        "password": "old_password",
    }


@pytest.mark.asyncio
@patch(
    "proxy.service.DbtCoreOperation.load",
    AsyncMock(side_effect=Exception()),
)
async def test_update_bigquery_credentials_wrong_name():
    with pytest.raises(PrefectException) as excinfo:
        await update_bigquery_credentials("dne", {})
    assert str(excinfo.value) == "no dbt core op block named dne"


@pytest.mark.asyncio
@patch(
    "proxy.service.DbtCoreOperation.load",
    AsyncMock(
        return_value=Mock(
            dbt_cli_profile=Mock(target_configs=Mock(type="not-bigquery"))
        )
    ),
)
async def test_update_bigquery_credentials_wrong_blocktype():
    with pytest.raises(TypeError) as excinfo:
        await update_bigquery_credentials("blockname", {})
    assert str(excinfo.value) == "wrong blocktype"


@pytest.mark.asyncio
@patch("proxy.service.DbtCoreOperation.load", new_callable=AsyncMock)
@patch("proxy.service.GcpCredentials", Mock(return_value={}))
@patch("proxy.service.BigQueryTargetConfigs", Mock())
async def test_update_bigquery_credentials_success(mock_load):
    dbt_coreop_block = Mock(
        dbt_cli_profile=Mock(
            target_configs=Mock(
                type="bigquery",
                dict=Mock(
                    return_value={
                        "extras": {},
                        "schema_": "old_schema",
                    }
                ),
            ),
            save=AsyncMock(),
        ),
        save=AsyncMock(),
    )
    dbt_coreop_block.dbt_cli_profile.name = "block-name"
    mock_load.return_value = dbt_coreop_block

    await update_bigquery_credentials("block-name", {})

    dbt_coreop_block.dbt_cli_profile.save.assert_called_once_with(
        name="block-name", overwrite=True
    )
    dbt_coreop_block.save.assert_called_once_with("block-name", overwrite=True)


@pytest.mark.asyncio
@patch(
    "proxy.service.DbtCoreOperation.load",
    AsyncMock(side_effect=Exception()),
)
async def test_update_target_configs_schema_no_block_named():
    with pytest.raises(PrefectException) as excinfo:
        await update_target_configs_schema("dne", {})
    assert str(excinfo.value) == "no dbt core op block named dne"


@pytest.mark.asyncio
@patch("proxy.service.DbtCoreOperation.load", new_callable=AsyncMock)
async def test_update_target_configs_schema(mock_load):
    dbt_coreop_block = Mock(
        dbt_cli_profile=Mock(
            target_configs=Mock(schema="oldtarget"),
            target="oldtarget",
            save=AsyncMock(),
        ),
        commands=["dbt run --target oldtarget"],
        save=AsyncMock(),
    )
    dbt_coreop_block.dbt_cli_profile.name = "block-name"
    mock_load.return_value = dbt_coreop_block

    await update_target_configs_schema("block-name", "newtarget")

    dbt_coreop_block.dbt_cli_profile.save.assert_called_once_with(
        name="block-name", overwrite=True
    )
    dbt_coreop_block.save.assert_called_once_with("block-name", overwrite=True)

    assert dbt_coreop_block.dbt_cli_profile.target_configs.schema == "newtarget"
    assert dbt_coreop_block.dbt_cli_profile.target == "newtarget"
    assert dbt_coreop_block.commands[0] == "dbt run --target newtarget"


@pytest.mark.asyncio
async def test_post_deployment_bad_payload():
    with pytest.raises(TypeError) as excinfo:
        await post_deployment(123)
    assert str(excinfo.value) == "payload must be a DeploymentCreate"


@pytest.mark.asyncio
@patch("proxy.service.Deployment.build_from_flow", new_callable=AsyncMock)
@patch(
    "proxy.service.deployment_schedule_flow_v2",
    new_callable=Mock,
)
async def test_post_deployment(deployment_schedule_flow_v2, mock_build):
    payload = DeploymentCreate(
        flow_name="flow-name",
        deployment_name="deployment-name",
        org_slug="org-slug",
        connection_blocks=[],
        dbt_blocks=[],
        cron=None,
    )
    deployment = Mock(
        apply=AsyncMock(return_value="deployment-id"),
    )
    deployment.name = "deployment-name"

    mock_build.return_value = deployment
    deployment_schedule_flow_v2.with_options = Mock(return_value="dsf")

    response = await post_deployment(payload)
    assert response["id"] == "deployment-id"
    assert response["name"] == "deployment-name"
    mock_build.assert_called_once_with(
        flow="dsf",
        name=payload.deployment_name,
        work_queue_name="ddp",
        tags=[payload.org_slug],
    )


def test_put_deployment_bad_param():
    payload = 123
    with pytest.raises(TypeError) as excinfo:
        put_deployment("deployment-id", payload)
    assert str(excinfo.value) == "payload must be a DeploymentUpdate"


@patch("proxy.service.prefect_patch")
def test_put_deployment(mock_patch: Mock):
    payload = DeploymentUpdate(cron="* * * * *")
    mock_patch.return_value = "retval"
    response = put_deployment("deployment-id", payload)
    mock_patch.assert_called_once_with(
        f"deployments/deployment-id",
        {"schedule": CronSchedule(cron="* * * * *").dict()},
    )
    assert response == "retval"


def test_get_deployment_bad_param():
    with pytest.raises(TypeError) as excinfo:
        get_deployment(123)
    assert str(excinfo.value) == "deployment_id must be a string"


@patch("proxy.service.prefect_get")
def test_put_deployment(mock_get: Mock):
    mock_get.return_value = "retval"
    response = get_deployment("deployment-id")
    mock_get.assert_called_once_with(f"deployments/deployment-id")
    assert response == "retval"


def test_get_flow_runs_by_deployment_id_type_error():
    with pytest.raises(TypeError):
        get_flow_runs_by_deployment_id(123, 10)
    with pytest.raises(TypeError):
        get_flow_runs_by_deployment_id("deployment_id", "invalid limit")


def test_get_flow_runs_by_deployment_id_value_error():
    with pytest.raises(ValueError):
        get_flow_runs_by_deployment_id("deployment_id", -1)


def test_get_flow_runs_by_deployment_id_prefect_post():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        deployment_id = "deployment_id"
        limit = 10
        get_flow_runs_by_deployment_id(deployment_id, limit)
        query = {
            "sort": "START_TIME_DESC",
            "deployments": {"id": {"any_": [deployment_id]}},
            "flow_runs": {
                "operator": "and_",
                "state": {"type": {"any_": ["COMPLETED", "FAILED"]}},
            },
            "limit": limit,
        }
        prefect_post_mock.assert_called_with("flow_runs/filter", query)


def test_get_flow_runs_by_deployment_id_result():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        flow_run = {
            "id": "flow_run_id",
            "name": "flow_run_name",
            "tags": ["tag1", "tag2"],
            "start_time": "2022-01-01T00:00:00Z",
            "expected_start_time": "2022-01-01T00:00:00Z",
            "total_run_time": 60,
            "state": {"type": "COMPLETED"},
        }
        prefect_post_mock.return_value = [flow_run]
        result = get_flow_runs_by_deployment_id("deployment_id", 10)
        assert result == [
            {
                "id": flow_run["id"],
                "name": flow_run["name"],
                "tags": flow_run["tags"],
                "startTime": flow_run["start_time"],
                "expectedStartTime": flow_run["expected_start_time"],
                "totalRunTime": flow_run["total_run_time"],
                "status": flow_run["state"]["type"],
            }
        ]


def test_get_flow_runs_by_deployment_id_exception():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        prefect_post_mock.side_effect = Exception("test error")
        with pytest.raises(PrefectException):
            get_flow_runs_by_deployment_id("deployment_id", 10)


def test_get_deployments_by_filter_type_error():
    with pytest.raises(TypeError):
        get_deployments_by_filter(123)
    with pytest.raises(TypeError):
        get_deployments_by_filter("org_slug", "invalid deployment_ids")


def test_get_deployments_by_filter_prefect_post():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        org_slug = "org_slug"
        deployment_ids = ["deployment1", "deployment2"]
        prefect_post_mock.return_value = [
            {
                "name": "name1",
                "id": "id1",
                "tags": "tags1",
                "schedule": {"cron": "cron1"},
                "is_schedule_active": True,
            }
        ]
        response = get_deployments_by_filter(org_slug, deployment_ids)
        query = {
            "deployments": {
                "operator": "and_",
                "tags": {"all_": [org_slug]},
                "id": {"any_": deployment_ids},
            }
        }
        prefect_post_mock.assert_called_with("deployments/filter", query)
        assert response == [
            {
                "name": "name1",
                "deploymentId": "id1",
                "tags": "tags1",
                "cron": "cron1",
                "isScheduleActive": True,
            }
        ]


@pytest.mark.asyncio
async def test_post_deployment_flow_run_badargs():
    with pytest.raises(TypeError) as excinfo:
        await post_deployment_flow_run(123)
    assert str(excinfo.value) == "deployment_id must be a string"


@pytest.mark.asyncio
@patch("proxy.service.run_deployment", new_callable=AsyncMock)
async def test_post_deployment_flow_run(mock_run_deployment: AsyncMock):
    mock_run_deployment.return_value = Mock(id="return-id")
    response = await post_deployment_flow_run("deployment-id")
    assert response["flow_run_id"] == "return-id"


@pytest.mark.asyncio
@patch("proxy.service.run_deployment", new_callable=AsyncMock)
async def test_post_deployment_flow_run_failed(mock_run_deployment: AsyncMock):
    mock_run_deployment.side_effect = Exception("exception")
    with pytest.raises(PrefectException) as excinfo:
        await post_deployment_flow_run("deployment-id")
    assert str(excinfo.value) == "Failed to create deployment flow run"


def test_parse_log_type_error():
    with pytest.raises(TypeError):
        parse_log("invalid log")


def test_parse_log_result():
    log = {
        "level": "INFO",
        "timestamp": "2022-01-01T00:00:00Z",
        "message": "test message",
    }
    result = parse_log(log)
    assert result == {
        "level": log["level"],
        "timestamp": log["timestamp"],
        "message": log["message"],
    }


def test_traverse_flow_run_graph_type_error():
    with pytest.raises(TypeError):
        traverse_flow_run_graph(123, [])
    with pytest.raises(TypeError):
        traverse_flow_run_graph("flow_run_id", "invalid flow_runs")


@patch("proxy.service.prefect_get")
def test_traverse_flow_run_graph_1(mock_get: Mock):
    mock_get.return_value = []
    response = traverse_flow_run_graph("4", ["1", "2", "3"])

    assert response == ["1", "2", "3", "4"]


def test_get_flow_run_logs_type_error():
    with pytest.raises(TypeError):
        get_flow_run_logs(123, 0)
    with pytest.raises(TypeError):
        get_flow_run_logs("flow_run_id", "invalid offset")


def test_get_flow_run_logs_prefect_post():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        with patch(
            "proxy.service.traverse_flow_run_graph"
        ) as traverse_flow_run_graph_mock:
            traverse_flow_run_graph_mock.return_value = ["flow_run_id"]
            flow_run_id = "flow_run_id"
            offset = 10
            get_flow_run_logs(flow_run_id, offset)
            query = {
                "logs": {
                    "operator": "and_",
                    "flow_run_id": {"any_": ["flow_run_id"]},
                },
                "sort": "TIMESTAMP_ASC",
                "offset": offset,
            }
            prefect_post_mock.assert_called_with("logs/filter", query)


def test_get_flow_runs_by_name_type_error():
    with pytest.raises(TypeError):
        get_flow_runs_by_name(123)


def test_get_flow_runs_by_name_prefect_post():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        flow_run_name = "flow_run_name"
        get_flow_runs_by_name(flow_run_name)
        query = {
            "flow_runs": {"operator": "and_", "name": {"any_": [flow_run_name]}},
        }
        prefect_post_mock.assert_called_with("flow_runs/filter", query)


def test_get_flow_runs_by_name_result():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        flow_run = {
            "id": "flow_run_id",
            "name": "flow_run_name",
            "tags": ["tag1", "tag2"],
            "start_time": "2022-01-01T00:00:00Z",
            "expected_start_time": "2022-01-01T00:00:00Z",
            "total_run_time": 60,
            "state": {"type": "COMPLETED"},
        }
        prefect_post_mock.return_value = [flow_run]
        result = get_flow_runs_by_name("flow_run_name")
        assert result == [flow_run]


def test_get_flow_runs_by_name_exception():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        prefect_post_mock.side_effect = Exception("test error")
        with pytest.raises(PrefectException):
            get_flow_runs_by_name("flow_run_name")


def test_set_deployment_schedule_prefect_post():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        deployment_id = "deployment_id"
        set_deployment_schedule(deployment_id, "active")
        prefect_post_mock.assert_called_with(
            f"deployments/{deployment_id}/set_schedule_active", {}
        )
        set_deployment_schedule(deployment_id, "inactive")
        prefect_post_mock.assert_called_with(
            f"deployments/{deployment_id}/set_schedule_inactive", {}
        )


@patch("proxy.service.prefect_get")
def test_get_flow_run_success(mock_get: Mock):
    mock_get.return_value = "flow-run"
    response = get_flow_run("flow-run-id")
    mock_get.assert_called_once_with("flow_runs/flow-run-id")
    assert response == "flow-run"


@patch("proxy.service.prefect_get")
def test_get_flow_run_falure(mock_get: Mock):
    mock_get.side_effect = Exception("exception")
    with pytest.raises(PrefectException) as excinfo:
        get_flow_run("flow-run-id")
    assert str(excinfo.value) == "failed to fetch a flow-run"


def test_set_deployment_schedule_result():
    with patch("proxy.service.prefect_post"):
        deployment_id = "deployment_id"
        result = set_deployment_schedule(deployment_id, "active")
        assert result is None
