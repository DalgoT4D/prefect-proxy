import asyncio
import tempfile
from unittest.mock import AsyncMock, patch, Mock

import pytest
import requests
from fastapi import HTTPException
import pendulum
from pydantic import BaseModel
from proxy.exception import PrefectException
from proxy.schemas import (
    AirbyteServerCreate,
    AirbyteServerUpdate,
    DbtCoreCreate,
    DbtProfileCreate,
    DbtProfileUpdate,
    DbtCliProfileBlockUpdate,
    DeploymentCreate2,
    DeploymentUpdate2,
    PrefectSecretBlockCreate,
)
from proxy.service import (
    _create_dbt_cli_profile,
    get_dbt_cli_profile,
    update_dbt_cli_profile,
    get_airbyte_server_block,
    create_airbyte_server_block,
    create_dbt_core_block,
    delete_airbyte_connection_block,
    delete_airbyte_server_block,
    delete_dbt_core_block,
    delete_shell_block,
    get_airbyte_server_block_id,
    get_deployments_by_filter,
    get_flow_run,
    get_flow_run_logs,
    get_flow_runs_by_deployment_id,
    get_flow_runs_by_name,
    parse_log,
    prefect_delete,
    prefect_get,
    prefect_post,
    prefect_patch,
    set_deployment_schedule,
    traverse_flow_run_graph,
    update_airbyte_server_block,
    update_airbyte_connection_block,
    update_postgres_credentials,
    update_bigquery_credentials,
    update_target_configs_schema,
    post_deployment_v1,
    put_deployment_v1,
    get_deployment,
    CronSchedule,
    post_deployment_flow_run,
    create_secret_block,
    upsert_secret_block,
    cancel_flow_run,
    get_flow_run_logs_v2,
    retry_flow_run,
    get_long_running_flow_runs,
    set_cancel_queued_flow_run,
)


class MockAirbyteServer:
    def __init__(self, server_host, server_port, api_version):
        pass

    async def save(self, block_name, **kwargs):
        pass

    async def load(self, block_name, **kwargs):
        pass

    def dict(self):
        return {"_block_document_id": "expected_server_block_id"}

    def model_dump(self):
        return {"_block_document_id": "expected_server_block_id"}


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
    mock_post.assert_called_once_with("http://localhost/test_endpoint", timeout=30, json=payload)


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
    mock_patch.assert_called_once_with("http://localhost/test_endpoint", timeout=30, json=payload)


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
    mock_patch.assert_called_once_with("http://localhost/test_endpoint", timeout=30, json=payload)


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


class MockBlock:
    def dict(self):
        return {"_block_document_id": "expected_block_id"}

    def model_dump(self):
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
    mock_load.side_effect = ValueError("no airbyte server block named invalid_blockname")
    blockname = "invalid_blockname"
    result = await get_airbyte_server_block_id(blockname)
    assert result is None
    mock_load.assert_called_once_with(blockname)


@pytest.mark.asyncio
async def test_get_airbyte_server_block_id_non_string_blockname():
    with pytest.raises(TypeError):
        await get_airbyte_server_block_id(123)


@pytest.mark.asyncio
async def test_get_airbyte_server_block_paramcheck():
    blockname = "test_blockname"
    with pytest.raises(TypeError) as excinfo:
        await get_airbyte_server_block(1)
    assert str(excinfo.value) == "blockname must be a string"


@pytest.mark.asyncio
async def test_get_airbyte_server_block():
    blockname = "test_blockname"
    with patch("proxy.service.AirbyteServer.load", new_callable=AsyncMock) as mock_load:
        mock_load.return_value = "expected_block_id"
        result = await get_airbyte_server_block(blockname)
        assert result == "expected_block_id"


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer", new=MockAirbyteServer)
async def test_create_airbyte_server_block():
    payload = AirbyteServerCreate(
        blockName="test_block",
        serverHost="test_host",
        serverPort="1234",
        apiVersion="test_version",
    )
    result = await create_airbyte_server_block(payload)
    assert result == ("expected_server_block_id", "testblock")


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer", new=MockAirbyteServer)
async def test_create_airbyte_server_block_failure():
    payload = AirbyteServerCreate(
        blockName="test_block",
        serverHost="test_host",
        serverPort="1234",
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


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer", new=MockAirbyteServer)
async def test_put_airbyte_server_block():
    payload = AirbyteServerUpdate(
        blockName="testblock",
        serverHost="test_host",
        serverPort="1234",
        apiVersion="test_version",
    )
    with patch("proxy.service.AirbyteServer.load", new_callable=AsyncMock) as mock_load, patch(
        "proxy.service.AirbyteServer.save", new_callable=AsyncMock
    ) as mock_save:
        mock_load.return_value = MockAirbyteServer(
            payload.serverHost, payload.serverPort, payload.apiVersion
        )
        result = await update_airbyte_server_block(payload)
        print(result)
        assert result == ("expected_server_block_id", "testblock")


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer", new=MockAirbyteServer)
async def test_put_airbyte_server_block_failure():
    payload = AirbyteServerUpdate(
        blockName="test_block",
        serverHost="test_host",
        serverPort="1234",
        apiVersion="test_version",
    )
    with patch("proxy.service.AirbyteServer.save", new_callable=AsyncMock) as mock_save, patch(
        "proxy.service.AirbyteServer.load", new_callable=AsyncMock
    ) as mock_load:
        mock_load.return_value = "expected_block_id"
        mock_save.side_effect = Exception("failed to update airbyte server block")
        with pytest.raises(Exception) as excinfo:
            await update_airbyte_server_block(payload)
        assert str(excinfo.value) == "failed to update airbyte server block"


@pytest.mark.asyncio
@patch("proxy.service.AirbyteServer", new=MockAirbyteServer)
async def test_put_airbyte_server_block_invalid_payload():
    payload = "invalid_payload"
    with pytest.raises(TypeError) as excinfo:
        await update_airbyte_server_block(payload)
    assert str(excinfo.value) == "payload must be an AirbyteServerUpdate"


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

    def model_dump(self):
        return {"_block_document_id": "expected_block_id"}


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
        cli_profile_block_name="test_cli_profile",
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

    assert str(excinfo.value) == "payload is of wrong type"


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
        cli_profile_block_name="test_cli_profile",
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
@patch("proxy.service.DbtCliProfile.load", new_callable=AsyncMock)
async def test_create_dbt_cli_profile_success(mock_load):
    """tests create_dbt_cli_profile"""
    mock_load.return_value = Mock()
    mock_load.return_value.get_profile = Mock(return_value={"key": "value"})
    result = await get_dbt_cli_profile("test_block_name")
    assert result == {"key": "value"}


@pytest.mark.asyncio
@patch("proxy.service.DbtCliProfile.load", new_callable=AsyncMock)
async def test_create_dbt_cli_profile_raises(mock_load):
    """tests create_dbt_cli_profile"""
    mock_load.side_effect = ValueError("error")
    with pytest.raises(HTTPException) as excinfo:
        await get_dbt_cli_profile("test_block_name")
    assert str(excinfo.value.detail) == "No dbt cli profile block named test_block_name"


@pytest.mark.asyncio
@patch("proxy.service.DbtCliProfile.load", new_callable=AsyncMock)
async def test_update_dbt_cli_profile(mock_load):
    """tests update_dbt_cli_profile"""
    mock_load.side_effect = ValueError("error")
    with pytest.raises(PrefectException) as excinfo:
        payload = DbtCliProfileBlockUpdate(
            cli_profile_block_name="dne", wtype=None, profile=None, credentials=None
        )
        await update_dbt_cli_profile(payload)
    assert str(excinfo.value) == "no dbt cli profile block named dne"


@pytest.mark.asyncio
@patch("proxy.service.DbtCliProfile.load", new_callable=AsyncMock)
async def test_update_dbt_cli_profile_postgres(mock_load: AsyncMock):
    """tests update_dbt_cli_profile"""
    payload = DbtCliProfileBlockUpdate(
        cli_profile_block_name="block-name",
        profile=DbtProfileUpdate(target_configs_schema="new_schema", name="profile-name"),
        credentials={
            "host": "new_host",
            "port": "new_port",
            "database": "new_database",
            "username": "new_username",
            "password": "new_password",
        },
        wtype="postgres",
    )
    mock_load.return_value = Mock(
        target_configs=Mock(schema="old-schema"),
        save=AsyncMock(),
        dict=Mock(return_value={"_block_document_id": "_block_document_id"}),
        model_dump=Mock(return_value={"_block_document_id": "_block_document_id"}),
    )
    block, block_id, block_name = await update_dbt_cli_profile(payload)
    assert block_name == "block-name"
    assert block_id == "_block_document_id"
    assert block.target_configs.schema == "new_schema"
    assert block.target == "new_schema"
    assert block.name == "profile-name"
    assert block.target_configs.extras["host"] == "new_host"
    assert block.target_configs.extras["port"] == "new_port"
    assert block.target_configs.extras["database"] == "new_database"
    assert block.target_configs.extras["username"] == "new_username"
    assert block.target_configs.extras["password"] == "new_password"


@pytest.mark.asyncio
@patch("proxy.service.DbtCliProfile.load", new_callable=AsyncMock)
async def test_update_dbt_cli_profile_postgres_override_target(mock_load: AsyncMock):
    """tests update_dbt_cli_profile"""
    payload = DbtCliProfileBlockUpdate(
        cli_profile_block_name="block-name",
        profile=DbtProfileUpdate(
            target_configs_schema="new_schema", name="profile-name", target="override"
        ),
        credentials={
            "host": "new_host",
            "port": "new_port",
            "database": "new_database",
            "username": "new_username",
            "password": "new_password",
        },
        wtype="postgres",
    )
    mock_load.return_value = Mock(
        target_configs=Mock(schema="old-schema"),
        save=AsyncMock(),
        dict=Mock(return_value={"_block_document_id": "_block_document_id"}),
        model_dump=Mock(return_value={"_block_document_id": "_block_document_id"}),
    )
    block, block_id, block_name = await update_dbt_cli_profile(payload)
    assert block_name == "block-name"
    assert block_id == "_block_document_id"
    assert block.target == "override"


@pytest.mark.asyncio
@patch("proxy.service.DbtCliProfile.load", new_callable=AsyncMock)
@patch("proxy.service.GcpCredentials", Mock(return_value={}))
async def test_update_dbt_cli_profile_bigquery(mock_load: AsyncMock):
    """tests update_dbt_cli_profile"""
    service_account_info = {
        "token_uri": "token_uri",
        "client_email": "client_email",
        "private_key": "private key",
    }
    payload = DbtCliProfileBlockUpdate(
        cli_profile_block_name="block-name",
        profile=DbtProfileUpdate(target_configs_schema="new_schema", name="profile-name"),
        credentials=service_account_info,
        wtype="bigquery",
        bqlocation="bq-location",
    )
    mock_load.return_value = Mock(
        target_configs=Mock(schema="old-schema"),
        save=AsyncMock(),
        dict=Mock(return_value={"_block_document_id": "_block_document_id"}),
        model_dump=Mock(return_value={"_block_document_id": "_block_document_id"}),
    )
    block, block_id, block_name = await update_dbt_cli_profile(payload)
    assert block_name == "block-name"
    assert block_id == "_block_document_id"
    assert block.target_configs.extras == {"location": "bq-location"}
    assert block.target == "new_schema"
    assert block.name == "profile-name"


@pytest.mark.asyncio
@patch("proxy.service.DbtCliProfile.save", new_callable=AsyncMock)
@patch("proxy.service.DbtCoreOperation.__init__", return_value=None)
@patch("proxy.service.DbtCoreOperation.save", new_callable=AsyncMock)
@patch("proxy.service._block_id", return_value=("test_block_id", "test_cleaned_blockname"))
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
            cli_profile_block_name="test_cli_profile",
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
@patch("proxy.service._block_id", return_value=("test_block_id", "test_cleaned_blockname"))
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
            cli_profile_block_name="test_cli_profile",
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
@patch("proxy.service.Secret.load", new_callable=AsyncMock)
@patch("proxy.service.Secret.save", new_callable=AsyncMock)
async def test_edit_secret_block(mock_save: AsyncMock, mock_load: AsyncMock):
    mock_load.return_value = Mock()

    mock_save.side_effect = Exception("exception thrown")
    payload = PrefectSecretBlockCreate(secret="my-secret", blockName="my-blockname")
    with pytest.raises(PrefectException) as excinfo:
        await upsert_secret_block(payload)
    assert str(excinfo.value) == "Could not edit the secret block"


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
    AsyncMock(return_value=Mock(dbt_cli_profile=Mock(target_configs=Mock(type="not-postgres")))),
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
                model_dump=Mock(
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

    await update_postgres_credentials("block-name", {"host": "new_host", "dbname": "new_database"})

    dbt_coreop_block.dbt_cli_profile.save.assert_called_once_with(name="block-name", overwrite=True)
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
    AsyncMock(return_value=Mock(dbt_cli_profile=Mock(target_configs=Mock(type="not-bigquery")))),
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
                model_dump=Mock(
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

    dbt_coreop_block.dbt_cli_profile.save.assert_called_once_with(name="block-name", overwrite=True)
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

    dbt_coreop_block.dbt_cli_profile.save.assert_called_once_with(name="block-name", overwrite=True)
    dbt_coreop_block.save.assert_called_once_with("block-name", overwrite=True)

    assert dbt_coreop_block.dbt_cli_profile.target_configs.schema == "newtarget"
    assert dbt_coreop_block.dbt_cli_profile.target == "newtarget"
    assert dbt_coreop_block.commands[0] == "dbt run --target newtarget"


async def test_post_deployment_bad_param():
    with pytest.raises(TypeError) as excinfo:
        await post_deployment_v1("deployment-id")
    assert str(excinfo.value) == "payload must be a DeploymentCreate"


@pytest.mark.asyncio
@patch("proxy.service.flow.from_source", new_callable=Mock)
async def test_post_deployment_1(mock_from_source):
    payload = DeploymentCreate2(
        work_queue_name="queue-name",
        work_pool_name="pool-name",
        flow_name="flow-name",
        flow_id="flow-id",
        deployment_name="deployment-name",
        org_slug="org-slug",
        deployment_params={"param1": "value1"},
        cron="* * * * *",
    )
    mock_deploy = Mock(return_value="deployment-id")
    mock_from_source.return_value.deploy = mock_deploy

    deployment = post_deployment_v1(payload)
    # mock_build_from_flow.assert_called_once_with(
    #     flow="flow",
    #     name=payload.deployment_name,
    #     work_queue_name="queue-name",
    #     work_pool_name="pool-name",
    #     tags=[payload.org_slug],
    #     is_schedule_active=True,
    # )
    assert deployment == {
        "id": "deployment-id",
        "name": payload.deployment_name,
        "params": payload.deployment_params,
    }
    # assert retval["name"] == "deployment-name"


@patch("proxy.service.prefect_patch")
def test_put_deployment_v1(mock_prefect_patch):
    payload = DeploymentUpdate2(
        deployment_params={"param1": "value1"},
        cron="* * * * *",
        work_pool_name="pool-name",
        work_queue_name="queue-name",
    )
    put_deployment_v1("deployment-id", payload)
    mock_prefect_patch.assert_called_once_with(
        "deployments/deployment-id",
        {
            "schedules": [
                {"schedule": CronSchedule(cron="* * * * *").model_dump(), "active": True}
            ],
            "parameters": {"param1": "value1"},
            "work_pool_name": "pool-name",
            "work_queue_name": "queue-name",
        },
    )


def test_get_deployment_bad_param():
    with pytest.raises(TypeError) as excinfo:
        get_deployment(123)
    assert str(excinfo.value) == "deployment_id must be a string"


@patch("proxy.service.prefect_get")
def test_get_deployment(mock_get: Mock):
    mock_get.return_value = "retval"
    response = get_deployment("deployment-id")
    mock_get.assert_called_once_with(f"deployments/deployment-id")
    assert response == "retval"


def test_get_flow_runs_by_deployment_id_type_error():
    with pytest.raises(TypeError):
        get_flow_runs_by_deployment_id(123, 10, "")
    with pytest.raises(TypeError):
        get_flow_runs_by_deployment_id("deployment_id", "invalid limit", None)


def test_get_flow_runs_by_deployment_id_value_error():
    with pytest.raises(ValueError):
        get_flow_runs_by_deployment_id("deployment_id", -1, "")


def test_get_flow_runs_by_deployment_id_prefect_post():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        deployment_id = "deployment_id"
        limit = 10
        get_flow_runs_by_deployment_id(deployment_id, limit, "")
        query = {
            "sort": "START_TIME_DESC",
            "deployments": {"id": {"any_": [deployment_id]}},
            "flow_runs": {
                "operator": "and_",
                "state": {"type": {"any_": ["COMPLETED", "FAILED", "CRASHED", "CANCELLED"]}},
            },
            "limit": limit,
        }
        prefect_post_mock.assert_called_with("flow_runs/filter", query)


def test_get_flow_runs_by_deployment_id_result():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        with patch("proxy.service.prefect_get") as prefect_get_mock:
            prefect_get_mock.return_value = []
            flow_run = {
                "id": "flow_run_id",
                "name": "flow_run_name",
                "tags": ["tag1", "tag2"],
                "start_time": "2022-01-01T00:00:00Z",
                "expected_start_time": "2022-01-01T00:00:00Z",
                "total_run_time": 60,
                "state": {"type": "COMPLETED", "name": "Completed"},
            }
            prefect_post_mock.side_effect = [[flow_run], []]
            result = get_flow_runs_by_deployment_id("deployment_id", 10, "")
            assert result == [
                {
                    "id": flow_run["id"],
                    "name": flow_run["name"],
                    "tags": flow_run["tags"],
                    "startTime": flow_run["start_time"],
                    "expectedStartTime": flow_run["expected_start_time"],
                    "totalRunTime": flow_run["total_run_time"],
                    "status": flow_run["state"]["type"],
                    "state_name": "Completed",
                }
            ]


def test_get_flow_runs_by_deployment_id_result_state_from_task():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        with patch("proxy.service.prefect_get") as prefect_get_mock:
            prefect_get_mock.return_value = []
            flow_run = {
                "id": "flow_run_id",
                "name": "flow_run_name",
                "tags": ["tag1", "tag2"],
                "start_time": "2022-01-01T00:00:00Z",
                "expected_start_time": "2022-01-01T00:00:00Z",
                "total_run_time": 60,
                "state": {"type": "COMPLETED", "name": "Completed"},
            }
            task_run = {"state": {"name": "DBT_TEST_FAILED"}}
            prefect_post_mock.side_effect = [[flow_run], [task_run]]
            result = get_flow_runs_by_deployment_id("deployment_id", 10, "")
            assert result == [
                {
                    "id": flow_run["id"],
                    "name": flow_run["name"],
                    "tags": flow_run["tags"],
                    "startTime": flow_run["start_time"],
                    "expectedStartTime": flow_run["expected_start_time"],
                    "totalRunTime": flow_run["total_run_time"],
                    "status": "COMPLETED",
                    "state_name": "DBT_TEST_FAILED",
                }
            ]


def test_get_flow_runs_by_deployment_id_exception():
    with patch("proxy.service.prefect_post") as prefect_post_mock:
        prefect_post_mock.side_effect = Exception("test error")
        with pytest.raises(PrefectException):
            get_flow_runs_by_deployment_id("deployment_id", 10, "")


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
        with patch("proxy.service.traverse_flow_run_graph") as traverse_flow_run_graph_mock:
            traverse_flow_run_graph_mock.return_value = ["flow_run_id"]
            flow_run_id = "flow_run_id"
            taks_run_id = "task_run_id"
            limit = 10
            offset = 10
            get_flow_run_logs(flow_run_id, taks_run_id, limit, offset)
            query = {
                "logs": {
                    "operator": "and_",
                    "flow_run_id": {"any_": ["flow_run_id"]},
                    "task_run_id": {"any_": ["task_run_id"]},
                },
                "sort": "TIMESTAMP_ASC",
                "offset": offset,
                "limit": limit,
            }
            prefect_post_mock.assert_called_with("logs/filter", query)


@patch("proxy.service.prefect_get")
@patch("proxy.service.prefect_post")
@patch("proxy.service.traverse_flow_run_graph_v2")
def test_get_flow_run_logs_v2_flow_run(
    mock_traverse_flow_run_graph_v2: Mock,
    mock_prefect_post: Mock,
    mock_prefect_get: Mock,
):
    mock_traverse_flow_run_graph_v2.return_value = [
        {
            "kind": "flow-run",
            "id": "run-id",
            "label": "run-label",
            "start_time": "start-time",
            "end_time": "end-time",
        }
    ]
    mock_prefect_get.return_value = {
        "state_name": "state-name",
        "state_type": "state-type",
    }
    mock_prefect_post.return_value = []
    retval = get_flow_run_logs_v2("flow_run_id")
    mock_prefect_get.assert_called_once_with("flow_runs/run-id")
    mock_prefect_post.assert_called_once_with(
        "logs/filter",
        {
            "logs": {
                "operator": "or_",
                "flow_run_id": {"any_": ["run-id"]},
                "task_run_id": {"any_": []},
            },
            "sort": "TIMESTAMP_ASC",
        },
    )
    assert retval == [
        {
            "id": "run-id",
            "kind": "flow-run",
            "label": "run-label",
            "state_type": "state-type",
            "state_name": "state-name",
            "start_time": "start-time",
            "end_time": "end-time",
            "logs": [],
        }
    ]


@patch("proxy.service.prefect_get")
@patch("proxy.service.prefect_post")
@patch("proxy.service.traverse_flow_run_graph_v2")
def test_get_flow_run_logs_v2_task_run(
    mock_traverse_flow_run_graph_v2: Mock,
    mock_prefect_post: Mock,
    mock_prefect_get: Mock,
):
    mock_traverse_flow_run_graph_v2.return_value = [
        {
            "kind": "task-run",
            "id": "run-id",
            "label": "run-label",
            "start_time": "start-time",
            "end_time": "end-time",
        }
    ]
    mock_prefect_get.return_value = {
        "state_name": "state-name",
        "state_type": "state-type",
    }
    mock_prefect_post.return_value = []
    retval = get_flow_run_logs_v2("flow_run_id")
    mock_prefect_get.assert_called_once_with("task_runs/run-id")
    mock_prefect_post.assert_called_once_with(
        "logs/filter",
        {
            "logs": {
                "operator": "or_",
                "flow_run_id": {"any_": []},
                "task_run_id": {"any_": ["run-id"]},
            },
            "sort": "TIMESTAMP_ASC",
        },
    )
    assert retval == [
        {
            "id": "run-id",
            "kind": "task-run",
            "label": "run-label",
            "state_type": "state-type",
            "state_name": "state-name",
            "start_time": "start-time",
            "end_time": "end-time",
            "logs": [],
        }
    ]


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
        prefect_post_mock.assert_called_with(f"deployments/{deployment_id}/set_schedule_active", {})
        set_deployment_schedule(deployment_id, "inactive")
        prefect_post_mock.assert_called_with(
            f"deployments/{deployment_id}/set_schedule_inactive", {}
        )


@patch("proxy.service.prefect_get")
@patch("proxy.service.update_flow_run_final_state")
def test_get_flow_run_success(mock_update_flow_run_final_state: Mock, mock_get: Mock):
    mock_get.return_value = {"id": "flow-run-id", "state": {"type": "COMPLETED"}}
    mock_update_flow_run_final_state.return_value = {
        "id": "flow-run-id",
        "state": {"type": "COMPLETED"},
        "state_name": "COMPLETED",
    }
    response = get_flow_run("flow-run-id")
    mock_get.assert_called_once_with("flow_runs/flow-run-id")
    mock_update_flow_run_final_state.assert_called_once_with(
        {"id": "flow-run-id", "state": {"type": "COMPLETED"}}
    )
    assert response == {
        "id": "flow-run-id",
        "state": {"type": "COMPLETED"},
        "state_name": "COMPLETED",
    }


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


async def test_cancel_flow_runs_type_error():
    with pytest.raises(TypeError):
        await cancel_flow_run(123)


async def test_cancel_flow_run_failure():
    with patch("proxy.service.get_client") as mock_cancel:
        mock_cancel.side_effect = Exception("exception")
        with pytest.raises(PrefectException) as excinfo:
            await cancel_flow_run("flow_run_id")
            assert str(excinfo.value) == "failed to cancel flow-run"


async def test_cancel_flow_run_success():
    with patch("proxy.service.get_client"):
        flow_run_id = "valid_flow_run_id"
        result = await cancel_flow_run(flow_run_id)
        assert result is None


def test_retry_flow_run_bad_param():
    with pytest.raises(TypeError) as excinfo:
        retry_flow_run(123)
    assert str(excinfo.value) == "flow_run_id must be a string"


@patch("proxy.service.prefect_post")
@patch("proxy.service.pendulum")
def test_retry_flow_run(mock_pendulum: Mock, mock_prefect_post: Mock):
    mock_pendulum.now.return_value = pendulum.time(0, 0, 0)
    mock_pendulum.duration.return_value = pendulum.duration(minutes=5)
    retry_flow_run("flow-run-id")
    mock_prefect_post.assert_called_once_with(
        "flow_runs/flow-run-id/set_state",
        {
            "force": True,
            "state": {
                "name": "AwaitingRetry",
                "message": "Retry via prefect proxy",
                "type": "SCHEDULED",
                "state_details": {
                    "scheduled_time": str(pendulum.time(0, 0, 0) + pendulum.duration(minutes=5))
                },  # using pendulum because prefect also uses it
            },
        },
    )


def test_get_long_running_flow_runs():
    """Test the get_long_running_flow_runs function."""
    nhours = 10
    start_time_str = "2024-01-01T00:00:00+05:30"
    request_parameters = {
        "flow_runs": {
            "operator": "and_",
            "state": {
                "operator": "and_",
                "type": {"any_": ["RUNNING"]},
            },
            "start_time": {"before_": "2023-12-31T08:30:00+00:00"},
        }
    }
    with patch("proxy.service.prefect_post") as mock_prefect_post:
        get_long_running_flow_runs(nhours, start_time_str)
    mock_prefect_post.assert_called_once_with("flow_runs/filter", request_parameters)


class PayloadModel(BaseModel):
    state: dict
    force: str


@pytest.fixture
def mock_payload():
    return PayloadModel(state={"name": "Cancelling", "type": "CANCELLING"}, force="TRUE")


@patch("proxy.service.prefect_get")
@patch("proxy.service.prefect_post")
def test_cancel_flow_run_pending(mock_prefect_post, mock_prefect_get, mock_payload: PayloadModel):
    """Test successful cancellation of a PENDING flow run."""
    mock_prefect_get.return_value = {"state_type": "PENDING"}

    set_cancel_queued_flow_run("valid_flow_run_id", mock_payload)

    mock_prefect_post.assert_called_once_with(
        "flow_runs/valid_flow_run_id/set_state", payload=mock_payload.model_dump()
    )


@patch("proxy.service.prefect_get")
@patch("proxy.service.prefect_post")
def test_cancel_flow_run_scheduled(mock_prefect_post, mock_prefect_get, mock_payload: PayloadModel):
    """Test successful cancellation of a SCHEDULED flow run."""
    mock_prefect_get.return_value = {"state_type": "SCHEDULED"}

    set_cancel_queued_flow_run("valid_flow_run_id", mock_payload)

    mock_prefect_post.assert_called_once_with(
        "flow_runs/valid_flow_run_id/set_state", payload=mock_payload.model_dump()
    )


@patch("proxy.service.prefect_get")
@patch("proxy.service.prefect_post")
def test_cannot_cancel_non_queued_run(
    mock_prefect_post, mock_prefect_get, mock_payload: PayloadModel
):
    """Test that cancellation fails if the flow run is not PENDING or SCHEDULED."""
    mock_prefect_get.return_value = {"state_type": "RUNNING"}

    with pytest.raises(ValueError, match="Unable to cancel a non-queued job."):
        set_cancel_queued_flow_run("valid_flow_run_id", mock_payload)

    mock_prefect_post.assert_not_called()
