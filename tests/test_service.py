from fastapi import HTTPException
import pytest
import requests
from unittest.mock import AsyncMock, patch
from exception import PrefectException
from schemas import AirbyteConnectionCreate, AirbyteServerCreate, PrefectShellSetup
from prefect_dbt.cli.commands import DbtCoreOperation, ShellOperation
from service import create_airbyte_connection_block, create_airbyte_server_block, create_shell_block, delete_airbyte_connection_block, delete_airbyte_server_block, delete_shell_block, get_airbyte_connection_block, get_airbyte_connection_block_id, get_airbyte_server_block_id, get_dbtcore_block_id, get_shell_block_id, prefect_delete, prefect_get, prefect_post



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
    mock_response._content = b'{"error": "Invalid input data"}'
    mock_post.return_value = mock_response

    endpoint = "test_endpoint"
    payload = {"test_key": "test_value"}
    
    with pytest.raises(HTTPException) as excinfo:
        prefect_post(endpoint, payload)
    
    assert excinfo.value.status_code == 400
    assert excinfo.value.detail == "Invalid input data"


def test_prefect_get_invalid_endpoint():
    with pytest.raises(TypeError) as excinfo:
        prefect_post(123, {})
    assert str(excinfo.value) == "endpoint must be a string"

    
@patch("requests.get")
@patch("os.getenv")
def test_prefect_get_failure(mock_getenv, mock_get):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 400
    mock_response._content = b'{"error": "Invalid request"}'
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
        prefect_post(123, {})
    assert str(excinfo.value) == "endpoint must be a string"


@patch("requests.delete")
@patch("os.getenv")
def test_prefect_delete_failure(mock_getenv, mock_delete):
    mock_getenv.return_value = "http://localhost"
    mock_response = requests.Response()
    mock_response.status_code = 400
    mock_response._content = b'{"error": "Invalid request"}'
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


class MockBlock:
    def dict(self):
        return {"_block_document_id": "expected_block_id"}

@pytest.mark.asyncio
@patch("service.AirbyteServer.load", new_callable=AsyncMock)
async def test_get_airbyte_server_block_id_valid_blockname(mock_load):
    mock_load.return_value = MockBlock()
    blockname = "valid_blockname"
    result = await get_airbyte_server_block_id(blockname)
    assert result == "expected_block_id"
    mock_load.assert_called_once_with(blockname)


@pytest.mark.asyncio
@patch("service.AirbyteServer.load", new_callable=AsyncMock)
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


class MockAirbyteServer:
    def __init__(self, server_host, server_port, api_version):
        pass

    async def save(self, block_name):
        pass

    def dict(self):
        return {"_block_document_id": "expected_block_id"}

@pytest.mark.asyncio
@patch("service.AirbyteServer", new=MockAirbyteServer)
async def test_create_airbyte_server_block():
    payload = AirbyteServerCreate(
        blockName="test_block",
        serverHost="test_host",
        serverPort=1234,
        apiVersion="test_version",
    )
    result = await create_airbyte_server_block(payload)
    assert result == "expected_block_id"


@pytest.mark.asyncio
@patch("service.AirbyteServer", new=MockAirbyteServer)
async def test_create_airbyte_server_block_failure():
    payload = AirbyteServerCreate(
        blockName="test_block",
        serverHost="test_host",
        serverPort=1234,
        apiVersion="test_version",
    )
    with patch("service.AirbyteServer.save", new_callable=AsyncMock) as mock_save:
        mock_save.side_effect = Exception("failed to create airbyte server block")
        with pytest.raises(Exception) as excinfo:
            await create_airbyte_server_block(payload)
        assert str(excinfo.value) == "failed to create airbyte server block"

@pytest.mark.asyncio
@patch("service.AirbyteServer", new=MockAirbyteServer)
async def test_create_airbyte_server_block_invalid_payload():
    payload = "invalid_payload"
    with pytest.raises(TypeError) as excinfo:
        await create_airbyte_server_block(payload)
    assert str(excinfo.value) == "payload must be an AirbyteServerCreate"


@patch("service.prefect_delete")
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


@pytest.mark.asyncio
@patch("service.AirbyteConnection.load", new_callable=AsyncMock)
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
@patch("service.AirbyteConnection.load", new_callable=AsyncMock)
async def test_get_airbyte_connection_block_id_invalid_blockname(mock_load):
    mock_load.side_effect = ValueError("no airbyte connection block named invalid_blockname")
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
@patch("service.prefect_get")
async def test_get_airbyte_connection_block_valid_blockid(mock_prefect_get):
    mock_prefect_get.return_value = {"key": "value"}
    blockid = "valid_blockid"
    result = await get_airbyte_connection_block(blockid)
    assert result == {"key": "value"}
    mock_prefect_get.assert_called_once_with(f"block_documents/{blockid}")

@pytest.mark.asyncio
@patch("service.prefect_get")
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
    def __init__(self, airbyte_server, connection_id):
        self.airbyte_server = airbyte_server
        self.connection_id = connection_id

    async def save(self, block_name):
        if self.connection_id == "test_error_connection_id":
            raise Exception("test error")

    def dict(self):
        return {"_block_document_id": "expected_connection_block_id"}

@pytest.mark.asyncio
@patch("service.AirbyteConnection", new=MockAirbyteConnection)
@patch("service.AirbyteServer.load", new_callable=AsyncMock)
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
@patch("service.AirbyteConnection", new=MockAirbyteConnection)
@patch("service.AirbyteServer.load", new_callable=AsyncMock)
async def test_create_airbyte_connection_block_save_error(mock_load):
    mock_load.return_value = MockAirbyteServer(None, None, None)
    conninfo = AirbyteConnectionCreate(
        serverBlockName="test_server_block",
        connectionBlockName="test_connection_block",
        connectionId="test_error_connection_id",
    )
    with pytest.raises(PrefectException) as excinfo:
        await create_airbyte_connection_block(conninfo)
    assert str(excinfo.value) == f"failed to create airbyte connection block for connection {conninfo.connectionId}"


@pytest.mark.asyncio
@patch("service.AirbyteServer.load", new_callable=AsyncMock)
async def test_create_airbyte_connection_block_invalid_server_block(mock_load):
    mock_load.side_effect = ValueError("no airbyte server block named invalid_server_block")
    conninfo = AirbyteConnectionCreate(
        serverBlockName="invalid_server_block",
        connectionBlockName="test_connection_block",
        connectionId="test_connection_id",
    )
    with pytest.raises(PrefectException) as excinfo:
        await create_airbyte_connection_block(conninfo)
    assert str(excinfo.value) == "could not find Airbyte Server block named invalid_server_block"
    mock_load.assert_called_once_with("invalid_server_block")


@pytest.mark.asyncio
async def test_create_airbyte_connection_block_invalid_conninfo():
    conninfo = "invalid_conninfo"
    with pytest.raises(TypeError) as excinfo:
        await create_airbyte_connection_block(conninfo)
    assert str(excinfo.value) == "conninfo must be an AirbyteConnectionCreate"


# =================================================================================================

@patch("service.prefect_delete")
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

    async def save(self, block_name):
        pass

    def dict(self):
        return {"_block_document_id": "expected_block_id"}

@pytest.mark.asyncio
@patch("service.ShellOperation.load", new_callable=AsyncMock)
async def test_get_shell_block_id_valid_blockname(mock_load):
    mock_load.return_value = MockShellOperation(None, None, None)
    blockname = "valid_blockname"
    result = await get_shell_block_id(blockname)
    assert result == "expected_block_id"
    mock_load.assert_called_once_with(blockname)

@pytest.mark.asyncio
@patch("service.ShellOperation.load", new_callable=AsyncMock)
async def test_get_shell_block_id_invalid_blockname(mock_load):
    mock_load.side_effect = ValueError("no shell operation block named invalid_blockname")
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
@patch("service.ShellOperation", new=MockShellOperation)
@patch("service.ShellOperation.load", new_callable=AsyncMock)
async def test_create_shell_block(mock_load):
    mock_load.return_value = MockShellOperation(None, None, None)
    shell = PrefectShellSetup(
        blockName="test_block_name",
        commands=["test_command"],
        env={"test_key": "test_value"},
        workingDir="test_working_dir",
    )
    result = await create_shell_block(shell)
    assert result == "expected_block_id"

@pytest.mark.asyncio
async def test_create_shell_block_invalid_shell():
    shell = "invalid_shell"
    with pytest.raises(TypeError) as excinfo:
        await create_shell_block(shell)
    assert str(excinfo.value) == "shell must be a PrefectShellSetup"


# @pytest.mark.asyncio
# @patch("service.ShellOperation", new=MockShellOperation)
# @patch("service.ShellOperation.load", new_callable=AsyncMock)
# async def test_create_shell_block_failure(mock_load):
#     mock_load.side_effect = Exception('load failed')

#     shell = PrefectShellSetup(
#         blockName="test_block_name",
#         commands=["test_command"],
#         env={"test_key": "test_value"},
#         workingDir="test_working_dir",
#     )

#     with pytest.raises(PrefectException) as excinfo:
#         await create_shell_block(shell)
#     assert str(excinfo.value) == 'failed to create shell block'

    
@patch("service.prefect_delete")
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
        return {'_block_document_id': 'expected_block_id'}
    
@pytest.mark.asyncio
async def test_get_dbtcore_block_id_success():

    mock_block = MockBlock()

    with patch('service.DbtCoreOperation.load', new_callable=AsyncMock) as mock_load:
        mock_load.return_value = mock_block
        result = await get_dbtcore_block_id('test_block_name')
        assert result == 'expected_block_id'


@pytest.mark.asyncio
@patch("service.DbtCoreOperation.load", new_callable=AsyncMock)
async def test_get_dbtcore_block_id_failure(mock_load):
    mock_load.side_effect = ValueError('load failed')

    with pytest.raises(HTTPException) as excinfo:
        await get_dbtcore_block_id('test_block_name')
    assert excinfo.value.status_code == 404
    assert excinfo.value.detail == 'No dbt core operation block named test_block_name'


@pytest.mark.asyncio
async def test_get_dbtcore_block_id_invalid_blockname():
    with pytest.raises(TypeError) as excinfo:
        await get_dbtcore_block_id(123)
    assert str(excinfo.value) == 'blockname must be a string'