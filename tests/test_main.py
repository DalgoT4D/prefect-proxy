from fastapi import FastAPI, HTTPException
from fastapi.testclient import TestClient
from unittest.mock import patch

import pytest

from main import app, create_airbyte_server_block, get_airbyte_connection_by_blockid, get_airbyte_connection_by_blockname, get_airbyte_server, post_airbyte_connection, post_airbyte_server
from schemas import AirbyteConnectionCreate, AirbyteServerCreate

app = FastAPI()
client = TestClient(app)


@pytest.mark.asyncio
async def test_get_airbyte_server_success():
    with patch('main.get_airbyte_server_block_id', return_value='12345'):
        response = await get_airbyte_server('test_block')
        assert response == {'block_id': '12345'}

@pytest.mark.asyncio
async def test_get_airbyte_server_failure():
    with patch('main.get_airbyte_server_block_id', return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_server('test_block')
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == 'no block having name test_block'


@pytest.mark.asyncio
async def test_post_airbyte_server_success():
    payload = AirbyteServerCreate(
        blockName='test_server',
        serverHost='http://test-server.com',
        serverPort=8000,
        apiVersion='v1'
    )
    with patch('main.create_airbyte_server_block', return_value='12345'):
        response = await post_airbyte_server(payload)
        assert response == {'block_id': '12345'}

@pytest.mark.asyncio
async def test_post_airbyte_server_failure():
    payload = AirbyteServerCreate(
        blockName='test_server',
        serverHost='http://test-server.com',
        serverPort=8000,
        apiVersion='v1'
    )
    with patch('main.create_airbyte_server_block', side_effect=Exception('test error')):
        with pytest.raises(HTTPException) as excinfo:
            await post_airbyte_server(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == 'failed to create airbyte server block'


@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockname_success():
    with patch('main.get_airbyte_connection_block_id', return_value='12345'):
        response = await get_airbyte_connection_by_blockname('test_block')
        assert response == {'block_id': '12345'}

@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockname_failure():
    with patch('main.get_airbyte_connection_block_id', return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_connection_by_blockname('test_block')
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == 'no block having name test_block'

@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockid_success():
    block_data = {'id': '12345', 'name': 'test_block', 'url': 'http://test-block.com'}
    with patch('main.get_airbyte_connection_block', return_value=block_data):
        response = await get_airbyte_connection_by_blockid('12345')
        assert response == block_data

@pytest.mark.asyncio
async def test_get_airbyte_connection_by_blockid_failure():
    with patch('main.get_airbyte_connection_block', return_value=None):
        with pytest.raises(HTTPException) as excinfo:
            await get_airbyte_connection_by_blockid('12345')
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == 'no block having id 12345'


@pytest.mark.asyncio
async def test_post_airbyte_connection_success():
    payload = AirbyteConnectionCreate(
        serverBlockName='test_server',
        connectionId='12345',
        connectionBlockName='test_connection'
    )
    with patch('main.create_airbyte_connection_block', return_value='12345'):
        response = await post_airbyte_connection(payload)
        assert response == {'block_id': '12345'}

@pytest.mark.asyncio
async def test_post_airbyte_connection_failure():
    payload = AirbyteConnectionCreate(
        serverBlockName='test_server',
        connectionId='12345',
        connectionBlockName='test_connection'
    )
    with patch('main.create_airbyte_connection_block', side_effect=Exception('test error')):
        with pytest.raises(HTTPException) as excinfo:
            await post_airbyte_connection(payload)
        assert excinfo.value.status_code == 400
        assert excinfo.value.detail == 'failed to create airbyte connection block'