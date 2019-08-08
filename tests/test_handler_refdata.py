"""
For test Handler we must use env_test for emulate Bloomberg
"""
import asyncio

import pytest

from async_blp.env_test import SessionOptions
from async_blp.handler_refdata import HandlerRef
from async_blp.requests import ReferenceDataRequest


@pytest.fixture()
def session_options() -> SessionOptions():
    """
    for test not important
    """
    session_options_ = SessionOptions()
    session_options_.setServerHost("localhost")
    session_options_.setServerPort(8194)
    return session_options_


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_handler_async(session_options):
    """
    Just open service and wait for RESPONSE
    """
    field_name = 'PX_LAST'
    security_id = 'F Equity'

    request = ReferenceDataRequest([security_id], [field_name])
    handler = HandlerRef(session_options)
    asyncio.create_task(handler.send_requests([request]))
    asyncio.create_task(request.process())
    result = await request.process()
    assert True


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_call_handler(session_options):
    """
    only handler knows when we can open Service
    """
    handler = HandlerRef(session_options)
    await handler.connection.wait()
    assert handler.connection.is_set()
