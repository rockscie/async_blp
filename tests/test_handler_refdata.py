"""
For test Handler we must use env_test for emulate Bloomberg
"""

import pytest

from async_blp.env_test import Event
from async_blp.env_test import Message
from async_blp.env_test import Session
from async_blp.handler_refdata import HandlerRef


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_handler_async():
    """
    Just open service and wait for RESPONSE
    """
    handler = HandlerRef()
    event_ = handler.complete_event
    handler.send_requests([])
    await event_.wait()
    assert event_.is_set()


@pytest.mark.asyncio
@pytest.mark.timeout(5)
async def test_call_handler():
    """
    only handler knows when we can open Service
    """
    handler = HandlerRef(start_session=False)
    handler(Event(
        type_=Event.OTHER,
        msgs=[
            Message(value=0,
                    name='ServiceOpened'),
            ]
        ), Session())
    await handler.connection.wait()
    assert handler.connection.is_set()
