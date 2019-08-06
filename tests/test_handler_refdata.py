"""
For test Handler we must env_test for emulate Bloomberg
"""

import pytest

from async_blp.handler_refdata import HandlerRef
from tests.env_test import Event
from tests.env_test import Message
from tests.env_test import Session


@pytest.mark.run_loop
@pytest.mark.timeout(5)
async def test_handler_async():
    """
    Just open service and wait  RESPONSE
    """
    handler = HandlerRef()
    event_ = handler.complete_event
    handler.send_requests([])
    await event_.wait()
    assert event_.is_set()


@pytest.mark.run_loop
@pytest.mark.timeout(5)
async def test_call_handler():
    """
    only handler know when we can open Service
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
