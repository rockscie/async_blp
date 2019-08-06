import asyncio
import time

import pytest

from async_blp.handler_refdata import HandlerRef


@pytest.mark.run_loop
@pytest.mark.timeout(5)
async def test_main_acync():
    handler = HandlerRef()
    event_ = handler.event
    await event_.wait()
    assert event_.is_set()

