"""
high level Api
"""
import asyncio
from typing import List
from typing import Optional

from async_blp.handler_refdata import HandlerRef
from async_blp.requests import ReferenceDataRequest
from async_blp.requests import SecurityIdType

# pylint: disable=ungrouped-imports
try:
    import blpapi
except ImportError:
    from async_blp import env_test as blpapi


async def get_reference_data(
        securities: List[str],
        fields: List[str],
        security_id_type: Optional[SecurityIdType] = None,
        overrides=None,
        host='127.0.0.1',
        port=8194,
        ):
    """
    Async API for Bloomberg ReferenceDataRequest
    """
    request = ReferenceDataRequest(securities, fields, security_id_type,
                                   overrides)

    session_options = blpapi.SessionOptions()
    session_options.setServerHost(host)
    session_options.setServerPort(port)

    handler = HandlerRef(session_options)
    asyncio.create_task(handler.send_requests([request]))
    data, errors = await request.process()

    handler.stop_session()
    await handler.session_stopped.wait()

    return data, errors
