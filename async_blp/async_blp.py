"""
high level Api
"""
import asyncio
from typing import List
from typing import Optional

from async_blp.handler_refdata import HandlerRef
from async_blp.requests import ReferenceDataRequest
from async_blp.requests import SecurityIdType

try:
    # pylint: disable=ungrouped-imports
    import blpapi
except ImportError:
    # pylint: disable=ungrouped-imports
    from async_blp import env_test as blpapi


async def get_reference_data(
        securities: List[str],
        fields: List[str],
        security_id_type: Optional[SecurityIdType] = None,
        overrides=None,
        session_options: Optional[blpapi.SessionOptions] = None):
    """
    Async API for Bloomberg ReferenceDataRequest
    by default session_options ip: localhost and  port: 8194
    """
    request = ReferenceDataRequest(securities, fields, security_id_type,
                                   overrides)
    if session_options is None:
        session_options = blpapi.SessionOptions()
        session_options.setServerHost("localhost")
        session_options.setServerPort(8194)
    handler = HandlerRef(session_options)
    asyncio.create_task(handler.send_requests([request]))
    asyncio.create_task(request.process())
    return await request.process()
