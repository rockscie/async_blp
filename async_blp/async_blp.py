from typing import List
from typing import Optional

from async_blp.handler_refdata import HandlerRef
from async_blp.requests import ReferenceDataRequest
from async_blp.requests import SecurityIdType


async def get_reference_data(
        securities: List[str],
        fields: List[str],
        security_id_type: Optional[SecurityIdType] = None,
        overrides=None):
    """
    Async API for Bloomberg ReferenceDataRequest
    """
    request = ReferenceDataRequest(securities, fields, security_id_type,
                                   overrides)

    handler = HandlerRef()
    handler.send_requests([request])
    await handler.complete_event.wait()

    # todo process received data
