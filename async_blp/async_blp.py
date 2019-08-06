from typing import List

from async_blp.requests import ReferenceDataRequest
from async_blp.requests import SecurityIdType


async def send_reference_request(securities: List[str],
                                 fields: List[str],
                                 security_id_type: SecurityIdType = None,
                                 overrides=None):
    request = ReferenceDataRequest(securities, fields, security_id_type,
                                   overrides)


async def send_historical_request():
    pass
