import asyncio
import enum
from typing import Dict
from typing import List
from typing import Optional

try:
    import blpapi
except ImportError:
    from async_blp import env_test as blpapi


class SecurityIdType(enum.Enum):
    """
    Some of the possible security identifier types. For more information see
    https://www.bloomberg.com/professional/support/api-library/
    """
    ticker = '/ticker/'
    isin = '/isin/'
    cusip = '/cusip/'
    sedol = '/sedol/'

    bl_security_identifier = '/bsid/'
    bl_security_symbol = '/bsym/'
    bl_unique_identifier = '/buid/'
    bl_global_identifier = '/bbgid'

    def __str__(self):
        return self.value


class ReferenceDataRequest:
    """
    Convenience wrapper around Bloomberg's ReferenceDataRequest
    """
    service_name = "//blp/refdata"
    request_name = "ReferenceDataRequest"

    def __init__(self,
                 securities: List[str],
                 fields: List[str],
                 security_id_type: Optional[SecurityIdType] = None,
                 overrdies: Optional[Dict] = None):

        self.securities = securities
        self.fields = fields
        self.security_id_type = security_id_type
        self.overrdies = overrdies or {}
        self.msg_queue = asyncio.Queue()

    async def process(self):
        msg = ''

        while msg != 'END':
            msg = await self.msg_queue.get()
            ...

        return Response()

    def create(self, service: blpapi.Service) -> blpapi.Request:
        request = service.createRequest(self.request_name)

        if self.security_id_type is None:
            prefix = ''
        else:
            prefix = str(self.security_id_type)

        for name in self.securities:
            name = prefix + name
            request.getElement("securities").appendValue(name)

        for field in self.fields:
            request.getElement("fields").appendValue(field)

        for key, value in self.overrdies.items():
            request.set(key, value)

        return request
