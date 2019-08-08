import asyncio
import datetime as dt
import enum
from typing import Dict
from typing import List
from typing import Optional
from typing import Tuple
from typing import Union

import pandas as pd

try:
    import blpapi
except ImportError:
    from async_blp import env_test as blpapi

REFERENCE_DATA_RESPONSE = blpapi.Name("ReferenceDataResponse")
HISTORICAL_DATA_RESPONSE = blpapi.Name("HistoricalDataResponse")
MARKET_DATA_EVENTS = blpapi.Name("MarketDataEvents")
RESPONSE_ERROR = blpapi.Name("responseError")
SECURITY_DATA = blpapi.Name("securityData")
SECURITY_ERROR = blpapi.Name("securityError")
ID = blpapi.Name("id")
ERROR_INFO = blpapi.Name("errorInfo")
MESSAGE = blpapi.Name("message")
FIELD_ID = blpapi.Name('fieldId')
FIELD_MNEMONIC = blpapi.Name("mnemonic")
FIELD_DATA = blpapi.Name("fieldData")
FIELD_INFO = blpapi.Name('fieldInfo')
FIELD_DATA_TYPE = blpapi.Name('datatype')
FIELD_DESC = blpapi.Name("description")
FIELD_DOC = blpapi.Name("documentation")
SECURITY = blpapi.Name("security")
CATEGORY = blpapi.Name("category")
CATEGORY_NAME = blpapi.Name("categoryName")
CATEGORY_ID = blpapi.Name("categoryId")
EXCLUDE = blpapi.Name("exclude")
FIELD_EXCEPTIONS = blpapi.Name('fieldExceptions')

BloombergValue = Union[str, int, float, dt.date, dt.datetime,
                       Dict[str, Union[str, int, float, dt.date, dt.datetime]]]


class SecurityIdType(enum.Enum):
    """
    Some of the possible security identifier types. For more information see
    https://www.bloomberg.com/professional/support/api-library/
    """
    TICKER = '/ticker/'
    ISIN = '/isin/'
    CUSIP = '/cusip/'
    SEDOL = '/sedol/'

    BL_SECURITY_IDENTIFIER = '/bsid/'
    BL_SECURITY_SYMBOL = '/bsym/'
    BL_UNIQUE_IDENTIFIER = '/buid/'
    BL_GLOBAL_IDENTIFIER = '/bbgid'

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
                 overrides: Optional[Dict] = None):

        self.securities = securities
        self.fields = fields
        self.security_id_type = security_id_type
        self.overrides = overrides or {}
        self.msg_queue = asyncio.Queue()

    async def process(self) -> pd.DataFrame:
        dataframes = []

        while True:
            msg = await self.msg_queue.get()

            if msg == blpapi.Event.RESPONSE:
                break
            try:
                msg_data = list(msg.getElement(SECURITY_DATA).values())

                msg_frames = [self._parse_security_data(security_data)
                              for security_data in msg_data]

                dataframes.extend(msg_frames)
            except KeyError:
                print("we can't parse it")

        if not dataframes:
            return pd.DataFrame()
        return pd.concat(dataframes, axis=0)

    def _parse_security_data(self, security_data) -> pd.DataFrame:
        security_id = security_data.getElementAsString(SECURITY)
        security_errors = security_data.getElement(SECURITY_ERROR)
        field_errors = security_data.getElement(FIELD_EXCEPTIONS)
        # todo clean security id, save errors

        data: blpapi.Element = security_data.getElement(FIELD_DATA)

        field_data = list(data.elements())

        security_df = pd.DataFrame()

        for field in field_data:
            field_name, field_value = self._parse_field_data(field)
            security_df.loc[security_id, field_name] = field_value

        return security_df

    def _parse_field_data(self,
                          field: blpapi.Element,
                          ) -> Tuple[str, List[BloombergValue]]:  # fix typing

        if field.isArray():
            return self._parse_array_field(field)

        else:
            field_name = str(field.name())
            field_value = field.getValue()
            return field_name, field_value

    def _parse_array_field(self, field: blpapi.Element):
        field_name = str(field.name())

        values = [
            {
                str(e2.name()): e2.getValue()
                for e2 in e1.elements()
                }
            for elem in field.elements()
            for e1 in elem.values()
            ]

        if values and len(values[0]) == 1:
            values = [list(value.values())[0]
                      for value in values]

        return field_name, values

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

        for key, value in self.overrides.items():
            request.set(key, value)

        return request
