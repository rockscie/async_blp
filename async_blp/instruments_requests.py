"""
This Request can search in bloomberg
"""

import asyncio
from typing import Dict

import pandas as pd

from .base_request import RequestBase
from .enums import ErrorBehaviour
from .errors import BloombergErrors
from .utils import log

# pylint: disable=ungrouped-imports
try:
    import blpapi
except ImportError:
    from async_blp.utils import env_test as blpapi

LOGGER = log.get_logger()


class InstrumentRequestBase(RequestBase):
    """
    all search requests have same response
    """
    service_name = '//blp/instruments'

    response_fields = []

    def __init__(self,
                 query: str,
                 max_results: int = 10,
                 options: Dict[str, str] = None,
                 error_behavior: ErrorBehaviour = ErrorBehaviour.RETURN,
                 loop: asyncio.AbstractEventLoop = None):

        request_options = {
            'query':      query,
            'maxResults': max_results,
            }

        if options:
            request_options.update(options)

        super().__init__(request_options, error_behavior, loop)

        self._query = query
        self._max_results = max_results
        self._options = options

    async def process(self):
        errors = BloombergErrors()
        securities = []

        while True:
            msg = await self._get_message_from_queue()

            if msg is None:
                break
            results = msg.getElement('results')

            for element in results.values():
                response = [element.getElementAsString(field_name)
                            for field_name in self.response_fields]

                securities.append(response)

        data_frame = pd.DataFrame(securities,
                                  columns=self.response_fields)

        return data_frame, errors

    @property
    def weight(self) -> int:
        return self._max_results * len(self.response_fields)


class SecurityLookupRequest(InstrumentRequestBase):
    """
    Non-exhaustive list of possible options (from Bloomberg docs it is
    unclear whether you can use other options or not):
    - yellowKeyFilter
    - languageOverride
    """
    request_name = 'instrumentListRequest'

    response_fields = ['security', 'description']


class CurveLookupRequest(SecurityLookupRequest):
    """
    Non-exhaustive list of possible options (from Bloomberg docs it is
    unclear whether you can use other options or not):
    - bbgid
    - countryCode
    - currencyCode
    - curveid
    - type
    - subtype
    """
    request_name = 'curveListRequest'

    response_fields = [
        'description',
        'country',
        'currency',
        'curveid',
        'type',
        'subtype',
        'publisher',
        'bbgid',
        ]


class GovernmentLookupRequest(SecurityLookupRequest):
    """
    Non-exhaustive list of possible options (from Bloomberg docs it is
    unclear whether you can use other options or not):
    - partialMatch
    - ticker
    - currencyCode
    - curveid
    - type
    - subtype
    """
    request_name = 'govtListRequest'

    response_fields = [
        'parseky',
        'name',
        'ticker',
        ]
