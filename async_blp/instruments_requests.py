import asyncio
from typing import Dict
from typing import Tuple

import blpapi
import pandas as pd

from .enums import ErrorBehaviour
from .errors import BloombergErrors
from .requests import ReferenceDataRequest
from .utils import log

LOGGER = log.get_logger()


class SecurityLookupRequest(ReferenceDataRequest):
    """
    Non-exhaustive list of possible options (from Bloomberg docs it is
    unclear whether you can use other options or not):
    - yellowKeyFilter
    - languageOverride
    """
    service_name = '//blp/instruments'
    request_name = 'instrumentListRequest'

    def __init__(self,
                 query: str,
                 options: Dict[str, str],
                 max_results: int = 10,
                 error_behavior: ErrorBehaviour = ErrorBehaviour.RETURN,
                 loop: asyncio.AbstractEventLoop = None):

        super().__init__([], [], None, {}, error_behavior, loop)

        self._max_results = max_results
        self._options = options
        self._query = query

    def create(self, service: blpapi.Service) -> blpapi.Request:
        bloomberg_request = super().create(service)

        bloomberg_request.set('query', self._query)

        if self._max_results:
            bloomberg_request.set('maxResults', self._max_results)

        return bloomberg_request

    async def process(self) -> Tuple[pd.DataFrame, BloombergErrors]:
        errors = BloombergErrors()
        securities = []

        while True:

            LOGGER.debug('%s: waiting for messages', self.__class__.__name__)
            msg = await self._msg_queue.get()

            if msg is None:
                LOGGER.debug('%s: last message received, processing is '
                             'finished',
                             self.__class__.__name__)
                break

            LOGGER.debug('%s: message received', self.__class__.__name__)

            results = msg.getElement('results')

            for element in results.values():
                security = element.getElementAsString('security')
                description = element.getElementAsString('description')

                securities.append((security, description))

        data_frame = pd.DataFrame(securities,
                                  columns=['security', 'description'])

        return data_frame, errors

    def _get_empty_df(self):
        return pd.DataFrame(columns=['security', 'description'])


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

    async def process(self) -> Tuple[pd.DataFrame, BloombergErrors]:
        errors = BloombergErrors()
        securities = []

        while True:

            LOGGER.debug('%s: waiting for messages', self.__class__.__name__)
            msg = await self._msg_queue.get()

            if msg is None:
                LOGGER.debug('%s: last message received, processing is '
                             'finished',
                             self.__class__.__name__)
                break

            LOGGER.debug('%s: message received', self.__class__.__name__)

            results = msg.getElement('results')

            for element in results.values():
                description = element.getElementAsString('description')
                country = element.getElementAsString('country')
                currency = element.getElementAsString('country')
                curveid = element.getElementAsString('curveid')
                type_ = element.getElementAsString('type')
                subtype = element.getElementAsString('subtype')
                publisher = element.getElementAsString('publisher')
                bbgid = element.getElementAsString('bbgid')

                securities.append((
                    description,
                    country,
                    currency,
                    curveid,
                    type_,
                    subtype,
                    publisher,
                    bbgid,
                    ))

        data_frame = pd.DataFrame(securities,
                                  columns=[
                                      'description',
                                      'country',
                                      'country',
                                      'curveid',
                                      'type',
                                      'subtype',
                                      'publisher',
                                      'bbgid',
                                      ])

        return data_frame, errors


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

    async def process(self) -> Tuple[pd.DataFrame, BloombergErrors]:
        errors = BloombergErrors()
        securities = []

        while True:

            LOGGER.debug('%s: waiting for messages', self.__class__.__name__)
            msg = await self._msg_queue.get()

            if msg is None:
                LOGGER.debug('%s: last message received, processing is '
                             'finished',
                             self.__class__.__name__)
                break

            LOGGER.debug('%s: message received', self.__class__.__name__)

            results = msg.getElement('results')

            for element in results.values():
                parseky = element.getElementAsString('parseky')
                name = element.getElementAsString('name')
                ticker = element.getElementAsString('ticker')

                securities.append((
                    parseky,
                    name,
                    ticker,
                    ))

        data_frame = pd.DataFrame(securities,
                                  columns=[
                                      'parseky',
                                      'name',
                                      'ticker',
                                      ])

        return data_frame, errors
