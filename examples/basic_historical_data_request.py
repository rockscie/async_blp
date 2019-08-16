import asyncio
import datetime as dt
import logging

from async_blp import AsyncBloomberg
from async_blp.enums import ErrorBehaviour


async def main():
    security_id = 'F US Equity'
    field = 'PX_LAST'
    start_date = dt.date(2019, 1, 1)
    end_date = dt.date(2019, 3, 1)

    bloomberg = AsyncBloomberg(error_behaviour=ErrorBehaviour.RETURN,
                               log_level=logging.DEBUG)

    data, errors = await bloomberg.get_historical_data([security_id],
                                                       [field],
                                                       start_date,
                                                       end_date)

    await bloomberg.stop()

    return data, errors


if __name__ == '__main__':
    asyncio.run(main())
