import asyncio
import datetime as dt
import logging

from async_blp import AsyncBloomberg
from async_blp.enums import ErrorBehaviour


async def main(sec=4):
    security_id = 'F US Equity'
    field = 'LAST_PRICE'

    bloomberg = AsyncBloomberg(error_behaviour=ErrorBehaviour.RETURN,
                               log_level=logging.DEBUG)

    await bloomberg.subscribe([security_id], [field])
    start = dt.datetime.now()
    data = []
    while (dt.datetime.now() - start) < dt.timedelta(seconds=sec):
        data.append(await bloomberg.read_subscriptions())
        await asyncio.sleep(1)

    await bloomberg.stop()

    return data


if __name__ == '__main__':
    data = asyncio.run(main())
    print('Data received', data)
