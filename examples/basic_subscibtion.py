import asyncio
import datetime as dt
import logging

from async_blp import AsyncBloomberg
from async_blp.enums import ErrorBehaviour


async def main(sec=4):
    security_id = 'F US Equity'
    field = 'PX_PRICE'

    bloomberg = AsyncBloomberg(error_behaviour=ErrorBehaviour.RETURN,
                               log_level=logging.DEBUG)

    await bloomberg.add_subscriber([security_id], [field])
    start = dt.datetime.now()
    data = []
    while (dt.datetime.now() - start) < dt.timedelta(seconds=sec):
        data.append(await bloomberg.read_subscriber())

    await bloomberg.stop()

    return data


if __name__ == '__main__':
    data = asyncio.run(main())
    print('Data received', data)
