import asyncio
import logging

from async_blp import AsyncBloomberg
from async_blp.enums import ErrorBehaviour


async def main():
    field = 'LAST_PRICE'

    bloomberg = AsyncBloomberg(error_behaviour=ErrorBehaviour.RETURN,
                               log_level=logging.DEBUG)

    data = await bloomberg.search_fields([field])

    await bloomberg.stop()

    return data


if __name__ == '__main__':
    data = asyncio.run(main())
    print('Data received', data)
