import asyncio
import logging

from async_blp import AsyncBloomberg
from async_blp.enums import ErrorBehaviour


async def main():
    security_id = 'F US Equity'
    field = 'LAST_PRICE'

    bloomberg = AsyncBloomberg(error_behaviour=ErrorBehaviour.RETURN,
                               log_level=logging.DEBUG)

    data, errors = await bloomberg.get_reference_data([security_id], [field])

    await bloomberg.stop()

    return data, errors


if __name__ == '__main__':
    asyncio.run(main())
