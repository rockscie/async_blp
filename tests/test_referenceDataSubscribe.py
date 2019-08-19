import asyncio

import pytest

from async_blp.requests import SubscribeData
from async_blp.utils.env_test import Message
from async_blp.utils.env_test import SubscriptionList


# pylint does not like pytest.fixture but we do
# pylint: disable=redefined-outer-name
# we need protected access in tests
# pylint: disable=protected-access

@pytest.fixture()
def simple_field_data():
    field_name = 'PX_LAST'
    security_id = 'F Equity'
    return field_name, security_id


@pytest.mark.asyncio
class TestReferenceDataSubscribe:

    def test__create(self,
                     simple_field_data,
                     ):
        """
        for Subscribe you do not need anything more open session
        """
        field_name, security_id = simple_field_data
        sub = SubscribeData([security_id],
                            [field_name])

        assert isinstance(sub.create(), SubscriptionList)

    async def test__process(self,
                            market_data_event):
        """
        simple market_data immediately return all data in queue
        """
        security_id = 'F Equity'
        field_name = 'MKTDATA'
        sub = SubscribeData([security_id],
                            [field_name])
        msg: Message = list(market_data_event)[0]
        cor_id = list(msg.correlationIds())[0]
        sub._ids_sec[cor_id] = security_id
        sub.send_queue_message(msg)
        await asyncio.sleep(0.0001)
        data, errors = await sub.process()
        assert not data.empty
