import pandas as pd
import pytest

from async_blp.instruments_requests import InstrumentRequestBase


@pytest.mark.asyncio
class TestInstrumentRequestBase:

    def test__weight(self):
        request = InstrumentRequestBase('query', max_results=5)
        request.response_fields = ['field_1', 'field_2']

        assert request.weight == 10

    async def test__process(self, security_lookup_msg):
        request = InstrumentRequestBase('query', max_results=5)
        request.response_fields = ['security', 'description']

        request.send_queue_message(security_lookup_msg)
        request.send_queue_message(None)

        data, _ = await request.process()

        expected_data = pd.DataFrame([['F US Equity', 'Ford Motors Co']],
                                     columns=['security', 'description'])

        pd.testing.assert_frame_equal(expected_data, data)
