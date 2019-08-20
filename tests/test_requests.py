import asyncio
import datetime as dt

import pandas as pd
import pytest

from async_blp.requests import HistoricalDataRequest
from async_blp.requests import ReferenceDataRequest


# we need protected access in tests
# pylint: disable=protected-access


class TestReferenceDataRequest:

    def test__init__not_inside_loop(self,
                                    simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        assert request._loop is None
        assert request._msg_queue is None

    @pytest.mark.asyncio
    async def test__set_running_loop_as_default__queue_is_empty(
            self,
            simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        request.set_running_loop_as_default()

        assert request._loop == asyncio.get_running_loop()

    @pytest.mark.asyncio
    async def test__set_running_loop_as_default__queue_is_not_empty(
            self,
            simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])
        request._msg_queue.put_nowait(1)

        with pytest.raises(RuntimeError):
            request.set_running_loop_as_default()

    def test__set_running_loop_as_default__not_inside_loop(
            self,
            simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        with pytest.raises(RuntimeError):
            request.set_running_loop_as_default()

    @pytest.mark.asyncio
    async def test__send_queue_message__inside_loop(self, simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        request.send_queue_message(1)
        await asyncio.sleep(0.001)

        assert request._msg_queue.get_nowait() == 1

    def test__send_queue_message__not_inside_loop(self, simple_field_data):
        field_name, _, security_id = simple_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        with pytest.raises(RuntimeError):
            request.send_queue_message(1)

    @pytest.mark.asyncio
    async def test__process__one_security(self,
                                          response_msg_one_security,
                                          one_value_array_field_data):
        field_name, field_value, security_id = one_value_array_field_data

        request = ReferenceDataRequest([security_id], [field_name])

        request.send_queue_message(response_msg_one_security)
        request.send_queue_message(None)

        expected_df = pd.DataFrame(columns=[field_name], index=[security_id])
        expected_df.at[security_id, field_name] = field_value

        actual_df, _ = await request.process()

        pd.testing.assert_frame_equal(actual_df, expected_df)

    @pytest.mark.asyncio
    async def test__process__several_securities(self,
                                                response_msg_several_securities,
                                                one_value_array_field_data,
                                                simple_field_data):
        field_name_1, field_value_1, security_id_1 = one_value_array_field_data
        field_name_2, field_value_2, security_id_2 = simple_field_data

        request = ReferenceDataRequest([security_id_1, security_id_2],
                                       [field_name_1, field_name_2])

        request.send_queue_message(response_msg_several_securities)
        request.send_queue_message(None)

        expected_df = pd.DataFrame(columns=[field_name_1, field_name_2],
                                   index=[security_id_1, security_id_2])
        expected_df.at[security_id_1, field_name_1] = field_value_1
        expected_df.at[security_id_2, field_name_2] = field_value_2

        actual_df, _ = await request.process()

        pd.testing.assert_frame_equal(actual_df, expected_df)

    @pytest.mark.asyncio
    async def test__process__empty(self, one_value_array_field_data):
        field_name, _, security_id = one_value_array_field_data

        request = ReferenceDataRequest([security_id], [field_name])
        request.send_queue_message(None)

        expected_df = pd.DataFrame(columns=[field_name], index=[security_id])

        actual_df, _ = await request.process()
        pd.testing.assert_frame_equal(actual_df, expected_df)


class TestHistoricalDataRequest:

    def test__weight(self):
        securities = ['security_1', 'security_2', 'security_3']
        fields = ['field_1', 'field_2', 'field_3']
        start_date = dt.date(2018, 1, 1)
        end_date = dt.date(2018, 1, 10)

        request = HistoricalDataRequest(securities, fields,
                                        start_date, end_date)

        assert request.weight == 3 * 3 * 9
