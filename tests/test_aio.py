# Standard Imports
import os
import unittest
from unittest.mock import patch

try:
    from unittest.mock import AsyncMock as Mock
except ImportError:
    from unittest.mock import Mock

try:
    from unittest import IsolatedAsyncioTestCase as AsyncTestCase
except ImportError:
    from unittest import TestCase as AsyncTestCase

# 3rd-Party Imports
import pandas as pd
import smartsheet
from dotenv import load_dotenv

# Local Imports
from src.smartsheet_dataframe.aio import (
    get_report_as_df,
    get_sheet_as_df,
    get_as_df,
    _async_do_request
)

load_dotenv()


class TestGetReportAsDf(AsyncTestCase):

    @patch('src.smartsheet_dataframe.aio._async_get_from_request')
    async def test_get_report_as_df_with_token_and_report_id(self, mock_get_from_request):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        df = await get_report_as_df(token="fake_token", report_id=12345)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")

    @patch('warnings.warn')
    @patch('src.smartsheet_dataframe.aio._async_get_from_request')
    async def test_get_report_as_df_with_both_token_and_report_obj(self, mock_get_from_request, mock_warn):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        mock_report_obj = Mock()
        mock_report_obj.to_dict.return_value = mock_response

        df = await get_report_as_df(token="fake_token", report_id=12345, report_obj=mock_report_obj)

        mock_warn.assert_called_with("A 'report_id' has been provided along with a 'report_obj' \n" +
                                     "The 'sheet_id' parameter will be ignored")

    async def test_get_report_as_df_without_token_or_report_obj(self):
        with self.assertRaises(ValueError) as context:
            await get_report_as_df()

    async def test_get_report_as_df_token_without_report_id_but_token_is_report_obj(self):
        with self.assertRaises(ValueError) as context:
            await get_report_as_df(token=smartsheet.models.Report())

    async def test_get_report_as_df_token_without_report_id(self):
        with self.assertRaises(ValueError) as context:
            await get_report_as_df(token="test")


class TestGetSheetAsDf(AsyncTestCase):
    @patch('src.smartsheet_dataframe.aio._async_get_from_request')
    async def test_get_sheet_as_df_with_token_and_sheet_id(self, mock_get_from_request):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        df = await get_sheet_as_df(token="fake_token", sheet_id=12345)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")

    @patch('warnings.warn')
    @patch('src.smartsheet_dataframe.aio._async_get_from_request')
    async def test_get_sheet_as_df_with_both_token_and_sheet_obj(self, mock_get_from_request, mock_warn):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        mock_sheet_obj = Mock()
        mock_sheet_obj.to_dict.return_value = mock_response

        df = await get_sheet_as_df(token="fake_token", sheet_id=12345, sheet_obj=mock_sheet_obj)

        mock_warn.assert_called_with("A 'sheet_id' has been provided along with a 'sheet_obj' \n" +
                                     "The 'sheet_id' parameter will be ignored")

    async def test_get_sheet_as_df_without_token_or_sheet_obj(self):
        with self.assertRaises(ValueError) as context:
            await get_sheet_as_df()

    async def test_get_sheet_as_df_token_without_sheet_id_but_token_is_sheet_obj(self):
        with self.assertRaises(ValueError) as context:
            await get_sheet_as_df(token=smartsheet.models.Sheet())

    async def test_get_sheet_as_df_token_without_sheet_id(self):
        with self.assertRaises(ValueError) as context:
            await get_sheet_as_df(token="test")


class TestGetAsDf(AsyncTestCase):

    async def test_get_as_df_with_report_obj(self):
        mock_report_obj = unittest.mock.Mock()
        mock_report_obj.to_dict.return_value = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = await get_as_df(type_="report", obj=mock_report_obj)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")

    async def test_get_as_df_with_sheet_obj(self):
        mock_sheet_obj = unittest.mock.Mock()
        mock_sheet_obj.to_dict.return_value = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = await get_as_df(type_="sheet", obj=mock_sheet_obj)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")

    async def test_get_as_df_without_token_or_obj(self):
        with self.assertRaises(ValueError) as context:
            await get_as_df(type_="test")

    async def test_get_as_df_token_without_id(self):
        with self.assertRaises(ValueError) as context:
            await get_as_df(type_="test", token="test")


class TestDoRequest(AsyncTestCase):

    @patch('src.smartsheet_dataframe.aio.aiohttp.ClientSession.get')
    async def test_do_request_success(self, mock_get):
        mock_response = Mock()
        mock_get.return_value = mock_response
        mock_get.return_value.__aenter__.return_value.status = 200
        mock_get.return_value.__aenter__.return_value.json.return_value = {"data": "some_data"}

        response = await _async_do_request(url="http://fakeurl.com", options={})

        self.assertEqual(await response.json(), {"data": "some_data"})

    @patch('src.smartsheet_dataframe.aio.aiohttp.ClientSession.get')
    async def test_do_request_rate_limit(self, mock_get):
        mock_response_rate_limit = Mock()
        mock_response_rate_limit.status = 429
        mock_response_rate_limit.json.return_value = {"errorCode": 4004}
        mock_response_rate_limit.text.return_value = "some_data"

        mock_response_success = Mock()
        mock_response_success.status = 200
        mock_response_success.json.return_value = {"data": "some_data"}

        mock_get.side_effect = [mock_response_rate_limit] * 3 + [mock_response_success]

        response = await _async_do_request(url="http://fakeurl.com", options={}, retries=4)

        self.assertEqual(response.json(), {"data": "some_data"})

    @patch('src.smartsheet_dataframe.aio.aiohttp.ClientSession.get')
    async def test_do_request_rate_limit_failure(self, mock_get):
        mock_response_rate_limit = Mock()
        mock_response_rate_limit.status = 429
        mock_response_rate_limit.json.return_value = {"errorCode": 4004}
        mock_response_rate_limit.text.return_value = "some text"

        mock_get.return_value = mock_response_rate_limit

        with self.assertRaises(Exception) as context:
            await _async_do_request(url="http://fakeurl.com", options={}, retries=3)

        self.assertTrue('Could not retrieve request after retrying' in str(context.exception))


@unittest.skipIf(bool(os.getenv('skip_tests', "true") == "true"), "Not testing API calls at this time")
class SheetAPICallsTest(AsyncTestCase):
    def setUp(self):
        import config
        self.token = config.smartsheet_access_token
        self.sheet_id = config.sheet_id
        self.report_id = config.report_id

    async def test_generic_vs_specific_requests(self):
        df1 = await get_sheet_as_df(token=self.token, sheet_id=self.sheet_id)
        df2 = await get_as_df(type_='sheet', token=self.token, id_=self.sheet_id)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    async def test_get_sheet_as_df(self):
        df = await get_sheet_as_df(token=self.token, sheet_id=self.sheet_id)

        self.assertEqual(df["Primary Column"].iloc[0], "This is a row in the sheet")

    async def test_get_as_df(self):
        df = await get_as_df(type_="sheet", token=self.token, id_=self.sheet_id)

        self.assertEqual(df["Primary Column"].iloc[0], "This is a row in the sheet")


@unittest.skipIf(bool(os.getenv('skip_tests', "true") == "true"), "Not testing API calls at this time")
class ReportAPICallsTest(AsyncTestCase):
    def setUp(self):
        import config
        self.token = config.smartsheet_access_token
        self.report_id = config.report_id

    async def test_generic_vs_specific_requests(self):
        df1 = await get_report_as_df(token=self.token, report_id=self.report_id)
        df2 = await get_as_df(type_='report', token=self.token, id_=self.report_id)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    async def test_get_report_as_df(self):
        df = await get_report_as_df(token=self.token, report_id=self.report_id)

        self.assertEqual(df["Primary"].iloc[0], "This is a row in the sheet")

    async def test_get_as_df(self):
        df = await get_as_df(type_='report', token=self.token, id_=self.report_id)

        self.assertEqual(df["Primary"].iloc[0], "This is a row in the sheet")


if __name__ == "__main__":
    unittest.main()
