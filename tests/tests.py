# Standard Imports
import os
import unittest
from unittest.mock import (
    patch,
    Mock
)

# 3rd-Party Imports
import pandas as pd
import smartsheet
import smartsheet.sheets
from dotenv import load_dotenv

# Local Imports
from src.smartsheet_dataframe.smartsheet_dataframe import (
    get_report_as_df,
    get_sheet_as_df,
    get_as_df,
    _do_request
)

load_dotenv()


class TestGetReportAsDf(unittest.TestCase):

    @patch('src.smartsheet_dataframe.smartsheet_dataframe._get_from_request')
    def test_get_report_as_df_with_token_and_report_id(self, mock_get_from_request):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        df = get_report_as_df(token="fake_token", report_id=12345)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")

    @patch('warnings.warn')
    @patch('src.smartsheet_dataframe.smartsheet_dataframe._get_from_request')
    def test_get_report_as_df_with_both_token_and_report_obj(self, mock_get_from_request, mock_warn):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        mock_report_obj = Mock()
        mock_report_obj.to_dict.return_value = mock_response

        df = get_report_as_df(token="fake_token", report_id=12345, report_obj=mock_report_obj)

        mock_warn.assert_called_with("A 'report_id' has been provided along with a 'report_obj' \n" +
                                     "The 'sheet_id' parameter will be ignored")

    def test_get_report_as_df_without_token_or_report_obj(self):
        with self.assertRaises(ValueError) as context:
            get_report_as_df()

    def test_get_report_as_df_token_without_report_id_but_token_is_report_obj(self):
        with self.assertRaises(ValueError) as context:
            get_report_as_df(token=smartsheet.models.Report())

    def test_get_report_as_df_token_without_report_id(self):
        with self.assertRaises(ValueError) as context:
            get_report_as_df(token="test")


class TestGetSheetAsDf(unittest.TestCase):
    @patch('src.smartsheet_dataframe.smartsheet_dataframe._get_from_request')
    def test_get_sheet_as_df_with_token_and_sheet_id(self, mock_get_from_request):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        df = get_sheet_as_df(token="fake_token", sheet_id=12345)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")

    @patch('warnings.warn')
    @patch('src.smartsheet_dataframe.smartsheet_dataframe._get_from_request')
    def test_get_sheet_as_df_with_both_token_and_sheet_obj(self, mock_get_from_request, mock_warn):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        mock_sheet_obj = Mock()
        mock_sheet_obj.to_dict.return_value = mock_response

        df = get_sheet_as_df(token="fake_token", sheet_id=12345, sheet_obj=mock_sheet_obj)

        mock_warn.assert_called_with("A 'sheet_id' has been provided along with a 'sheet_obj' \n" +
                                     "The 'sheet_id' parameter will be ignored")

    def test_get_sheet_as_df_without_token_or_sheet_obj(self):
        with self.assertRaises(ValueError) as context:
            get_sheet_as_df()

    def test_get_sheet_as_df_token_without_sheet_id_but_token_is_sheet_obj(self):
        with self.assertRaises(ValueError) as context:
            get_sheet_as_df(token=smartsheet.models.Sheet())

    def test_get_sheet_as_df_token_without_sheet_id(self):
        with self.assertRaises(ValueError) as context:
            get_sheet_as_df(token="test")


class TestGetAsDf(unittest.TestCase):

    def test_get_as_df_with_report_obj(self):
        mock_report_obj = Mock()
        mock_report_obj.to_dict.return_value = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = get_as_df(type_="report", obj=mock_report_obj)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")

    def test_get_as_df_with_sheet_obj(self):
        mock_sheet_obj = Mock()
        mock_sheet_obj.to_dict.return_value = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = get_as_df(type_="sheet", obj=mock_sheet_obj)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")

    def test_get_as_df_without_token_or_obj(self):
        with self.assertRaises(ValueError) as context:
            get_as_df(type_="test")

    def test_get_as_df_token_without_id(self):
        with self.assertRaises(ValueError) as context:
            get_as_df(type_="test", token="test")


class TestDoRequest(unittest.TestCase):

    @patch('src.smartsheet_dataframe.smartsheet_dataframe.requests.get')
    def test_do_request_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "some_data"}
        mock_get.return_value = mock_response

        response = _do_request(url="http://fakeurl.com", options={})

        self.assertEqual(response.json(), {"data": "some_data"})

    @patch('src.smartsheet_dataframe.smartsheet_dataframe.requests.get')
    def test_do_request_rate_limit(self, mock_get):
        mock_response_rate_limit = Mock()
        mock_response_rate_limit.status_code = 429
        mock_response_rate_limit.json.return_value = {"errorCode": 4004}

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"data": "some_data"}

        mock_get.side_effect = [mock_response_rate_limit] * 3 + [mock_response_success]

        response = _do_request(url="http://fakeurl.com", options={}, retries=4)

        self.assertEqual(response.json(), {"data": "some_data"})

    @patch('src.smartsheet_dataframe.smartsheet_dataframe.requests.get')
    def test_do_request_rate_limit_failure(self, mock_get):
        mock_response_rate_limit = Mock()
        mock_response_rate_limit.status_code = 429
        mock_response_rate_limit.json.return_value = {"errorCode": 4004}

        mock_get.return_value = mock_response_rate_limit

        with self.assertRaises(Exception) as context:
            _do_request(url="http://fakeurl.com", options={}, retries=3)

        self.assertTrue('Could not retrieve request after retrying' in str(context.exception))


@unittest.skipIf(bool(os.getenv('skip_tests', "true") == "true"), "Not testing API calls at this time")
class SheetAPICallsTest(unittest.TestCase):
    def setUp(self):
        import config
        self.token = config.smartsheet_access_token
        self.sheet_id = config.sheet_id
        self.report_id = config.report_id
        self.sheet_client = smartsheet.Smartsheet(self.token)
        self.sheet_obj = self.sheet_client.Sheets.get_sheet(self.sheet_id, include=['objectValue'], level=1)

    def test_object_and_request_are_equal(self):
        df1 = get_sheet_as_df(token=self.token, sheet_id=self.sheet_id)
        df2 = get_sheet_as_df(sheet_obj=self.sheet_obj)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    def test_generic_vs_specific_requests(self):
        df1 = get_sheet_as_df(token=self.token, sheet_id=self.sheet_id)
        df2 = get_as_df(type_='sheet', token=self.token, id_=self.sheet_id)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    def test_generic_vs_specific_object(self):
        df1 = get_sheet_as_df(sheet_obj=self.sheet_obj)
        df2 = get_as_df(type_='sheet', obj=self.sheet_obj)

        self.assertTrue(df1.to_dict() == df2.to_dict())


@unittest.skipIf(bool(os.getenv('skip_tests', "true") == "true"), "Not testing API calls at this time")
class ReportAPICallsTest(unittest.TestCase):
    def setUp(self):
        import config
        self.token = config.smartsheet_access_token
        self.report_id = config.report_id
        self.sheet_client = smartsheet.Smartsheet(self.token)
        self.report_obj = self.sheet_client.Reports.get_report(self.report_id)

    def test_report_object_and_request_are_equal(self):
        df1 = get_report_as_df(token=self.token, report_id=self.report_id)
        df2 = get_report_as_df(report_obj=self.report_obj)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    def test_generic_vs_specific_requests(self):
        df1 = get_report_as_df(token=self.token, report_id=self.report_id)
        df2 = get_as_df(type_='report', token=self.token, id_=self.report_id)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    def test_generic_vs_specific_object(self):
        df1 = get_report_as_df(report_obj=self.report_obj)
        df2 = get_as_df(type_='report', obj=self.report_obj)

        self.assertTrue(df1.to_dict() == df2.to_dict())


if __name__ == "__main__":
    unittest.main()
