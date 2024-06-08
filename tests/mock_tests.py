# Standard Imports
import unittest
from unittest.mock import (
    patch,
    Mock
)

# 3rd-Party Imports
import pandas as pd
import smartsheet.sheets

# Local Imports
from src.smartsheet_dataframe.smartsheet_dataframe import (
    get_report_as_df,
    get_sheet_as_df,
    get_as_df,
    _do_request,
    _to_dataframe
)


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


class TestToDataFrame(unittest.TestCase):

    def test_to_dataframe_empty_sheet(self):
        mock_object_dict = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": []
        }

        df = _to_dataframe(mock_object_dict)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)

    def test_to_dataframe_with_data(self):
        mock_object_dict = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "parentId": 0, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = _to_dataframe(mock_object_dict)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")
        self.assertEqual(df.loc[0, "Column2"], "Value2")


if __name__ == '__main__':
    unittest.main()
