# Standard Imports
import builtins
import sys
from unittest.mock import (
    patch,
    Mock
)

# 3rd-Party Imports
import pandas as pd
import pytest
import smartsheet

# Local Imports
from smartsheet_dataframe import (
    get_report_as_df,
    get_sheet_as_df,
    get_as_df,
)
from smartsheet_dataframe.smartsheet_dataframe import (
    _do_request,
    _to_dataframe
)


@pytest.mark.skip("Not testing API calls at this time")
class TestSheet:
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

        assert df1.to_dict() == df2.to_dict()

    def test_generic_vs_specific_requests(self):
        df1 = get_sheet_as_df(token=self.token, sheet_id=self.sheet_id)
        df2 = get_as_df(type_='sheet', token=self.token, id_=self.sheet_id)

        assert df1.to_dict() == df2.to_dict()

    def test_generic_vs_specific_object(self):
        df1 = get_sheet_as_df(sheet_obj=self.sheet_obj)
        df2 = get_as_df(type_='sheet', obj=self.sheet_obj)

        assert df1.to_dict() == df2.to_dict()


@pytest.mark.skip("Not testing API calls at this time")
class TestReport:
    def setUp(self):
        import config
        self.token = config.smartsheet_access_token
        self.report_id = config.report_id
        self.sheet_client = smartsheet.Smartsheet(self.token)
        self.report_obj = self.sheet_client.Reports.get_report(self.report_id)

    def test_report_object_and_request_are_equal(self):
        df1 = get_report_as_df(token=self.token, report_id=self.report_id)
        df2 = get_report_as_df(report_obj=self.report_obj)

        assert df1.to_dict() == df2.to_dict()

    def test_generic_vs_specific_requests(self):
        df1 = get_report_as_df(token=self.token, report_id=self.report_id)
        df2 = get_as_df(type_='report', token=self.token, id_=self.report_id)

        assert df1.to_dict() == df2.to_dict()

    def test_generic_vs_specific_object(self):
        df1 = get_report_as_df(report_obj=self.report_obj)
        df2 = get_as_df(type_='report', obj=self.report_obj)

        assert df1.to_dict() == df2.to_dict()


class TestGetReportAsDf:
    @patch('smartsheet_dataframe.smartsheet_dataframe._get_from_request')
    def test_get_report_as_df_with_token_and_report_id(self, mock_get_from_request):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        df = get_report_as_df(token="fake_token", report_id=12345)

        assert isinstance(df, pd.DataFrame)
        assert "Column1" in df.columns
        assert "Column2" in df.columns
        assert df.loc[0, "Column1"] == "Value1"

    @patch('warnings.warn')
    @patch('smartsheet_dataframe.smartsheet_dataframe._get_from_request')
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
        with pytest.raises(ValueError):
            get_report_as_df()

    def test_get_report_as_df_token_without_report_id_but_token_is_report_obj(self):
        with pytest.raises(ValueError):
            get_report_as_df(token=smartsheet.models.Report())  # type: ignore

    def test_get_report_as_df_token_without_report_id(self):
        with pytest.raises(ValueError):
            get_report_as_df(token="test")


class TestGetSheetAsDf:
    @patch('smartsheet_dataframe.smartsheet_dataframe._get_from_request')
    def test_sheet_id__with_token(self, mock_get_from_request):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        df = get_sheet_as_df(token="fake_token", sheet_id=12345)

        assert isinstance(df, pd.DataFrame)
        assert "Column1" in df.columns
        assert "Column2" in df.columns
        assert df.loc[0, "Column1"] in "Value1"

    @patch('smartsheet_dataframe.smartsheet_dataframe._get_from_request')
    def test_sheet_obj(self, mock_get_from_request):
        mock_response = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }
        mock_get_from_request.return_value = mock_response

        mock_sheet_obj = Mock()
        mock_sheet_obj.to_dict.return_value = mock_response

        df = get_sheet_as_df(sheet_obj=mock_sheet_obj)

        assert isinstance(df, pd.DataFrame)
        assert "Column1" in df.columns
        assert "Column2" in df.columns
        assert df.loc[0, "Column1"] in "Value1"

    @patch('warnings.warn')
    @patch('smartsheet_dataframe.smartsheet_dataframe._get_from_request')
    def test_sheet_id_and_obj__with_token(self, mock_get_from_request, mock_warn):
        """Ensure that a warning is raised if both sheet_id and sheet_obj are provided
        and that the sheet_id is ignored."""

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

        assert isinstance(df, pd.DataFrame)
        assert "Column1" in df.columns
        assert "Column2" in df.columns
        assert df.loc[0, "Column1"] in "Value1"

    @patch.dict(sys.modules, {'smartsheet': None})
    def test_sheet_obj__with_token__missing_smartsheet_import(self):
        with pytest.raises(ValueError) as e:
            get_sheet_as_df(token="fake_token", sheet_obj=smartsheet.models.Sheet())

        assert "A sheet_id must be included in the parameters if a token is provided" in str(e.value)

    def test_sheet_obj_with_token(self):
        with pytest.raises(ValueError) as e:
            get_sheet_as_df(token="fake_token", sheet_obj=smartsheet.models.Sheet())

        assert "A sheet_id must be included in the parameters if a token is provided" in str(e.value)

    def test_without_token_or_sheet_obj(self):
        with pytest.raises(ValueError) as e:
            get_sheet_as_df()

        assert "One of 'token' or 'sheet_obj' must be included in parameters" in str(e.value)

    def test_token__without_sheet_id_but_token_is_sheet_obj(self):
        with pytest.raises(ValueError) as e:
            get_sheet_as_df(token=smartsheet.models.Sheet())  # type: ignore

        assert "Function must be called with the 'sheet_obj=' keyword argument" in str(e.value)

    def test_token__without_sheet_id(self):
        with pytest.raises(ValueError) as e:
            get_sheet_as_df(token="test")

        assert "A sheet_id must be included in the parameters if a token is provided" in str(e.value)


class TestGetAsDf:

    def test_get_as_df_with_report_obj(self):
        mock_report_obj = Mock()
        mock_report_obj.to_dict.return_value = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = get_as_df(type_="report", obj=mock_report_obj)

        assert isinstance(df, pd.DataFrame)
        assert "Column1" in df.columns
        assert "Column2" in df.columns
        assert df.loc[0, "Column1"] == "Value1"

    def test_get_as_df_with_sheet_obj(self):
        mock_sheet_obj = Mock()
        mock_sheet_obj.to_dict.return_value = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = get_as_df(type_="sheet", obj=mock_sheet_obj)

        assert isinstance(df, pd.DataFrame)
        assert "Column1" in df.columns
        assert "Column2" in df.columns
        assert df.loc[0, "Column1"] == "Value1"

    def test_get_as_df_without_token_or_obj(self):
        with pytest.raises(ValueError):
            get_as_df(type_="test")

    def test_get_as_df_token_without_id(self):
        with pytest.raises(ValueError):
            get_as_df(type_="test", token="test")


class TestDoRequest:

    @patch('smartsheet_dataframe.smartsheet_dataframe.requests.get')
    def test_do_request_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "some_data"}
        mock_get.return_value = mock_response

        response = _do_request(url="https://fakeurl.com", options={})

        assert response.json() == {"data": "some_data"}

    @patch("smartsheet_dataframe.smartsheet_dataframe.time")
    @patch('smartsheet_dataframe.smartsheet_dataframe.requests.get')
    def test_do_request_rate_limit(self, mock_get, mock_time):
        mock_time.sleep.return_value = None

        mock_response_rate_limit = Mock()
        mock_response_rate_limit.status_code = 429
        mock_response_rate_limit.json.return_value = {"errorCode": 4004}

        mock_response_success = Mock()
        mock_response_success.status_code = 200
        mock_response_success.json.return_value = {"data": "some_data"}

        mock_get.side_effect = [mock_response_rate_limit] * 3 + [mock_response_success]

        response = _do_request(url="https://fakeurl.com", options={}, retries=4)

        assert response.json() == {"data": "some_data"}

    @patch("smartsheet_dataframe.smartsheet_dataframe.time")
    @patch('smartsheet_dataframe.smartsheet_dataframe.requests.get')
    def test_do_request_rate_limit_failure(self, mock_get, mock_time):
        mock_time.sleep.return_value = None

        mock_response_rate_limit = Mock()
        mock_response_rate_limit.status_code = 429
        mock_response_rate_limit.json.return_value = {"errorCode": 4004}

        mock_get.return_value = mock_response_rate_limit

        with pytest.raises(Exception) as e:
            _do_request(url="https://fakeurl.com", options={}, retries=3)

        assert 'Could not retrieve request after retrying' in str(e.value)


class TestToDataFrame:

    def test_to_dataframe_empty_sheet(self):
        mock_object_dict = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": []
        }

        df = _to_dataframe(mock_object_dict)

        assert isinstance(df, pd.DataFrame)
        assert df.empty is True
        assert "Column1" in df.columns
        assert "Column2" in df.columns

    def test_to_dataframe_with_data(self):
        mock_object_dict = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "parentId": 0, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = _to_dataframe(mock_object_dict)

        assert isinstance(df, pd.DataFrame)
        assert "Column1" in df.columns
        assert "Column2" in df.columns
        assert df.loc[0, "Column1"] == "Value1"
        assert df.loc[0, "Column2"] == "Value2"
