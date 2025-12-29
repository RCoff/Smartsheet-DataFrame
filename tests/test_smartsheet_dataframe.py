# Standard Imports
import os
from unittest.mock import (
    patch,
    Mock
)

# 3rd-Party Imports
import pandas as pd
import pytest
from dotenv import load_dotenv

# Local Imports
from smartsheet_dataframe import (
    get_report_as_df,
    get_sheet_as_df,
    get_as_df,
)
from smartsheet_dataframe.smartsheet_dataframe import (
    _do_request,
    _get_from_request,
    _handle_object_value,
    to_dataframe,
)
from smartsheet_dataframe.utils.constants import (
    REPORT,
    SHEET,
)
from smartsheet_dataframe.utils.exceptions import (
    AuthenticationError,
)

load_dotenv()


@pytest.mark.skipif(str(os.environ.get("SKIP_LIVE_TESTS", "1")) == "1",
                    reason="Not testing live API calls at this time")
class TestSheet:
    def test_df_has_all_rows(self, smartsheet_access_token: str, sheet_id: int):
        df = get_sheet_as_df(token=smartsheet_access_token, sheet_id=sheet_id)

        assert len(df.index) > 100

    def test_object_and_request_are_equal(self, smartsheet_access_token: str, sheet_id: int, sheet):
        df1 = get_sheet_as_df(token=smartsheet_access_token, sheet_id=sheet_id)
        df2 = to_dataframe(object_dict=sheet.to_dict())

        assert df1.to_dict() == df2.to_dict()

    def test_generic_vs_specific_requests(self, smartsheet_access_token: str, sheet_id: int):
        df1 = get_sheet_as_df(token=smartsheet_access_token, sheet_id=sheet_id)
        df2 = get_as_df(object_type='sheet', token=smartsheet_access_token, object_id=sheet_id)

        assert df1.to_dict() == df2.to_dict()


@pytest.mark.skipif(str(os.environ.get("SKIP_LIVE_TESTS", "1")) == "1",
                    reason="Not testing live API calls at this time")
class TestReport:
    def test_df_has_all_rows(self, smartsheet_access_token: str, report_id: int):
        df = get_report_as_df(token=smartsheet_access_token, report_id=report_id)

        assert len(df.index) > 100

    def test_report_object_and_request_are_equal(self, smartsheet_access_token: str, report_id: int, report):
        df1 = get_report_as_df(token=smartsheet_access_token, report_id=report_id)
        df2 = to_dataframe(object_dict=report.to_dict())

        assert df1.to_dict() == df2.to_dict()

    def test_generic_vs_specific_requests(self, smartsheet_access_token: str, report_id: int):
        df1 = get_report_as_df(token=smartsheet_access_token, report_id=report_id)
        df2 = get_as_df(object_type='report', token=smartsheet_access_token, object_id=report_id)

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


class TestGetAsDf:
    """ """


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

    @patch('smartsheet_dataframe.smartsheet_dataframe.requests.get')
    @pytest.mark.parametrize("error_code", [1002, 1003, 1004])
    def test_do_request_auth_failure(self, mock_get, error_code, caplog):
        mock_response = Mock()
        mock_response.status_code = 401
        mock_response.json.return_value = {"errorCode": error_code}
        mock_response.text = "Test authentication failure message"
        mock_get.return_value = mock_response

        # Uncomment for 1.0 release
        # with pytest.raises(AuthenticationError, match="auth") as e:
        #     _do_request(url="https://fakeurl.com", options={})
        # assert "Could not connect using the supplied auth token" in str(e.value)

        _do_request(url="https://fakeurl.com", options={})

        assert mock_get.call_count == 1
        assert caplog.records[-1].levelname == "ERROR"
        assert "Smartsheet returned an error status code" in caplog.text


class TestToDataFrame:
    def test_to_dataframe_empty_sheet(self):
        mock_object_dict = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": []
        }

        df = to_dataframe(mock_object_dict)

        assert isinstance(df, pd.DataFrame)
        assert df.empty is True
        assert "Column1" in df.columns
        assert "Column2" in df.columns

    def test_to_dataframe_with_data(self):
        mock_object_dict = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "parentId": 0, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = to_dataframe(mock_object_dict)

        assert isinstance(df, pd.DataFrame)
        assert "Column1" in df.columns
        assert "Column2" in df.columns
        assert df.loc[0, "Column1"] == "Value1"
        assert df.loc[0, "Column2"] == "Value2"


class TestGetFromRequest:
    @pytest.mark.skipif(str(os.environ.get("SKIP_LIVE_TESTS", "1")) == "1",
                        reason="Not testing live API calls at this time")
    def test_report_live(self, smartsheet_access_token: str, report_id: int):
        """ Ensure that a report can be retrieved. """

        response_json = _get_from_request(token=smartsheet_access_token,
                                          object_id=report_id,
                                          object_type="REPORT")

        assert response_json is not None
        assert isinstance(response_json, dict)

    def test_unknown_type_raises(self):
        """ Ensure that an unknown "object_type" argument raises an exception. """

        with pytest.raises(ValueError) as e:
            _get_from_request(token="fake_token", object_id=1234, object_type="UNKNOWN")

        assert "parameter must be one of SHEET or REPORT" in str(e.value)

    @pytest.mark.parametrize("object_type", ["SHEET", "REPORT", REPORT, SHEET])
    @patch('smartsheet_dataframe.smartsheet_dataframe._do_request')
    def test_known_types_do_not_raise(self, mock_do_request, object_type):
        MockResponse = Mock(
            status_code=200,
            json=lambda: {"data": "some_data"}
        )

        mock_do_request.return_value = MockResponse

        response_json = _get_from_request(token="fake_token", object_id=1234, object_type=object_type)

        assert response_json is not None
        assert isinstance(response_json, dict)
        assert response_json["data"] == "some_data"
        assert mock_do_request.call_count == 1


class TestHandleObjectValue:
    @pytest.mark.parametrize("object_type,values,expected", [
        (
                "MULTI_CONTACT",
                [{"email": "test1@test.com"}, {"email": "test2@test.com"}],
                "test1@test.com, test2@test.com"
        ),
        (
                "MULTI_CONTACT", [], ""
        ),
        (
                "SOMETHING_NOT_SUPPORTED", None, ""
        )
    ])
    def test_success(self, object_type, values, expected):
        """Ensure that objectValue cell types are handled correctly."""

        object_value = {
            "objectType": object_type,
            "values": values
        }

        result = _handle_object_value(object_value)

        assert result == expected
