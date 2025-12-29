# Standard Imports
from unittest.mock import (
    patch,
    Mock,
)

# 3rd-Party Imports
import pytest

# Local Imports
from smartsheet_dataframe.utils._http import (
    _async_do_request,
    _do_request,
)


class TestDoRequest:
    @patch('smartsheet_dataframe.utils.http.httpx.get')
    def test_do_request_success(self, mock_get):
        mock_response = Mock()
        mock_response.status_code = 200
        mock_response.json.return_value = {"data": "some_data"}
        mock_get.return_value = mock_response

        response = _do_request(url="https://fakeurl.com", options={})

        assert response.json() == {"data": "some_data"}

    @patch("smartsheet_dataframe.utils.http.time")
    @patch('smartsheet_dataframe.utils.http.httpx.get')
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

    @patch("smartsheet_dataframe.utils.http.time")
    @patch('smartsheet_dataframe.utils.http.httpx.get')
    def test_do_request_rate_limit_failure(self, mock_get, mock_time):
        mock_time.sleep.return_value = None

        mock_response_rate_limit = Mock()
        mock_response_rate_limit.status_code = 429
        mock_response_rate_limit.json.return_value = {"errorCode": 4004}

        mock_get.return_value = mock_response_rate_limit

        with pytest.raises(Exception) as e:
            _do_request(url="https://fakeurl.com", options={}, retries=3)

        assert 'Could not retrieve request after retrying' in str(e.value)

    @patch('smartsheet_dataframe.utils.http.httpx.get')
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
