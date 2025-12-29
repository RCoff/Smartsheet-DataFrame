# Standard Imports
from unittest.mock import patch

# 3rd-Party Imports
import pytest

# Local Imports
from smartsheet_dataframe import Client


@pytest.fixture
def client() -> Client:
    return Client(token="test_token", include_row_id=True, include_parent_id=True)


class TestClient:
    @pytest.mark.parametrize("include_row_id", [True, False])
    @pytest.mark.parametrize("include_parent_id", [True, False])
    def test_create_client(self, include_row_id: bool, include_parent_id: bool):
        client = Client(token="fake_token",
                        include_row_id=include_row_id,
                        include_parent_id=include_parent_id)

        assert client.token == "fake_token"
        assert client.include_row_id is include_row_id
        assert client.include_parent_id is include_parent_id

    @patch("smartsheet_dataframe.client.get_sheet_as_df")
    @pytest.mark.parametrize("include_parent_id", [None, True, False])
    @pytest.mark.parametrize("include_row_id", [None, True, False])
    def test_get_sheet_as_df(self, mock_get_sheet_as_df, client: Client, include_row_id, include_parent_id):
        mock_get_sheet_as_df.return_value = "mocked_dataframe"

        expected_include_row_id = include_row_id
        if expected_include_row_id is None:
            expected_include_row_id = client.include_row_id

        expected_include_parent_id = include_parent_id
        if expected_include_parent_id is None:
            expected_include_parent_id = client.include_parent_id

        kwargs = {}
        if include_row_id is not None:
            kwargs["include_row_id"] = include_row_id
        if include_parent_id is not None:
            kwargs["include_parent_id"] = include_parent_id

        client.get_sheet_as_df(sheet_id=1234, **kwargs)

        assert mock_get_sheet_as_df.call_count == 1
        assert mock_get_sheet_as_df.call_args.kwargs["include_row_id"] == expected_include_row_id
        assert mock_get_sheet_as_df.call_args.kwargs["include_parent_id"] == expected_include_parent_id

    @patch("smartsheet_dataframe.client.get_report_as_df")
    @pytest.mark.parametrize("include_parent_id", [None, True, False])
    @pytest.mark.parametrize("include_row_id", [None, True, False])
    def test_get_report_as_df(self, mock_get_report_as_df, client: Client, include_row_id, include_parent_id):
        mock_get_report_as_df.return_value = "mocked_dataframe"

        expected_include_row_id = include_row_id
        if expected_include_row_id is None:
            expected_include_row_id = client.include_row_id

        expected_include_parent_id = include_parent_id
        if expected_include_parent_id is None:
            expected_include_parent_id = client.include_parent_id

        kwargs = {}
        if include_row_id is not None:
            kwargs["include_row_id"] = include_row_id
        if include_parent_id is not None:
            kwargs["include_parent_id"] = include_parent_id

        client.get_report_as_df(report_id=5678, **kwargs)

        assert mock_get_report_as_df.call_count == 1
        assert mock_get_report_as_df.call_args.kwargs["include_row_id"] == expected_include_row_id
        assert mock_get_report_as_df.call_args.kwargs["include_parent_id"] == expected_include_parent_id
