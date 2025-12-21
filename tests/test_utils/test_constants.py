# Local Imports
from smartsheet_dataframe.utils.constants import (
    REPORT,
    SHEET,
    REPORTS_ENDPOINT,
    SHEETS_ENDPOINT,
    SMARTSHEET_API_BASE_URL,
    SMARTSHEET_API_V2_BASE_URL,
)


def test_report_constant():
    """Test the REPORT constant."""
    assert REPORT == "REPORT"


def test_sheet_constant():
    """Test the SHEET constant."""
    assert SHEET == "SHEET"


def test_smartsheet_api_base_url_constant():
    """Test the SMARTSHEET_API_BASE_URL constant."""
    assert SMARTSHEET_API_BASE_URL == "https://api.smartsheet.com/"


def test_smartsheet_api_v2_base_url_constant():
    """Test the SMARTSHEET_API_BASE_URL constant."""
    assert SMARTSHEET_API_V2_BASE_URL == "https://api.smartsheet.com/2.0/"


def test_sheets_endpoint_constant():
    """Test the SHEETS_ENDPOINT constant."""
    assert SHEETS_ENDPOINT == "https://api.smartsheet.com/2.0/sheets/"


def test_reports_endpoint_constant():
    """Test the REPORTS_ENDPOINT constant."""
    assert REPORTS_ENDPOINT == "https://api.smartsheet.com/2.0/reports/"
