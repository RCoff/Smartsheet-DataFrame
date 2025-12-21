# Standard Imports
import os

# 3rd-party imports
import pytest
import smartsheet
from dotenv import load_dotenv

load_dotenv()


@pytest.fixture
def smartsheet_access_token() -> str:
    return os.environ["smartsheet_access_token"]


@pytest.fixture
def smartsheet_client(smartsheet_access_token: str) -> smartsheet.Smartsheet:
    return smartsheet.Smartsheet(smartsheet_access_token)


@pytest.fixture
def sheet_id() -> int:
    return int(os.environ["testing_sheet_id"])


@pytest.fixture
def report_id() -> int:
    return int(os.environ["testing_report_id"])


@pytest.fixture
def sheet(sheet_id: int, smartsheet_client: smartsheet.Smartsheet):
    return smartsheet_client.Sheets.get_sheet(sheet_id, include=["objectValue"], level=1)


@pytest.fixture
def report(report_id: int, smartsheet_client: smartsheet.Smartsheet):
    return smartsheet_client.Reports.get_report(report_id, include=["objectValue"], level=1, page_size=10000)
