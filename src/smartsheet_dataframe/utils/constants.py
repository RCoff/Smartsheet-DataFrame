"""Constants for the smartsheet_dataframe package."""

# Standard Imports
from typing import Final
from urllib.parse import urljoin

SMARTSHEET_API_BASE_URL: Final[str] = "https://api.smartsheet.com/"
SMARTSHEET_API_V2_BASE_URL: Final[str] = urljoin(SMARTSHEET_API_BASE_URL, "2.0/")
SHEETS_ENDPOINT: Final[str] = urljoin(SMARTSHEET_API_V2_BASE_URL, "sheets/")
REPORTS_ENDPOINT: Final[str] = urljoin(SMARTSHEET_API_V2_BASE_URL, "reports/")

REPORT: Final[str] = "REPORT"
SHEET: Final[str] = "SHEET"
