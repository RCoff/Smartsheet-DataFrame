"""Custom exceptions for the smartsheet_dataframe package."""

# Standard Imports
from typing import Optional


class SmartsheetAPIError(BaseException):
    """Raised when there is an error with the Smartsheet API."""

    def __init__(self, status_code: int, error_code: int, message: str) -> None:
        """

        :param status_code: HTTP status code returned by the API
        :type status_code: int

        :param error_code: Smartsheet-specific error code
        :type error_code: int

        :param message: Error message returned by the API
        :type message: str
        """

        self.status_code = status_code
        self.error_code = error_code
        self.message = message

        super().__init__(f"Smartsheet API Error [{status_code}] ({error_code}) {message}")


class AuthenticationError(SmartsheetAPIError):
    """Raised when the user is not authenticated."""


class AuthorizationError(SmartsheetAPIError):
    """Raised when the user is authenticated, but not authorized to perform an action."""


class InvalidAccessTokenError(AuthenticationError):
    """Raised when the provided access token is invalid."""

    def __init__(self, message: Optional[str] = None) -> None:
        """Initialize the InvalidAccessTokenError class.

        `status_code` and `error_code` are hardcoded to 401 and 1002 respectively
            per https://developers.smartsheet.com/api/smartsheet/error-codes.

        :param message: Optional custom error message.
            Defaults to "Invalid Access Token" if not provided.
        :type message: Optional[str]
        """

        if message is None:
            message = "Invalid Access Token"

        super().__init__(status_code=401, error_code=1002, message=message)


class RateLimitExceededError(SmartsheetAPIError):
    """Raised when the Smartsheet API rate limit has been exceeded."""

    def __init__(self, status_code: int, error_code: int, message: str, retries: int) -> None:
        """Initialize the RateLimitExceededError class.

        :param status_code: HTTP status code returned by the API
        :type status_code: int

        :param error_code: Smartsheet-specific error code
        :type error_code: int

        :param message: Error message returned by the API
        :type message: str

        :param retries: Number of retries attempted
        :type retries: int
        """

        self.retries = retries
        self.message = f"{message} after {retries} retries."

        super().__init__(status_code=status_code, error_code=error_code, message=self.message)
