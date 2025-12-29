# Local Imports
from typing import Optional

# 3rd-Party Imports
import pytest

# Local Imports
from smartsheet_dataframe.utils.exceptions import (
    AuthenticationError,
    AuthorizationError,
    InvalidAccessTokenError,
    RateLimitExceededError,
    SmartsheetAPIError,
)


class TestSmartsheetAPIError:
    def test_exception_message(self):
        exc = SmartsheetAPIError(status_code=400, error_code=1001, message="Bad Request")

        assert str(exc) == "Smartsheet API Error [400] (1001) Bad Request"
        assert exc.status_code == 400
        assert exc.error_code == 1001
        assert exc.message == "Bad Request"


class TestAuthenticationError:
    def test_inherits_from_smartsheet_api_error(self):
        exc = AuthenticationError(status_code=401, error_code=2001, message="Unauthorized")

        assert isinstance(exc, SmartsheetAPIError)
        assert issubclass(AuthenticationError, SmartsheetAPIError)
        assert str(exc) == "Smartsheet API Error [401] (2001) Unauthorized"
        assert exc.status_code == 401
        assert exc.error_code == 2001
        assert exc.message == "Unauthorized"


class TestAuthorizationError:
    def test_inherits_from_smartsheet_api_error(self):
        exc = AuthorizationError(status_code=403, error_code=3001, message="Forbidden")

        assert isinstance(exc, SmartsheetAPIError)
        assert issubclass(AuthorizationError, SmartsheetAPIError)
        assert str(exc) == "Smartsheet API Error [403] (3001) Forbidden"
        assert exc.status_code == 403
        assert exc.error_code == 3001
        assert exc.message == "Forbidden"


class TestInvalidAccessTokenError:
    @pytest.mark.parametrize("custom_message", [None, "Custom invalid token message"])
    def test_inherits_from_authentication_error(self, custom_message: Optional[str]):
        exc = InvalidAccessTokenError(message=custom_message)

        assert isinstance(exc, AuthenticationError)
        assert isinstance(exc, SmartsheetAPIError)
        assert issubclass(InvalidAccessTokenError, AuthenticationError)
        assert issubclass(InvalidAccessTokenError, SmartsheetAPIError)
        assert exc.status_code == 401
        assert exc.error_code == 1002

        if custom_message is not None:
            assert exc.message == custom_message
            assert str(exc) == f"Smartsheet API Error [401] (1002) {custom_message}"
        else:
            assert exc.message == "Invalid Access Token"
            assert str(exc) == "Smartsheet API Error [401] (1002) Invalid Access Token"


class TestRateLimitExceededError:
    def test_inherits_from_smartsheet_api_error(self):
        exc = RateLimitExceededError(status_code=429, error_code=4004, message="Rate limit exceeded", retries=3)

        assert isinstance(exc, SmartsheetAPIError)
        assert issubclass(RateLimitExceededError, SmartsheetAPIError)
        assert str(exc) == "Smartsheet API Error [429] (4004) Rate limit exceeded after 3 retries."
        assert exc.status_code == 429
        assert exc.error_code == 4004
        assert exc.message == "Rate limit exceeded after 3 retries."
