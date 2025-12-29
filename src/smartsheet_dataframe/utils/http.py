# Standard Imports
import logging
import time
import warnings

# 3rd-Party Imports
import httpx

# Local Imports
from .exceptions import AuthenticationError

logger = logging.getLogger(__name__)


def _do_request(url: str, options: dict, retries: int = 3) -> httpx.Response:
    """Do the HTTP request, handling rate limit retrying.

    :param url: Smartsheet API URL
    :type url: str

    :param options: API request headers
    :type options: dict

    :param retries: Number of retries
    :type retries: int

    :return: httpx Response object
    :rtype: httpx.Response
    """

    i = 0
    response: httpx.Response | None = None

    for i in range(retries):
        try:
            # Use httpx to perform a simple GET. Keep timeout modest to avoid hanging.
            response = httpx.get(url, headers=options, timeout=30.0)

            # Attempt to parse JSON (tests use mocked .json())
            response_json = response.json()

            if response.status_code != 200:
                if response_json["errorCode"] in (1002, 1003, 1004):
                    raise AuthenticationError("Could not connect using the supplied auth token \n" +
                                              response.text)
                elif response_json["errorCode"] == 4004:
                    logger.debug(f"Rate limit exceeded. Waiting and trying again... {i}")
                    time.sleep(5 + (i * 5))
                    continue
                else:
                    warnings.warn("An unhandled status_code was returned by the Smartsheet API: \n" + response.text)
                    return response
        except AuthenticationError:
            logger.exception("Smartsheet returned an error status code")
            # TODO: For 1.0 release, ensure that this is re-raised
            break
        except Exception:
            logger.exception(f"Not able to retrieve get response. Retrying... {i}")
            time.sleep(5 + (i * 5))
            continue
        break
    else:
        # TODO: For 1.0 release, re-raise exception
        raise Exception(f"Could not retrieve request after retrying {i} times")

    return response  # TODO: Fix reportPossiblyUnboundVariable

