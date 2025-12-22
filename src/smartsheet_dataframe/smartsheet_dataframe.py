"""Primary Smartsheet-DataFrame module.

This package contains functions to retrieve Smartsheet
reports and sheets as a Pandas DataFrame
"""

# Standard Imports
import logging
import time
import warnings
from typing import Any

# 3rd-Party Imports
import pandas as pd
import requests

# Local Imports
from .exceptions import AuthenticationError
from .utils.constants import (
    REPORT,
    SHEET,
)

logger = logging.getLogger(__name__)


def get_report_as_df(token: str,
                     report_id: int,
                     include_row_id: bool = True,
                     include_parent_id: bool = True) -> pd.DataFrame:
    """Get a Smartsheet report as a Pandas DataFrame.

    :param token: Smartsheet Personal Access Token
    :type token: str

    :param report_id: Id of report to retrieve
    :type report_id: int

    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :type include_row_id: bool

    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populate with parent ID for each nested row
    :type include_parent_id: bool

    :return: Pandas DataFrame with report data
    :rtype: pd.DataFrame
    """

    return to_dataframe(_get_from_request(token=token, object_id=report_id, object_type=REPORT),
                        include_row_id,
                        include_parent_id)


def get_sheet_as_df(token: str,
                    sheet_id: int,
                    include_row_id: bool = True,
                    include_parent_id: bool = True) -> pd.DataFrame:
    """Get a Smartsheet sheet as a Pandas DataFrame.

    :param token: Smartsheet personal authentication token
    :type token: str

    :param sheet_id: Smartsheet source sheet id to get
    :type sheet_id: int

    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :type include_row_id: bool

    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populate with parent ID for each nested row
    :type include_parent_id: bool

    :return: Pandas DataFrame with sheet data
    :rtype: pd.DataFrame
    """

    return to_dataframe(_get_from_request(token=token, object_id=sheet_id, object_type=SHEET),
                        include_row_id,
                        include_parent_id)


def get_as_df(token: str,
              object_type: Literal["REPORT", "SHEET"],
              object_id: int,
              include_row_id: bool = True,
              include_parent_id: bool = True) -> pd.DataFrame:
    """Get a Smartsheet report or sheet as a Pandas DataFrame.

    :param token: Smartsheet personal authentication token
    :type token: str

    :param object_type: type of object to get. Must be one of 'report' or 'sheet'
    :type object_type: str

    :param object_id: Smartsheet object (report or sheet) ID
    :type object_id: int

    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :type include_row_id: bool

    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populate with parent ID for each nested row
    :type include_parent_id: bool

    :return: Pandas DataFrame with object data
    :rtype: pd.DataFrame
    """

    return to_dataframe(_get_from_request(token=token, object_id=object_id, object_type=object_type),
                        include_row_id,
                        include_parent_id)


def _get_from_request(token: str,
                      object_type: str,
                      object_id: int) -> dict:
    """Get a Smartsheet object from the API via HTTP request.

    :param token: Smartsheet personal authentication token
    :type token: str

    :param object_id: Smartsheet object (report or sheet) ID
    :type object_id: int

    :param object_type: type of object to get. Must be one of 'REPORT' or 'SHEET'
    :type object_type: str

    :return: Smartsheet sheet or report object dictionary
    :rtype: dict
    """

    if str(object_type).upper() not in (SHEET, REPORT):
        raise ValueError(
            f"'object_type' parameter must be one of SHEET or REPORT. The current value is '{object_type.upper()}'")

    if object_type.upper() == SHEET:
        url = f"https://api.smartsheet.com/2.0/sheets/{object_id}?include=objectValue&level=1"
        logger.debug("Getting sheet request", extra={"id": object_id, "url": url, "object_type": SHEET})
    elif object_type.upper() == REPORT:
        url = f"https://api.smartsheet.com/2.0/reports/{object_id}?pageSize=50000"
        logger.debug("Getting report request", extra={"id": object_id, "url": url, "object_Type": REPORT})
    else:
        # Leaving for type checking purposes
        raise ValueError(
            f"'object_type' parameter must be one of SHEET or REPORT. The current value is '{object_type.upper()}'")

    credentials: dict = {"Authorization": f"Bearer {token}"}
    response = _do_request(url, options=credentials)

    return response.json()


def to_dataframe(object_dict: dict,
                 include_row_id: bool = True,
                 include_parent_id: bool = True) -> pd.DataFrame:
    """Convert a Smartsheet object dictionary to a Pandas DataFrame.

    :param object_dict: Smartsheet object dictionary
    :type object_dict: dict

    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :type include_row_id: bool

    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populate with parent ID for each nested row
    :type include_parent_id: bool

    :return: Pandas DataFrame with object data
    :rtype: pd.DataFrame
    """

    columns_list: list[str] = [column["title"] for column in object_dict["columns"]]

    if include_parent_id:
        columns_list.insert(0, "parent_id")
    if include_row_id:
        columns_list.insert(0, "row_id")

    rows_list: list[list[Any]] = []

    # Handle empty sheet condition
    if not object_dict.get("rows", None):
        return pd.DataFrame(columns=columns_list)  # pyright: ignore

    for row in object_dict["rows"]:
        cells_list: list[Any] = []
        if include_row_id:
            cells_list.append(int(row["id"]))
        if include_parent_id:
            cells_list.append(int(row["parentId"])) if "parentId" in row else cells_list.append("")

        for cell in row["cells"]:
            if "value" in cell:
                cells_list.append(cell["value"])
            elif "objectValue" in cell:
                cells_list.append(_handle_object_value(cell["objectValue"]))
            else:
                cells_list.append("")
        else:
            rows_list.append(cells_list)

    return pd.DataFrame(rows_list, columns=columns_list)  # pyright: ignore


def _do_request(url: str, options: dict, retries: int = 3) -> requests.Response:
    """Do the HTTP request, handling rate limit retrying.

    :param url: Smartsheet API URL
    :type url: str

    :param options: API request headers
    :type options: dict

    :param retries: Number of retries
    :type retries: int

    :return: Requests response object
    :rtype: requests.Response
    """

    i = 0
    for i in range(retries):
        try:
            response = requests.get(url, headers=options)
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
                    warnings.warn("An unhandled status_code was returned by the Smartsheet API: \n" +
                                  response.text)
                    return  # TODO: Fix reportReturnType
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


def _handle_object_value(object_value: dict) -> str:
    """Handle Smartsheet objectValue cell types.

    :param object_value: Smartsheet objectValue dictionary
    :type object_value: dict

    :return: String representation of objectValue
    :rtype: str
    """

    email_list_string: str = ""

    if object_value["objectType"].upper() == "MULTI_CONTACT":
        email_list_string = ", ".join(obj["email"] for obj in object_value["values"])

    return email_list_string
