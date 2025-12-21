"""Primary Smartsheet-DataFrame module.

This package contains functions to retrieve Smartsheet
reports and sheets as a Pandas DataFrame
"""

# Standard Imports
import logging
import time
import warnings
from typing import (
    Any,
    Optional,
)

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


def get_report_as_df(token: Optional[str] = None,
                     report_id: Optional[int] = None,
                     include_row_id: bool = True,
                     include_parent_id: bool = True,
                     report_obj: Optional[Any] = None) -> pd.DataFrame:
    """Get a Smartsheet report as a Pandas DataFrame.

    :param token: Smartsheet Personal Access Token
    :type token: str

    :param report_id: ID of report to retrieve
    :type report_id: int

    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :type include_row_id: bool

    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populate with parent ID for each nested row
    :type include_parent_id: bool

    :param report_obj: Smartsheet Python SDK Report object
        Should not be included if token and id_ are provided.
        If both token and id_, and obj are provided, obj will be ignored
    :type report_obj: Any

    :return: Pandas DataFrame with report data
    :rtype: pd.DataFrame
    """

    if token and not report_id:
        try:
            import smartsheet.models  # noqa: PLC0415
            if isinstance(token, smartsheet.models.sheet.Sheet):
                raise ValueError("Function must be called with the 'report_obj=' keyword argument")
        except ModuleNotFoundError:
            pass

        raise ValueError("A report_id must be included in the parameters if a token is provided")

    if report_obj and report_id:
        warnings.warn("A 'report_id' has been provided along with a 'report_obj' \n" +
                      "The 'report_id' parameter will be ignored")

    if token and report_id:
        return _to_dataframe(_get_from_request(token, report_id, type_="REPORT"), include_row_id, include_parent_id)
    elif report_obj:
        return _to_dataframe(report_obj.to_dict(), include_row_id, include_parent_id)
    else:
        raise ValueError("One of 'token' or 'report_obj' must be included in parameters")


def get_sheet_as_df(token: Optional[str] = None,
                    sheet_id: Optional[int] = None,
                    include_row_id: bool = True,
                    include_parent_id: bool = True,
                    sheet_obj: Optional[Any] = None) -> pd.DataFrame:
    """Get a Smartsheet sheet as a Pandas DataFrame.

    :param token: Smartsheet personal authentication token
    :type token: str

    :param sheet_id: Smartsheet source sheet ID to get
    :type sheet_id: int

    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :type include_row_id: bool

    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populat with parent ID for each nested row
    :type include_parent_id: bool

    :param sheet_obj: Smartsheet Python SDK sheet object
        Should not be included if token and id_ are provided.
        If both token and id_, and obj are provided, obj will be ignored
    :type sheet_obj: Any

    :return: Pandas DataFrame with sheet data
    :rtype: pd.DataFrame
    """

    if token and not sheet_id:
        try:
            import smartsheet.models  # noqa: PLC0415
            if isinstance(token, smartsheet.models.sheet.Sheet):
                raise ValueError("Function must be called with the 'sheet_obj=' keyword argument")
        except ModuleNotFoundError:
            pass

        raise ValueError("A sheet_id must be included in the parameters if a token is provided")

    if sheet_obj and sheet_id:
        warnings.warn("A 'sheet_id' has been provided along with a 'sheet_obj' \n" +
                      "The 'sheet_id' parameter will be ignored")

    if token and sheet_id:
        return _to_dataframe(_get_from_request(token, sheet_id, type_="SHEET"), include_row_id, include_parent_id)
    elif sheet_obj:
        return _to_dataframe(sheet_obj.to_dict(), include_row_id, include_parent_id)
    else:
        raise ValueError("One of 'token' or 'sheet_obj' must be included in parameters")


def get_as_df(type_: str,
              token: Optional[str] = None,
              id_: Optional[int] = None,
              obj: Optional[Any] = None,
              include_row_id: bool = True,
              include_parent_id: bool = True) -> pd.DataFrame:
    """Get a Smartsheet report or sheet as a Pandas DataFrame.

    :param type_: type of object to get. Must be one of 'report' or 'sheet'
    :type type_: str

    :param token: Smartsheet personal authentication token
    :type token: str

    :param id_: Smartsheet object (report or sheet) ID
    :type id_: int

    :param obj: Smartsheet Python SDK report or sheet object
        Should not be included if token and id_ are provided.
        If both token and id_, and obj are provided, obj will be ignored
    :type obj: Any

    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :type include_row_id: bool

    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populate with parent ID for each nested row
    :type include_parent_id: bool

    :return: Pandas DataFrame with object data
    :rtype: pd.DataFrame
    """

    if not (token or obj):
        raise ValueError("One of 'token' or 'obj' must be included in parameters")

    if token and not id_:
        try:
            import smartsheet.models  # noqa: PLC0415
            if isinstance(token, smartsheet.models.sheet.Sheet):
                raise ValueError("Function must be called with the 'sheet_obj=' keyword argument")
        except ModuleNotFoundError:
            pass

        raise ValueError("A sheet_id must be included in the parameters if a token is provided")

    if obj and id_:
        warnings.warn("An 'id' has been provided along with a 'obj' \n" +
                      "The 'id' parameter will be ignored")

    if token and id_:
        return _to_dataframe(_get_from_request(token, id_, type_), include_row_id, include_parent_id)
    elif obj:
        return _to_dataframe(obj.to_dict(), include_row_id, include_parent_id)
    else:
        raise ValueError("One of 'token' or 'obj' must be included in parameters")


def _get_from_request(token: str, id_: int, type_: str) -> dict:
    """Get a Smartsheet object from the API via HTTP request.

    :param token: Smartsheet personal authentication token
    :type token: str

    :param id_: Smartsheet object (report or sheet) ID
    :type id_: int

    :param type_: type of object to get. Must be one of 'REPORT' or 'SHEET'
    :type type_: str

    :return: Smartsheet sheet or report object dictionary
    :rtype: dict
    """

    if str(type_).upper() not in (SHEET, REPORT):
        raise ValueError(f"'type_' parameter must be one of SHEET or REPORT. The current value is '{type_.upper()}'")

    if type_.upper() == "SHEET":
        url = f"https://api.smartsheet.com/2.0/sheets/{id_}?include=objectValue&level=1"
        logger.debug("Getting sheet request", extra={"id": id_,
                                                     "url": url,
                                                     "object_type": "sheet"})
    elif type_.upper() == "REPORT":
        url = f"https://api.smartsheet.com/2.0/reports/{id_}?pageSize=50000"
        logger.debug("Getting report request", extra={"id": id_,
                                                      "url": url,
                                                      "object_Type": "report"})
    else:
        # Leaving for type checking purposes
        raise ValueError(f"'type_' parameter must be one of SHEET or REPORT. The current value is '{type_.upper()}'")

    credentials: dict = {"Authorization": f"Bearer {token}"}
    response = _do_request(url, options=credentials)

    return response.json()


def _to_dataframe(object_dict: dict,
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
