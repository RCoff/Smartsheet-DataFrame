"""
Smartsheet-DataFrame Async
...

This file contains functions to retrieve Smartsheet
reports and sheets asynchronously and return as a Pandas DataFrame

"""

import logging
import warnings
from typing import Any

import aiohttp
from asyncio import sleep as async_sleep
import pandas as pd

from .exceptions import AuthenticationError
from .utils import _to_dataframe

logger = logging.getLogger(__name__)


async def get_report_as_df(token: str = None,
                           report_id: int = None,
                           include_row_id: bool = True,
                           include_parent_id: bool = True,
                           report_obj: Any = None) -> pd.DataFrame:
    """
    Get a Smartsheet report as a Pandas DataFrame

    :param token: Smartsheet Personal Access Token
    :type token: str

    :param report_id: ID of report to retrieve
    :type report_id: int

    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :type include_row_id: bool

    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populat with parent ID for each nested row
    :type include_parent_id: bool

    :param report_obj: Smartsheet Python SDK Report object
        Should not be included if token and id_ are provided.
        If both token and id_, and obj are provided, obj will be ignored
    :type report_obj: Any

    :return: Pandas DataFrame with report data
    :rtype: pd.DataFrame
    """

    if not (token or report_obj):
        raise ValueError("One of 'token' or 'report_obj' must be included in parameters")

    if token and not report_id:
        try:
            import smartsheet.models
            if isinstance(token, smartsheet.models.sheet.Sheet):
                raise ValueError("Function must be called with the 'report_obj=' keyword argument")
        except ModuleNotFoundError:
            pass

        raise ValueError("A report_id must be included in the parameters if a token is provided")

    if report_obj and report_id:
        warnings.warn("A 'report_id' has been provided along with a 'report_obj' \n" +
                      "The 'sheet_id' parameter will be ignored")

    if token and report_id:
        object_dict = await _async_get_from_request(token=token, id_=report_id, type_="REPORT")
        return _to_dataframe(object_dict=object_dict,
                             include_row_id=include_row_id,
                             include_parent_id=include_parent_id)
    elif report_obj:
        return _to_dataframe(object_dict=report_obj.to_dict(),
                             include_row_id=include_row_id,
                             include_parent_id=include_parent_id)


async def get_sheet_as_df(token: str = None,
                          sheet_id: int = None,
                          include_row_id: bool = True,
                          include_parent_id: bool = True,
                          sheet_obj: Any = None) -> pd.DataFrame:
    """
    Get a Smartsheet sheet as a Pandas DataFrame

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

    if not (token or sheet_obj):
        raise ValueError("One of 'token' or 'sheet_obj' must be included in parameters")

    if token and not sheet_id:
        try:
            import smartsheet.models
            if isinstance(token, smartsheet.models.sheet.Sheet):
                raise ValueError("Function must be called with the 'sheet_obj=' keyword argument")
        except ModuleNotFoundError:
            pass

        raise ValueError("A sheet_id must be included in the parameters if a token is provided")

    if sheet_obj and sheet_id:
        warnings.warn("A 'sheet_id' has been provided along with a 'sheet_obj' \n" +
                      "The 'sheet_id' parameter will be ignored")

    if token and sheet_id:
        object_dict = await _async_get_from_request(token, sheet_id, type_="SHEET")
        return _to_dataframe(object_dict=object_dict,
                             include_row_id=include_row_id,
                             include_parent_id=include_parent_id)
    elif sheet_obj:
        return _to_dataframe(object_dict=sheet_obj.to_dict(),
                             include_row_id=include_row_id,
                             include_parent_id=include_parent_id)


async def get_as_df(type_: str,
                    token: str = None,
                    id_: int = None,
                    obj: Any = None,
                    include_row_id: bool = True,
                    include_parent_id: bool = True) -> pd.DataFrame:
    """
    Get a Smartsheet report or sheet as a Pandas DataFrame

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
            import smartsheet.models
            if isinstance(token, smartsheet.models.sheet.Sheet):
                raise ValueError("Function must be called with the 'sheet_obj=' keyword argument")
        except ModuleNotFoundError:
            pass

        raise ValueError("A sheet_id must be included in the parameters if a token is provided")

    if obj and id_:
        warnings.warn("An 'id' has been provided along with a 'obj' \n" +
                      "The 'id' parameter will be ignored")

    if token and id_:
        object_dict = await _async_get_from_request(token, id_, type_)
        return _to_dataframe(object_dict=object_dict,
                             include_row_id=include_row_id,
                             include_parent_id=include_parent_id)
    elif obj:
        return _to_dataframe(object_dict=obj.to_dict(),
                             include_row_id=include_row_id,
                             include_parent_id=include_parent_id)


async def _async_do_request(url: str, options: dict, retries: int = 3) -> aiohttp.ClientResponse:
    """
    Do the HTTP request asynchronously, handling rate limit retrying

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
    async with aiohttp.ClientSession() as session:
        async with session.get(url, headers=options) as response:
            for i in range(retries):
                try:
                    response_json = await response.json()
                    if response.status != 200:
                        if response_json['errorCode'] == 1002 or response_json['errorCode'] == 1003 or \
                                response_json['errorCode'] == 1004:
                            raise AuthenticationError("Could not connect using the supplied auth token \n" +
                                                      await response.text())
                        elif response_json['errorCode'] == 4004:
                            logger.debug(f"Rate limit exceeded. Waiting and trying again... {i}")
                            await async_sleep(5 + (i * 5))
                            continue
                        else:
                            warnings.warn("An unhandled status_code was returned by the Smartsheet API: \n" +
                                          await response.text())
                            return
                except AuthenticationError:
                    logger.exception("Smartsheet returned an error status code")
                    break
                except Exception:
                    logger.exception(f"Not able to retrieve get response. Retrying... {i}")
                    await async_sleep(5 + (i * 5))
                    continue
                break
            else:
                raise Exception(f"Could not retrieve request after retrying {i} times")

    return response


async def _async_get_from_request(token: str, id_: int, type_: str) -> dict:
    if type_.upper() == "SHEET":
        url = f"https://api.smartsheet.com/2.0/sheets/{id_}?include=objectValue&level=1"
        logger.debug("Getting sheet request", extra={'id': id_,
                                                     'url': url,
                                                     'object_type': 'sheet'})
    elif type_.upper() == "REPORT":
        url = f"https://api.smartsheet.com/2.0/reports/{id_}?pageSize=50000"
        logger.debug("Getting report request", extra={'id': id_,
                                                      'url': url,
                                                      'object_Type': 'report'})
    else:
        raise ValueError(f"'type_' parameter must be one of SHEET or REPORT. The current value is {type_.upper()}")

    credentials: dict = {"Authorization": f"Bearer {token}"}
    response = await _async_do_request(url, options=credentials)

    return await response.json()
