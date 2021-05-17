"""
Smartsheet-DataFrame
####################

This package contains functions to retrieve Smartsheet
reports and sheets as a Pandas DataFrame

"""

import pandas as pd
import requests
from typing import Any
import logging
import warnings
import time


def get_report_as_df(token: str = None,
                     report_id: int = None,
                     include_row_id: bool = True,
                     include_parent_id: bool = True,
                     report_obj: Any = None) -> pd.DataFrame:
    pass


def get_sheet_as_df(token: str = None,
                    sheet_id: int = None,
                    include_row_id: bool = True,
                    include_parent_id: bool = True,
                    sheet_obj: Any = None) -> pd.DataFrame:
    """
    Get a Smartsheet sheet as a Pandas DataFrame

    :param token: Smartsheet Personal Authentication Token
    :param sheet_id: Smartsheet source sheet ID to get
    :param include_row_id: Include row IDs in DataFrame
    :param include_parent_id: Include row Parent IDs in DataFrame
    :param sheet_obj: Smartsheet Python SDK sheet object

    :return: Pandas DataFrame with sheet data
    """
    _setup_logging()

    if (not token and not sheet_obj) or (token and sheet_obj):
        raise ValueError("One of 'token' or 'sheet_obj' must be included in parameters")

    if token and not sheet_id:
        raise ValueError("A sheet_id must be included in the parameters")

    if sheet_obj and sheet_id:
        logging.warning("A 'sheet_id' has been provided along with a 'sheet_obj'." +
                        "The 'sheet_id' parameter will be ignored")

    if token and sheet_id:
        return _get_sheet_from_request(token, sheet_id, include_row_id, include_parent_id)
    elif sheet_obj:
        pass
        # TODO:  return _get_sheet_from_sdk_obj(sheet_obj)


def _get_sheet_from_request(token: str, sheet_id: int, include_row_id: bool, include_parent_id: bool) -> pd.DataFrame:
    credentials: dict = {"Authorization": f"Bearer {token}"}
    response = _do_request(f"https://api.smartsheet.com/2.0/sheets/{sheet_id}", options=credentials)
    # requests.get(f"https://api.smartsheet.com/2.0/sheets/{sheet_id}", headers=credentials)
    response_json = response.json()

    columns_list = [column['title'] for column in response_json['columns']]

    if include_row_id:
        columns_list.insert(0, "row_id")
    if include_parent_id:
        columns_list.insert(1, "parent_id")

    rows_list = []
    for row in response_json['rows']:
        cells_list = []
        for cell in row['cells']:
            if row['id'] not in cells_list:
                if include_row_id:
                    cells_list.append(int(row['id']))
                if include_parent_id:
                    if 'parentId' in row:
                        cells_list.append(int(row['parentId']))

            if 'value' in cell:
                cells_list.append(cell['value'])
            else:
                cells_list.append('')
        else:
            rows_list.append(cells_list)

    return pd.DataFrame(rows_list, columns=columns_list)


def _get_sheet_from_sdk_obj(sheet_obj):
    pass


def _do_request(url: str, options: dict, retries: int = 3) -> requests.Response:
    i = 0
    for i in range(retries):
        try:
            response = requests.get(url, headers=options)
            response_json = response.json()

            if response.status_code != 200:
                if response_json['errorCode'] == 1002 or response_json['errorCode'] == 1003 or \
                   response_json['errorCode'] == 1004:
                    raise AuthenticationError("Could not connect using the supplied auth token \n" +
                                              response.text)
                elif response_json['errorCode'] == 4004:
                    logging.debug(f"Rate limit exceeded. Waiting and trying again... {i}")
                    time.sleep(5 + (i * 5))
                    continue
                else:
                    warnings.warn("An unhandled status_code was returned by the Smartsheet API: \n" +
                                  response.text)
                    return
        except AuthenticationError:
            logging.exception("Smartsheet returned an error status code")
            break
        except:
            logging.exception(f"Not able to retrieve get response. Retrying... {i}")
            time.sleep(5 + (i * 5))
            continue
        break
    else:
        raise Exception(f"Could not retrieve request after retrying {i} times")

    return response


def _setup_logging():
    logging.basicConfig(level=logging.DEBUG,
                        format="%(asctime)s %(levelname)s %(lineno)s %(message)s",
                        handlers=[logging.StreamHandler()])


class AuthenticationError(Exception):
    pass
