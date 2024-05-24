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


logger = logging.getLogger(__name__)


def get_report_as_df(token: str = None,
                     report_id: int = None,
                     include_row_id: bool = True,
                     include_parent_id: bool = True,
                     report_obj: Any = None) -> pd.DataFrame:
    """
    Get a Smartsheet report as a Pandas DataFrame

    :param token: Smartsheet Personal Access Token
    :param report_id: ID of report to retrieve
    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populat with parent ID for each nested row
    :param report_obj: Smartsheet Python SDK Report object

    :return: Pandas DataFrame with report data
    """

    if not (token or report_obj):
        raise ValueError("One of 'token' or 'report_obj' must be included in parameters")

    if token and not report_id:
        try:
            import smartsheet.models
            if isinstance(token, smartsheet.models.sheet.Sheet):
                raise ValueError("Function must be called with the 'report_obj=' keyword argument")
        except ModuleNotFoundError:
            raise ValueError("A report_id must be included in the parameters if a token is provided")

    if report_obj and report_id:
        logger.warning("A 'report_id' has been provided along with a 'report_obj' \n" +
                       "The 'sheet_id' parameter will be ignored")

    if token and report_id:
        return _to_dataframe(_get_from_request(token, report_id, type_="REPORT"), include_row_id, include_parent_id)
    elif report_obj:
        return _to_dataframe(report_obj.to_dict(), include_row_id, include_parent_id)


def get_sheet_as_df(token: str = None,
                    sheet_id: int = None,
                    include_row_id: bool = True,
                    include_parent_id: bool = True,
                    sheet_obj: Any = None) -> pd.DataFrame:
    """
    Get a Smartsheet sheet as a Pandas DataFrame

    :param token: Smartsheet personal authentication token
    :param sheet_id: Smartsheet source sheet ID to get
    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populat with parent ID for each nested row
    :param sheet_obj: Smartsheet Python SDK sheet object

    :return: Pandas DataFrame with sheet data
    """

    if not (token or sheet_obj):
        raise ValueError("One of 'token' or 'sheet_obj' must be included in parameters")

    if token and not sheet_id:
        try:
            import smartsheet.models
            if isinstance(token, smartsheet.models.sheet.Sheet):
                raise ValueError("Function must be called with the 'sheet_obj=' keyword argument")
        except ModuleNotFoundError:
            raise ValueError("A sheet_id must be included in the parameters if a token is provided")

    if sheet_obj and sheet_id:
        logger.warning("A 'sheet_id' has been provided along with a 'sheet_obj' \n" +
                       "The 'sheet_id' parameter will be ignored")

    if token and sheet_id:
        return _to_dataframe(_get_from_request(token, sheet_id, type_="SHEET"), include_row_id, include_parent_id)
    elif sheet_obj:
        return _to_dataframe(sheet_obj.to_dict(), include_row_id, include_parent_id)


def get_as_df(type_: str,
              token: str = None,
              id_: int = None,
              obj: Any = None,
              include_row_id: bool = True,
              include_parent_id: bool = True) -> pd.DataFrame:
    """
    Get a Smartsheet report or sheet as a Pandas DataFrame

    :param type_: type of object to get. Must be one of 'report' or 'sheet'
    :param token: Smartsheet personal authentication token
    :param id_: Smartsheet object ID
    :param obj: Smartsheet SDK object
    :param include_row_id: If True, will append a 'row_id' column to the dataframe
            and populate with row id for each row in sheet
    :param include_parent_id: If True, will append a 'parent_id' column to the
            dataframe and populat with parent ID for each nested row

    :return: Pandas DataFrame with object data
    """
    if not (token or obj):
        raise ValueError("One of 'token' or 'obj' must be included in parameters")

    if token and not id_:
        try:
            import smartsheet.models
            if isinstance(token, smartsheet.models.sheet.Sheet):
                raise ValueError("Function must be called with the 'sheet_obj=' keyword argument")
        except ModuleNotFoundError:
            raise ValueError("A sheet_id must be included in the parameters if a token is provided")

    if obj and id_:
        logger.warning("An 'id' has been provided along with a 'obj' \n" +
                       "The 'id' parameter will be ignored")

    if token and id_:
        return _to_dataframe(_get_from_request(token, id_, type_), include_row_id, include_parent_id)
    elif obj:
        return _to_dataframe(obj.to_dict(), include_row_id, include_parent_id)


def _get_from_request(token: str, id_: int, type_: str) -> dict:
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
        url = None
        raise ValueError(f"'type_' parameter must be one of SHEET or REPORT. The current value is {type_.upper()}")

    credentials: dict = {"Authorization": f"Bearer {token}"}
    response = _do_request(url, options=credentials)

    return response.json()


def _to_dataframe(object_dict: dict, include_row_id: bool = True, include_parent_id: bool = True) -> pd.DataFrame:
    columns_list = [column['title'] for column in object_dict['columns']]

    if include_parent_id:
        columns_list.insert(0, "parent_id")
    if include_row_id:
        columns_list.insert(0, "row_id")

    rows_list = []
    
	# Handle empty sheet condition
    if not object_dict.get('rows', None):
        return pd.DataFrame(columns=columns_list)
    
    for row in object_dict['rows']:
        cells_list = []
        if include_row_id:
            cells_list.append(int(row['id']))
        if include_parent_id:
            cells_list.append(int(row['parentId'])) if 'parentId' in row else cells_list.append('')

        for cell in row['cells']:
            if 'value' in cell:
                cells_list.append(cell['value'])
            elif 'objectValue' in cell:
                cells_list.append(_handle_object_value(cell['objectValue']))
            else:
                cells_list.append('')
        else:
            rows_list.append(cells_list)

    return pd.DataFrame(rows_list, columns=columns_list)


def _do_request(url: str, method: str = 'GET', options: dict = None,
                data: dict = None, json=None, token: str = None, retries: int = 3) -> requests.Response:
    if not (options or token):
        raise ValueError("Neither 'options' nor 'token' were provided. One of these is required")
    elif options and not token:
        if not options.get("Authorization"):
            raise ValueError("'Authorization' missing from options.")
    elif options and token:
        if not options.get("Authorization"):
            options.update({"Authorization": f"Bearer {token}"})
    elif token:
        options = {"Authorization": f"Bearer {token}"}

    i = 0
    response = None
    for i in range(retries):
        try:
            response = requests.request(method=method.upper(), url=url, headers=options, json=json, data=data)
            response_json = response.json()

            if response.status_code != 200:
                if response_json['errorCode'] == 1002 or response_json['errorCode'] == 1003 or \
                        response_json['errorCode'] == 1004:
                    raise AuthenticationError("Could not connect using the supplied auth token \n" +
                                              response.text)
                elif response_json['errorCode'] == 4004:
                    logger.debug(f"Rate limit exceeded. Waiting and trying again... {i}")
                    time.sleep(5 + (i * 5))
                    continue
                else:
                    warnings.warn("An unhandled status_code was returned by the Smartsheet API: \n" +
                                  response.text)
        except AuthenticationError:
            logger.exception("Smartsheet returned an error status code")
            break
        except:
            logger.exception(f"Not able to retrieve get response. Retrying... {i}")
            time.sleep(5 + (i * 5))
            continue
        break
    else:
        raise Exception(f"Could not retrieve request after retrying {i} times")

    return response


def set_as_df(df: pd.DataFrame,
              sheet_id: int,
              token: str = None,
              smartsheet_client: Any = None,
              erase_sheet: bool = False,
              insert_columns: bool = False,
              column_mapping: dict = None) -> None:
    """
    Set values in a Smartsheet from a Pandas DataFrame
    ..................................................

    One of 'token' or 'smartsheet_client' parameters must be supplied

    :param df: Pandas DataFrame to copy into sheet
    :param sheet_id: ID of destination sheet
    :param token: Smartsheet Authentication Token
    :param smartsheet_client: smartsheet-python-sdk authentication object
    :param erase_sheet: If True, will erase all values in a sheet before inserting DataFrame
    :param insert_columns: If True, columns not found in the sheet will be inserted
    :param column_mapping: Mapping for Pandas Columns to Dict containing key-value source:destination pairs.
            This should look like: {'pandas df column name': 'smartsheet_column_name'}

    :return: None
    """

    sheet = None
    if token:
        if smartsheet_client:
            logging.debug("Both 'token' and 'smartsheet_client' were given in function parameters. \n" +
                          "'smartsheet_client' will be ignored")

        sheet = _get_from_request(token=token, id_=sheet_id, type_='sheet')
    elif smartsheet_client:
        sheet = smartsheet_client.Sheets.get_sheet(sheet_id).to_dict()

    columns_dict = _get_columns_dict(sheet)  # {col['title']: col['id'] for col in sheet['columns']}
    row_ids_list = [str(row['id']) for row in sheet['rows']]

    if erase_sheet:
        if row_ids_list:
            logging.debug("Deleting rows from sheet")
            if smartsheet_client:
                smartsheet_client.Sheets.delete_rows(sheet_id, row_ids_list)
            elif token:
                _do_request(f"https://api.smartsheet.com/2.0/sheets/{sheet_id}/rows?ids={','.join(row_ids_list)}&ignoreRowsNotFound=true",
                            token=token, method='DELETE')

    add_columns_list = []
    if insert_columns:
        for col in df.columns.tolist():
            if col != 'row_id' and col != 'parent_id':
                if not columns_dict.get(col):
                    # TODO: Create function to guess column type
                    add_columns_list.append({'title': col, 'type': "TEXT_NUMBER", "index": len(columns_dict)})
        else:
            if add_columns_list:
                if smartsheet_client:
                    import smartsheet.models
                    add_smartsheet_cols = [smartsheet.models.Column(col) for col in add_columns_list]
                    smartsheet_client.Sheets.add_columns(sheet_id, add_smartsheet_cols)
                elif token:
                    _do_request(f"https://api.smartsheet.com/2.0/sheets/{sheet_id}/columns",
                                token=token, method='POST', json=add_columns_list,
                                options={"Content-Type": "application/json"})

    for index, row in df.iterrows():
        pass

    print("pause")


def _handle_object_value(object_value: dict) -> str:
    email_list_string: str = ""
    if object_value['objectType'].upper() == "MULTI_CONTACT":
        email_list_string = ', '.join(obj['email'] for obj in object_value['values'])

    return email_list_string


def _get_columns_dict(sheet: dict) -> dict:
    return {col['title']: {'id': col['id'],
                           'type': col['type'],
                           'index': col['index']} for col in sheet['columns']}


class AuthenticationError(Exception):
    pass
