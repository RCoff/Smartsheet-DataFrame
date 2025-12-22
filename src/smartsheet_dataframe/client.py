# Standard Imports
import logging
from typing import Optional

# 3rd-Party Imports
import pandas as pd

# Local Imports
from .smartsheet_dataframe import (
    get_sheet_as_df,
    get_report_as_df,
)

logger = logging.getLogger(__name__)


class Client:
    __slots__ = ("token", "include_row_id", "include_parent_id")

    def __init__(self, token: str, include_row_id: bool = True, include_parent_id: bool = True):
        self.token = token
        self.include_row_id = include_row_id
        self.include_parent_id = include_parent_id

    def get_sheet_as_df(self,
                        sheet_id: int,
                        include_row_id: Optional[bool] = None,
                        include_parent_id: Optional[bool] = None) -> pd.DataFrame:
        """Get a Smartsheet sheet as a Pandas DataFrame.

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

        if include_row_id is None:
            include_row_id = self.include_row_id
        if include_parent_id is None:
            include_parent_id = self.include_parent_id

        return get_sheet_as_df(token=self.token,
                               sheet_id=sheet_id,
                               include_row_id=include_row_id,
                               include_parent_id=include_parent_id)

    def get_report_as_df(self,
                         report_id: int,
                         include_row_id: Optional[bool] = None,
                         include_parent_id: Optional[bool] = None) -> pd.DataFrame:
        """Get a Smartsheet report as a Pandas DataFrame.

        :param report_id: Smartsheet source report id to get
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

        if include_row_id is None:
            include_row_id = self.include_row_id
        if include_parent_id is None:
            include_parent_id = self.include_parent_id

        return get_report_as_df(token=self.token,
                                report_id=report_id,
                                include_row_id=include_row_id,
                                include_parent_id=include_parent_id)
