# Standard Imports
import unittest

# 3rd-Party Imports
import smartsheet

# Local Imports
from src.smartsheet_dataframe import (
    get_report_as_df,
    get_sheet_as_df,
    get_as_df
)


@unittest.skip("Not testing API calls at this time")
class TestSheet(unittest.TestCase):
    def setUp(self):
        import config
        self.token = config.smartsheet_access_token
        self.sheet_id = config.sheet_id
        self.report_id = config.report_id
        self.sheet_client = smartsheet.Smartsheet(self.token)
        self.sheet_obj = self.sheet_client.Sheets.get_sheet(self.sheet_id, include=['objectValue'], level=1)

    def test_object_and_request_are_equal(self):
        df1 = get_sheet_as_df(token=self.token, sheet_id=self.sheet_id)
        df2 = get_sheet_as_df(sheet_obj=self.sheet_obj)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    def test_generic_vs_specific_requests(self):
        df1 = get_sheet_as_df(token=self.token, sheet_id=self.sheet_id)
        df2 = get_as_df(type_='sheet', token=self.token, id_=self.sheet_id)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    def test_generic_vs_specific_object(self):
        df1 = get_sheet_as_df(sheet_obj=self.sheet_obj)
        df2 = get_as_df(type_='sheet', obj=self.sheet_obj)

        self.assertTrue(df1.to_dict() == df2.to_dict())


@unittest.skip("Not testing API calls at this time")
class TestReport(unittest.TestCase):
    def setUp(self):
        import config
        self.token = config.smartsheet_access_token
        self.report_id = config.report_id
        self.sheet_client = smartsheet.Smartsheet(self.token)
        self.report_obj = self.sheet_client.Reports.get_report(self.report_id)

    def test_report_object_and_request_are_equal(self):
        df1 = get_report_as_df(token=self.token, report_id=self.report_id)
        df2 = get_report_as_df(report_obj=self.report_obj)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    def test_generic_vs_specific_requests(self):
        df1 = get_report_as_df(token=self.token, report_id=self.report_id)
        df2 = get_as_df(type_='report', token=self.token, id_=self.report_id)

        self.assertTrue(df1.to_dict() == df2.to_dict())

    def test_generic_vs_specific_object(self):
        df1 = get_report_as_df(report_obj=self.report_obj)
        df2 = get_as_df(type_='report', obj=self.report_obj)

        self.assertTrue(df1.to_dict() == df2.to_dict())


if __name__ == "__main__":
    unittest.main()
