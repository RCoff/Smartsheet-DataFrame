# Standard Imports
import unittest

# 3rd-Part Imports
import pandas as pd

# Local Imports
from src.smartsheet_dataframe.utils import _to_dataframe


class ToDataFrameTest(unittest.TestCase):

    def test_to_dataframe_empty_sheet(self):
        mock_object_dict = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": []
        }

        df = _to_dataframe(mock_object_dict)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertTrue(df.empty)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)

    def test_to_dataframe_with_data(self):
        mock_object_dict = {
            "columns": [{"title": "Column1"}, {"title": "Column2"}],
            "rows": [{"id": 1, "parentId": 0, "cells": [{"value": "Value1"}, {"value": "Value2"}]}]
        }

        df = _to_dataframe(mock_object_dict)

        self.assertIsInstance(df, pd.DataFrame)
        self.assertIn("Column1", df.columns)
        self.assertIn("Column2", df.columns)
        self.assertEqual(df.loc[0, "Column1"], "Value1")
        self.assertEqual(df.loc[0, "Column2"], "Value2")
