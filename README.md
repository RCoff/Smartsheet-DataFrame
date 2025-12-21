smartsheet-dataframe
===

Python library to ease movement of data between a
Smartsheet or report and a Pandas DataFrame.

This package can be used alongside the ``smartsheet-python-sdk``
package or can be used standalone.

TODO:

- Implement functions to write to a sheet
- Implement usage of Pandas kwargs

# Installation

## Requirements

* Python 3+ (Tested using 3.8+)
* Pandas >= 0.24.0
* Requests >= 2.0.0

## From PyPI

    pip install smartsheet-dataframe

# Usage

## Sheet

To get a sheet as a dataframe:

```python
from smartsheet_dataframe import get_as_df, get_sheet_as_df

# Standalone (without smartsheet-python-sdk)
df = get_sheet_as_df(token='smartsheet_auth_token',
                     sheet_id=sheet_id_int)

# Using 'generic' function (without smartsheet-python-sdk)                     
df = get_as_df(type_='sheet',
               token='smartsheet_auth_token',
               id_=sheet_id_int)
```

Alternatively, sheet objects can be used from the ``smartsheet-python-sdk`` package:

```python
from smartsheet_dataframe import get_as_df, get_sheet_as_df
import smartsheet

# Using smartsheet-python-sdk
smartsheet_client = smartsheet.Smartsheet('smartsheet_auth_token')
sheet = smartsheet_client.Sheets.get_sheet(sheet_id_int)

df = get_sheet_as_df(sheet_obj=sheet)

# And using the 'generic' function
df = get_as_df(type_='sheet',
               obj=sheet)
```

## Report

To get a report as a dataframe:

```python
from smartsheet_dataframe import get_as_df, get_report_as_df

# Standalone (without smartsheet-python-sdk)
df = get_report_as_df(token='smartsheet_auth_token',
                      report_id=report_id_int)

# Using 'generic' function (without smartsheet-python-sdk)                     
df = get_as_df(type_='report',
               token='smartsheet_auth_token',
               id_=report_id_int)
```

And using a report object from the ``smartsheet-python-sdk`` package:

```python
from smartsheet_dataframe import get_as_df, get_report_as_df
import smartsheet

# Using smartsheet-python-sdk
smartsheet_client = smartsheet.Smartsheet('smartsheet_auth_token')
report = smartsheet_client.Reports.get_report(report_id_int)

df = get_report_as_df(report_obj=report)

# And using the 'generic' function
df = get_as_df(type_='report',
               obj=report)
```