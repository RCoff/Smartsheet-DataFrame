import pandas as pd


def _handle_object_value(object_value: dict) -> str:
    email_list_string: str = ""
    if object_value['objectType'].upper() == "MULTI_CONTACT":
        email_list_string = ', '.join(obj['email'] for obj in object_value['values'])

    return email_list_string


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
