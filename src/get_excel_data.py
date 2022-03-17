import pandas as pd
import os
from config import My_Config
from checks import largest, smallest


def get_raw_data():
    # Get the list of all files and directories
    path = My_Config.document_path() # INSERT YOUR PATH HERE
    df = pd.DataFrame()
    for x in os.listdir(path):
        if x.endswith(".xlsx"):
            excel_data_fragment = pd.read_excel(path+"//"+x, sheet_name=0)
            df = pd.concat([df,excel_data_fragment],ignore_index=True)
        # You can add more file extentions
        # example:
        # if x.endswith(".csv"):
        # pd.read_csv(path+"//"+x, sheet_name=0).head(5)
    records = df.to_dict('records')
    return records


def get_file_dimentions():
    # Get the list of all files and directories in path
    path = My_Config.document_path() # INSERT YOUR PATH HERE
    info = []
    for x in os.listdir(path):
        if x.endswith(".xlsx"):
            df = pd.read_excel(path+"//"+x, sheet_name=0)
            data = {"File name": x, "Columns": str(len(df.columns)), "Rows": str(len(df))}
            info.append(data)
    return info


def analyze_files():
    raw_file_info = get_file_dimentions()
    files = 0
    max_columns = 0
    min_columns = 0
    total_rows = 0
    for item in raw_file_info:
        print(item["File name"] + " | kolonner: " + item["Columns"] + " | rader: " + item["Rows"])
        total_rows = total_rows + int(item["Rows"])
        temp_col_max = largest(max_columns,int(item["Columns"]))
        max_columns = temp_col_max
        temp_col_min = smallest(min_columns,int(item["Columns"]))
        min_columns = temp_col_min
        files += 1
    print(
    "\n | Files searched " +str(files) +
    "\n | Rows found: " + str(total_rows) +
    "\n | Max Columns: " + str(max_columns) +
    "\n | Min Columns: " + str(min_columns)
    )
    return files, total_rows, max_columns, min_columns