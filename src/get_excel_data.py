import pandas as pd
import os
import json

def get_raw_data():
    # Get the list of all files and directories
    path = "//mnt//c//dev//cl//VKK-retail//docs" # INSERT YOUR PATH
    df = pd.DataFrame()
    for x in os.listdir(path):
        if x.endswith(".xlsx"):
            excel_data_fragment = pd.read_excel(path+"//"+x, sheet_name=0)
            df = pd.concat([df,excel_data_fragment],ignore_index=True)
        # Kan legge til  flere filformater f.eks:if x.endswith(".csv"):
        # pd.read_csv(path+"//"+x, sheet_name=0).head(5)
    records = df.to_dict('records')
    return records


def get_data_dimentions():
    # Get the list of all files and directories in path
    path = "//mnt//c//dev//cl//VKK-retail//docs" # INSERT YOUR PATH
    info = []
    for x in os.listdir(path):
        if x.endswith(".xlsx"):
            df = pd.read_excel(path+"//"+x, sheet_name=0)
            data = {"File name": x, "Columns": str(len(df.columns)), "Rows": str(len(df))}
            info.append(data)
    return info


#raw_data_info = get_data_dimentions()
#for item in raw_data_info:
#    print(item["File name"] + " har " + item["Columns"] + " kolonner og " + item["Rows"] + " rader")

