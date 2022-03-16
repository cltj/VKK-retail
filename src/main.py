import numpy as np
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt
from config import My_Config
from get_excel_data import get_data_dimentions


def analyze_data():
    raw_data_info = get_data_dimentions()
    for item in raw_data_info:
        print(item["File name"] + " har " + item["Columns"] + " kolonner og " + item["Rows"] + " rader")

def main():
    analyze_data()

if __name__ == "__main__":
    main()