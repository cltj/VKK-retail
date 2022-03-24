from scipy import rand
from get_excel_data import get_raw_data, analyze_files
from pymongo_db import get_database, create_collection, analyze_db_raw, get_colums_raw, find_distinct, get_rows_from_raw
from az_table import new_table, entity_crud
from checks import equal
import pandas as pd
import random
import string


def random_char(y):
    """
    returns y number of uppercase random characters
    """
    return ''.join(random.choice(string.ascii_uppercase) for x in range(y))


def analyze_docs_files():
    file_analysis = analyze_files()
    file_rows = file_analysis[1]
    max_columns = file_analysis[2]
    return file_rows, max_columns


def create_raw_db():
    # Create the database
    db = get_database(name="vkk-retail-raw")
    # Get data from excel
    excel_data = get_raw_data()
    # Insert into DB
    create_collection(database=db,col_name="raw_data",data=excel_data)
    print("---------------------------------")
    return True


def create_trimmed_db(trimmed_data):
    new_db = get_database(name="vkk-retail-raw")
    create_collection(db=new_db,name="trimmed_data",data=trimmed_data)
    print("---------------------------------")
    return True


def stage_1():
    file_rows, max_columns = analyze_docs_files()
    res = create_raw_db()
    if res == True:
        db_rows = analyze_db_raw()
        equal(file_rows, db_rows)
        return 1
    else:
        return 0


def stage_2():
    db = get_database(name="vkk-retail-raw")
    columns = get_colums_raw(db, name="raw_data")
    sustained_columns = []
    dropped_columns = []
    for col in columns:
        distinct = find_distinct(db,col,collection_name="raw_data")
        if len(distinct) < 3:
            # add to delete list
            dropped_columns.append(col)
            print(distinct)
        else:
            # add to sutained
            sustained_columns.append(col)

    print(len(dropped_columns), len(sustained_columns))
    result = get_rows_from_raw(db_name="vkk-retail-raw", dropped_cols=dropped_columns)
    create_trimmed_db(trimmed_data=result)

    return dropped_columns, sustained_columns


def stage_3():
    db = get_database(name="vkk-retail-raw")
    columns = get_colums_raw(db, name="trimmed_data")
    dimentions = []
    unclear_dimentions = []
    facts = []
    for col in columns:
        distinct = find_distinct(db,col,collection_name="trimmed_data")
        if len(distinct) > 1000:
            facts.append(col)
        elif len(distinct) < 50:
            print(distinct)
            col_name = str(col.replace(" ", ""))
            new_table_name = "DIM0"+ col_name
            response = new_table(table_name=new_table_name)
            print(response.table_name + " was created...")
            entities = pd.DataFrame.to_dict(distinct)
            temp = entities[col].values()
            new_entity = {}
            for item in temp:
                new_id = random_char(6)
                new_entity["PartitionKey"] = col_name
                new_entity["RowKey"] = new_id
                new_entity[col_name+"s"] = item
                entity_crud(table_name=new_table_name, operation="create", entity=new_entity)
            dimentions.append(col)
        else:
            unclear_dimentions.append(col)

    ret = {"Done":True, "dim":dimentions, "unclear":unclear_dimentions, "facts": facts}
    return ret

def main():
    #result = stage_1()
    #if result == 0:
    #    print("Stage 1 failed!")
    #    exit()

    #dropped_columns, sustained_columns = stage_2()
    #print("---|Filtered out " + str(dropped_columns) + " columns \n"
    #"---| Sustained Columns =  " + str(sustained_columns))

    ret = stage_3()
    print(ret['Done'])

if __name__ == "__main__":
    main()