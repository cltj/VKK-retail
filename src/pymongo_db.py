from config import My_Config
from get_excel_data import get_raw_data
import pandas as pd

def get_database(name):
    from pymongo import MongoClient

    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = My_Config.mongo_db_conn_str()

    # Create a connection using MongoClient.
    client = MongoClient(CONNECTION_STRING)

    # Create the database
    return client[name]


def create_collection(db, name, data):
    # Create collection name
    collection_name = db[name]

    # Insert into database collection
    collection_name.insert_many(data)
    print("Data inserted...")


def analyze_db_raw():
    # Get the database
    dbname = get_database()

    # Create a new collection ????
    collection_name = dbname["raw_data"]

    counted_documents = collection_name.estimated_document_count()
    print("Documents found in DB (raw): " + str(counted_documents))
    return counted_documents


def find_distinct(db,column,collection_name):
    # Get the database
    dbname = get_database(db.name)

    # Create a new collection
    collection_name = dbname[collection_name]
    find = collection_name.find({},{ "_id": 0, column: 1})
    df = pd.DataFrame(find)
    print(len(df))
    distinct = df.drop_duplicates(keep='last')
    print("| Documents searched: " + str(len(df)) + "\n| Distinct rows found:" + str(len(distinct)) + "\n| Column searched: " + column)
    return distinct


def get_colums_raw(db, name):
    collection_name = db[name]
    find = collection_name.find_one()
    columns = []
    for column in find:
        columns.append(column)
    return columns


def get_rows_from_raw(db_name, dropped_cols):

    # Get the database
    dbname = get_database(db_name)

    # Get data from raw collection
    collection_name = dbname["raw_data"]
    find = collection_name.find()
    df = pd.DataFrame(find)
    print(len(df.columns))
    for elem in dropped_cols:
        df.drop([elem], axis = 1, inplace=True)
    print(len(df.columns))
    records = df.to_dict('records')
    return records

#hhh = ['Organization Name', 'Company Name','Tenant Reference', 'Agreement Name']
#r = get_rows_from_raw(db_name="vkk-retail-raw", dropped_columns=hhh)
#print(r)