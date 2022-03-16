from config import My_Config
from read_excel_test import get_raw_data

def get_database():
    from pymongo import MongoClient
    import pymongo

    # Provide the mongodb atlas url to connect python to mongodb using pymongo
    CONNECTION_STRING = My_Config.mongo_db_conn_str()

    # Create a connection using MongoClient.
    client = MongoClient(CONNECTION_STRING)

    # Create the database
    return client['vkk-retail-raw']

def create_collection(dbname):
    # Create collection name
    collection_name = dbname["raw_data"]

    # Get data from excel
    data = get_raw_data()

    # Insert into database collection
    collection_name.insert_many(data)
    print("End: data inserted")


if __name__ == "__main__":

    # Get the database
    dbname = get_database()

    #Create collection and insert data
    create_collection(dbname)
