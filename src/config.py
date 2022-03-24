import os
from dotenv import load_dotenv


class My_Config():
    load_dotenv()
    def az_table_conn_str():
        connection_string = os.getenv("AZURE_TABLE_CONNECTION_STRING")
        return connection_string
    def az_table():
        azure_table_name = os.getenv("AZURE_TABLE_NAME")
        return azure_table_name
    def az_queue():
        azure_queue_name = os.getenv("AZURE_QUEUE_NAME")
        return azure_queue_name
    def mongo_db_conn_str():
        mongo_db_conn_str = os.getenv("MONGO_DB_CONN_STR")
        return mongo_db_conn_str
    def document_path():
        document_path = os.getenv("DOCUMENT_PATH")
        return document_path
