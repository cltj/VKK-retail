from azure.data.tables import TableClient, UpdateMode
from azure.core.exceptions import ResourceExistsError, HttpResponseError, ResourceNotFoundError

from config import My_Config


def entity_crud(table_name, operation, entity):
    connection_string = My_Config.az_table_conn_str()
    with TableClient.from_connection_string(connection_string, table_name) as table_client:
        if operation == 'create':
            try:
                response = table_client.create_entity(entity=entity)
                return(response)
            except ResourceExistsError:
                print("Entity already exists")
        elif operation == 'query':
            try:
                queried_entity = table_client.get_entity(partition_key=entity['PartitionKey'],row_key=entity['RowKey'])
                return (queried_entity)
            except HttpResponseError as e:
                print(e.message)
        elif operation == 'update':
            try:
                response = table_client.update_entity(mode=UpdateMode.MERGE, entity=entity)
                return response
            except HttpResponseError as e:
                print(e.message)
        elif operation == 'delete':
            try:
                response = table_client.delete_entity(partition_key=entity['PartitionKey'],row_key=entity['RowKey'])
                return (response)
            except ResourceNotFoundError:
                print("Entity does not exists")


def list_entities(connection_string, table_name, select):
    with TableClient.from_connection_string(connection_string, table_name=table_name) as table_client:
        try:
            entities = list(table_client.list_entities(select=select))
            return entities
        except HttpResponseError as e:
                print(e.message)

def query_entities_values(connection_string, table_name, filter, select): # , parameters
    lst = []
    with TableClient.from_connection_string(connection_string, table_name) as table_client:
        try:
            entities = table_client.query_entities(filter, select=[select])
            for entity in entities:
                lst.append(entity)
            print(len(lst))
            return lst
        except HttpResponseError as e:
            print(e.message)


def new_table(table_name):
    connection_string = My_Config.az_table_conn_str()
    from azure.data.tables import TableServiceClient
    table_service_client = TableServiceClient.from_connection_string(conn_str=connection_string)
    table_client = table_service_client.create_table(table_name=table_name)
    return table_client