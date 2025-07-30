from opcua import Client, ua

import datetime
from datetime import datetime, timedelta 
# import sys

def OpcConnect(con_string):
    try:
        client = Client(con_string)
        client.session_timeout = 60000
        client.connect()
        print("Connected to Opc Server Successfully")
        return client
    except Exception as e:
        print(f"Error connecting to OPC Server: {e}")

def OPC_CON_TEST(client, node_identifier):
    try:
        
        # Get the node object using the identifier
        Datavalue = client.get_node(node_identifier).get_value()
        # print(Datavalue)
        if type(Datavalue)==float:
#             value = '{0:.3f}'.format(Datavalue)
            value = round(Datavalue,3)
        else:
            value = Datavalue

        return value
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")


def plcDataOpc(client, node_ids):
    try:
        # Create node objects
        nodes = [client.get_node(node_id) for node_id in node_ids]
        # Perform the batch read
        values = client.get_values(nodes)

        timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
        return values, timestamp
    
    except Exception as e:
        print(f"An unexpected error occurred: {e}")



