from pymodbus.client import ModbusTcpClient
import pandas as pd
import datetime
from datetime import datetime, timedelta 


def Modbuc_connect(PLC_IP, PLC_PORT):
    # Create a Modbus TCP client
    client = ModbusTcpClient(PLC_IP, port=PLC_PORT)

    # Connect to the PLC
    connection = client.connect()
    return connection, client

def Modbus_read3_register(Port, address, count):
    print("Connected to Siemens PLC")

    # Read holding registers (for example, address 40001 -> register 0 in pymodbus)
    # Adjust the address and count based on your configuration in TIA Portal
    response = client.read_holding_registers(address=address, count=count,slave=1)
    Value = response.registers 
    Timestamp = datetime.now().strftime('%Y-%m-%d %H:%M:%S.%f')[:-3]
    print(Value, Timestamp)

    return Value, Timestamp
