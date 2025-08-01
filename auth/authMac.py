import getmac as gma
from sqlalchemy import text

import datetime
from datetime import datetime, date, timedelta

from config import postgreGetCon
import pandas as pd

# from skewFrontendUI import Ui_MainWindow

from cryptography.fernet import Fernet
from getmac import get_mac_address as gma
import base64
# import zlib
import datetime
import pandas as pd
from datetime import datetime



def mac_insert(styEnckey):
    try:
        # Open connections        
        conn, cursorRead, cursorWrite = postgreGetCon.get_db_connection()
        engine, engineConRead, engineConWrite = postgreGetCon.get_db_connection_engine()

        # Read data from Info_DB
        query = text('SELECT * FROM "Info_DB"')
        dfInfo = pd.read_sql_query(query, engineConRead)
        # Update Software_sold_date if empty
        if dfInfo.loc[dfInfo['Particulars'] == 'Software_sold_date', 'Info'].isnull().values[0]:

            date_now = date.today().isoformat()

            enc_date = encrypt_date(date_now)
            query = """UPDATE "Info_DB" SET "Info" = %s WHERE "Particulars" = %s"""
            cursorWrite.execute(query, (enc_date, "Software_sold_date"))
            conn.commit()

        # Encrypt and update Activation_Key or generate new Activation_Key
        mac = gma(interface="Ethernet")

        if styEnckey:

            if styEnckey == "gAAAAABmzFfoZWkCZCOAZAIMhI3u34OIaOJV990lC0zp2SHVZdebf2Oe_CuFjS4ngG3Pe33bs5pe6KfMNOI9M8MTeUvZc0BjCw==":
                enc_msg, s_version = encrypt(mac, styEnckey)
                
                dec = decrypt(enc_msg)
                dec_key = s_version + dec
                
                enc_key, _ = encrypt(dec_key, styEnckey)
                enc_key_str = base64.b64encode(enc_key).decode('utf-8')
                query = """UPDATE "Info_DB" SET "Info" = %s WHERE "Particulars" = %s"""
                cursorWrite.execute(query, (enc_key_str, "Activation_Key"))
                conn.commit()  # Commit the transaction after update 
                # msg = 'Licence Activation successfully updated'
                # Ui_MainWindow.popUp(msg)
            else:
                
                key = base64.b64decode(styEnckey).decode('utf-8')
                deckey = decrypt(key)
                
                mac_inserted = deckey[-17:]
                stype = int(deckey[:-17])
                if mac_inserted == mac:
                    query = """UPDATE "Info_DB" SET "Info" = %s WHERE "Particulars" = %s"""
                    cursorWrite.execute(query, (styEnckey, "Activation_Key"))
                    conn.commit()
                    return "Licence Activation successfully updated"

        elif styEnckey =="":
            
            active_key = dfInfo.loc[4, 'Info']
            key = base64.b64decode(active_key).decode('utf-8')
            deckey = decrypt(key)
            stype = int(deckey[:-17])

            styEnckey, _ = encrypt(str(stype), key)
            enc_msg, s_version = encrypt(mac, styEnckey)
            
            dec = decrypt(enc_msg)
            dec_key = s_version + dec

            enc_key, _ = encrypt(dec_key, styEnckey)
            enc_key_str = base64.b64encode(enc_key).decode('utf-8')
            query = """UPDATE "Info_DB" SET "Info" = %s WHERE "Particulars" = %s"""
            cursorWrite.execute(query, (enc_key_str, "Activation_Key"))
            conn.commit()  # Commit the transaction after update  
            
        return "Licence Activation successfully updated"
        
    except Exception as e:
        # msg = f"Error updating license: {e}"
        # Ui_MainWindow.popUp(msg)
        print(f"Error updating license: {e}")
        return f"Error updating license: {e}"


def encrypt(enc_msg, styEnckey):
    # Fixed encryption key (ensure it's kept secure)
    crypto_key = b'9tvb2SoOaB11TA4YN3CydnGq4IfvSVSZJy25B6bdskM='
    # Initialize Fernet with the encryption key
    fernet = Fernet(crypto_key)
    # Encrypt the message
    enc_mac = fernet.encrypt(enc_msg.encode())
    # Decrypt a sample version (this seems to be an example encrypted string)
    s_version = decrypt(styEnckey)
    return enc_mac, s_version

def encrypt_date(enc_msg):

    # Fixed encryption key (ensure it's kept secure)
    crypto_key = b'9tvb2SoOaB11TA4YN3CydnGq4IfvSVSZJy25B6bdskM='
    # Initialize Fernet with the encryption key
    fernet = Fernet(crypto_key)
    # Encrypt the message
    enc_mac = fernet.encrypt(enc_msg.encode())
    enc_mac = base64.b64encode(enc_mac).decode('utf-8')
    # Decrypt a sample version (this seems to be an example encrypted string)
    return enc_mac

def decrypt(dec_msg):
    crypto_key = b'9tvb2SoOaB11TA4YN3CydnGq4IfvSVSZJy25B6bdskM='
    # Initialize Fernet with the encryption key
    fernet = Fernet(crypto_key)
    # Decrypt the encrypted message
    dec_mac = fernet.decrypt(dec_msg).decode()
    return dec_mac

def authentication_main(df):
    mac, softwaretype, Software_sold_date = licence_dec(df)
    day_bal, auth_status = validate(mac, softwaretype, Software_sold_date)
    return day_bal, auth_status, softwaretype   

def licence_dec(df):
    try:
        Activation_Key = df.loc[4, 'Info']
        Activation_Key = base64.b64decode(Activation_Key).decode('utf-8')
        dec_key = decrypt(Activation_Key)
        
        
        mac = dec_key[-17:]
        softwaretype = int(dec_key[:-17])
       


        # Initialize 'Software_sold_date' to an empty string
        Software_sold_date = ''

        # Check if 'Info' for 'Software_sold_date' exists and decrypt if available
        if df.loc[3, 'Info']:
            software_date = df.loc[3, 'Info']
            dec_sold_date = decrypt(software_date)
            Software_sold_date = datetime.strptime(dec_sold_date, '%Y-%m-%d').strftime('%d/%m/%Y')

    except Exception as e:
        print(f"Error updating license: {e}")

    return mac, softwaretype, Software_sold_date

def validate(mac, softwaretype, Software_sold_date):          
    try:
        # Assuming the date format is 'DD/MM/YYYY'
        auth_status = 0

        current_date = datetime.now()
        Software_sold_date = datetime.strptime(Software_sold_date, '%d/%m/%Y')
        
        if gma(0) == mac:
            auth_status = 1 
            if softwaretype == 0:
                day_bal = (current_date - Software_sold_date).days
                # Check if the software is within the allowed time period (1 month)
                if (current_date - Software_sold_date ).days > 30:
                    auth_status = 2                                 
            else:
                day_bal = 0
                
    except ValueError as e:
        print("Error parsing release date:", e)
    return day_bal, auth_status 