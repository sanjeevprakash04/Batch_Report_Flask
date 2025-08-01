import logging
from config.postgreGetCon import get_db_connection, get_db_connection_engine
from plc_connection import pylogix
from sqlalchemy import text

from itertools import product
from datetime import datetime
import pandas as pd
import time
from flask import session
import psycopg2
from psycopg2 import sql
# === Logging Setup ===
logging.basicConfig(
    filename='plc_monitor.log',
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)

rows = 5
columns = 20
sides = ['A', 'B']
search_order = [f"{r:02}{c:02}{s}" for c, r, s in product(range(1, columns + 1), range(1, rows + 1), sides)]

latest_data = {}
retrieval_cancelled = False

def get_latest_data():
    return latest_data

def set_latest_data(data):
    global latest_data
    latest_data = data

def boxIn(boxready, barcode, status):
    if boxready == True and barcode:
        conn = get_db_connection_engine()
        query = "SELECT * FROM storage;"
        df = pd.read_sql_query(query, conn)
        fill_in = find_next_empty_cell_fast(df)
        logging.info(f"BoxIn called: barcode={barcode}, assigned_cell={fill_in}")
        return fill_in
    elif boxready == False and status == 0:
        return "Box not yet placed"
    else:
        return "PlcDisconnected"

def find_next_empty_cell_fast(df):
    occupied_cells = set(df[df['status'].isin(['Occupied', 'Excluded'])]['cell_name'])
    for cell in search_order:
        if cell not in occupied_cells:
            logging.debug(f"Next empty cell found: {cell}")
            return cell
    logging.warning("No empty cell found")
    return None

def updateDB(place, barcode, dfread):
    try:
        
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        
        con = get_db_connection()
        
        conn = get_db_connection_engine()
        
        query = "SELECT * FROM product;"
        
        dfpro = pd.read_sql_query(query, conn)
        product_code_series = dfpro[dfpro['barcode'] == barcode]['product_code']

        if product_code_series.empty:
            logging.warning(f"No product code found for barcode: {barcode}")
            return "Product code not found"

        product_code = product_code_series.iloc[0]
        update_query = """
            UPDATE storage 
            SET product_code = %s, barcode = %s, timestamp = %s, activity_type = %s, status = %s, "user" = %s
            WHERE cell_name = %s;
        """
        cursor = con.cursor()
        cursor.execute(update_query, (product_code, barcode, current_time, "Stored", "Occupied", "Admin", place))
        con.commit()
        cursor.close()
        con.close()
        logging.info(f"Storage updated: Cell={place}, Barcode={barcode}, ProductCode={product_code}")
        return "Success"

    except Exception as e:
        logging.exception(f"Error updating DB for place={place}, barcode={barcode}")
        return "Error"

def update4retrival(place):
    try:
        current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        con = get_db_connection()
        conn = get_db_connection_engine()
        current_user = session.get('role')
        if not current_user:
            current_user = "Admin"
        update_query = """
            UPDATE storage 
            SET product_code = %s, barcode = %s, timestamp = %s, activity_type = %s, status = %s, "user" = %s
            WHERE cell_name = %s;
        """
        cursor = con.cursor()
        cursor.execute(update_query, ("", "", current_time, "Modified", "Empty", current_user, place))
        con.commit()
        # cursor.close()
        # con.close()
        logging.info(f"Product retrieved and cell cleared: {place}")
        return "Success"
    except Exception as e:
        logging.exception(f"Error updating DB during retrieval for cell={place}")
        return "Error"

def product_table():
    try:
        conn = get_db_connection_engine()
        query = "SELECT * FROM product"
        dfpro = pd.read_sql_query(query, conn)
        dfpro.columns = ['Id', 'Barcode', 'ProductCode', 'ProductDetails', 'CreatedAt', 'UpdatedAt', 'User']
        dfpro = dfpro.sort_values(by='Id', ascending=True)
        logging.info("Product table fetched successfully")
        return dfpro
    except Exception as e:
        logging.exception("Error fetching product table")
        return pd.DataFrame()

def receive(ids):
    try:

        if not isinstance(ids, list):
            ids = [ids]

        ids = [int(i) for i in ids]  # Ensure integers

        conn = get_db_connection_engine()

        # Use SQLAlchemy's `text()` and `tuple` parameter substitution
        query = text("SELECT * FROM storage WHERE Id = ANY(:ids)")

        with conn.connect() as connection:
            df = pd.read_sql_query(query, con=connection, params={"ids": ids})

        logging.info(f"Received data for IDs: {ids}")

        return df

    except Exception as e:
        logging.exception(f"Error receiving data for IDs: {ids}")
        return pd.DataFrame()

def retrival_place(dfretPlace):
    try:
        conn = get_db_connection_engine()
        query = 'SELECT * FROM "Data";'
        dfPlcdb = pd.read_sql_query(query, conn)

        query = 'SELECT * FROM "Info_DB";'
        dfInfo = pd.read_sql_query(query, conn)

        plcIP = dfInfo.loc[0, 'Info']

        plc = pylogix.connectABPLC(plcIP)

        tag_name = dfPlcdb.loc[dfPlcdb['Name'] == 'RetrivalStatus', 'Tag_name'].iloc[0]

        status_ret, _ = pylogix.readABPLC(plc, tag_name, "INT")

        # status_ret = 1
        if status_ret == 1:
            pylogix.writeinAb(plc, "Write.RetrievalStatus", 1)
            for idx, row_data in dfretPlace.iterrows():
                
                Ref = row_data.iloc[0]  # Safe positional access


                place = row_data.get('cell_name')
                if place and isinstance(place, str) and len(place) >= 5:
                    row = place[:2]
                    column = place[2:4]
                    side = place[4]
                    try:
                        res = pylogix.writeplace(dfPlcdb, int(row), int(column), side, plc)
                        logging.info(f"Sent place {place} to PLC for retrieval")

                        while True:
                            status_ret, _ = pylogix.readABPLC(plc, tag_name, "INT")


                            if status_ret == 3:
                                pylogix.writeinAb(plc, tag_name, 1)

                                update4retrival(place)
                                query = 'SELECT * FROM storage'
                                dfstorage = pd.read_sql_query(query, conn)
                                pylogix.write_product_code(plc, dfstorage)
                                logging.info(f"Retrieval completed for {place}")
                                data_json = {str(Ref): f"Retrieval completed for {place}", "ref_id": Ref}
                                set_latest_data(data_json)
                                
                                break
                            elif status_ret == 2:
                                data_json = {str(Ref): f"Retrieval Processing for {place}", "ref_id": Ref}
                                set_latest_data(data_json)
                            elif retrieval_cancelled:
                                logging.info(f"Retrieval cancelled by user for {place}")
                                break  # ✅ Exit loop if cancel flag is set

                            time.sleep(5)

                    except Exception as e:
                        logging.exception(f"Failed to write place {place} to PLC")
                        
            pylogix.writeinAb(plc, "Write.RetrievalStatus", 0)

        elif status_ret == 2:
            logging.warning("Not ready for retrieval")
            return "Not Ready for Retrieval"
        else:
            logging.error("Technical error during retrieval")
            return "Technical Error"

        return dfPlcdb

    except Exception as e:
        logging.exception("Error during retrieval operation")
        return None

def importProduct(df, table_name='product', id_col='id'):
    try:
        con = get_db_connection()
        cur = con.cursor()

        # Step 1: Clean and format values
        for i, row in df.iterrows():
            columns = list(row.index)
            values = []

            for val in row.values:
                if pd.isna(val):
                    values.append(None)
                elif isinstance(val, pd.Timestamp):
                    values.append(val.strftime('%Y-%m-%d %H:%M:%S'))
                else:
                    values.append(str(val))

            try:
                # Build ON CONFLICT UPDATE clause (skip primary key)
                update_clause = sql.SQL(', ').join(
                    sql.SQL(f'"{col}" = EXCLUDED."{col}"') for col in columns if col != id_col
                )

                # Build full INSERT ... ON CONFLICT DO UPDATE query
                insert_query = sql.SQL("""
                    INSERT INTO {} ({}) VALUES ({})
                    ON CONFLICT ({}) DO UPDATE SET {}
                """).format(
                    sql.Identifier(table_name),
                    sql.SQL(', ').join(map(sql.Identifier, columns)),
                    sql.SQL(', ').join(sql.Placeholder() * len(values)),
                    sql.Identifier(id_col),
                    update_clause
                )

                cur.execute(insert_query, values)

            except Exception as e:
                print(f" Failed to insert/update row {i}: {e}")
                con.rollback()
                continue
            
        con.commit()
        
        print(" Product data imported and updated successfully.")

    except Exception as e:
        print(f" ERROR: {e}")
        

def df_split(dfPlcdb):
    try:
        if not dfPlcdb.loc[dfPlcdb['Sample_mode'] == "Trigger"].empty:
            dfplcdb_Periodic = dfPlcdb[dfPlcdb["Sample_mode"] == "Periodic"]
            #spliting DF for Trigger and Periodic
            unique_triggers = dfPlcdb['Trigger'].dropna().unique()
            df_trigger = dfPlcdb[dfPlcdb["Name"].isin(unique_triggers)]
            
            # Create DataFrames based on unique triggers and store them in the dictionary
            for a in unique_triggers:
                # setattr(self, a, dfPlcdb[dfPlcdb['Trigger'] == a])
                globals()[a] = dfPlcdb[dfPlcdb['Trigger'] == a]
                
        return dfPlcdb, df_trigger, dfplcdb_Periodic            

    except Exception as e:    
        print(f" ERROR: {e}")




