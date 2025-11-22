import logging
from config import sqliteCon
from plc_connection import pylogix
from sqlalchemy import text
from modules import Report

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




def data_process(hours, from_time, to_time):
    try:
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()
        df = sqliteCon.data_batch(conn, hours, from_time, to_time, engineConRead)

        if df is not None and not df.empty:
            # Define and filter columns
            column_order = [
                "BatchNo", "TimeStamp", "Plant Name", "Recipe Name",
                "Start Date Time", "End Date Time", "Total Batch Weight"
            ]
            existing_columns = [col for col in column_order if col in df.columns]
            df = df[existing_columns]

            # Convert to JSON-safe types
            df = df.astype(str)
            df["BatchNo"] = df["BatchNo"].astype(int)
            # Sort by column 'Score' in descending order
            df_sorted = df.sort_values(by='BatchNo', ascending=False)

            total_weight_tons = round(float(df["Total Batch Weight"].astype(float).sum() / 1000), 2)

            return {
                "success": True,
                "data": df_sorted.to_dict(orient="records"),
                "total_weight": total_weight_tons
            }

        return {"success": True, "data": [], "total_weight": 0.0}

    except Exception as e:
        print(f"❌ Error in data_process: {e}")
        return {"success": False, "error": str(e)}

def plc_data_process(batch_no):
    try:
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()
        # Query database for plc_data of selected BatchNo
        query = f"""
            SELECT *
            FROM plc_data
            WHERE BatchNo == '{batch_no}'
        """
        df = pd.read_sql_query(query, engineConRead)
        

        if df.empty:
            return {"success": True, "data": []}
        # Separate 'Info' category and transform data
        df_string = df[df["Category"] == "Info"].copy()
        df = df[df["Category"] != "Info"].copy()
        # print(df_string)
        df_pivot = df.pivot(index='Category', columns='Name', values='Value')

        # Maintain column order consistency
        original_order = df['Name'].unique()
        df_pivot = df_pivot[original_order].reset_index()

        # Sort by numerical silo category if present
        df_pivot['Category_numeric'] = df_pivot['Category'].str.extract(r'Silo-(\d+)').astype(float)
        df_pivot = df_pivot.sort_values('Category_numeric').drop('Category_numeric', axis=1).reset_index(drop=True)

        # Compute 'Difference' column
        df_pivot["Difference"] = df_pivot.apply(lambda row: Report.difference(row["SetWeight"], row["ActualWeight"]), axis=1)

        # Retrieve DailyBatchNo
        query_daily_batch = f"""
            SELECT DISTINCT DailyBatchNo
            FROM plc_data
            WHERE BatchNo == '{batch_no}'
        """
        df_daily_batch = pd.read_sql_query(query_daily_batch, engineConRead)
        # print(df_daily_batch)
        # Ensure DailyBatchNo exists and is assigned
        if not df_daily_batch.empty:
            DailyBatchNo = df_daily_batch.iloc[0]['DailyBatchNo']
            print(f"BatchNo for {batch_no}: {DailyBatchNo}")
        else:
            DailyBatchNo = None
            print(f"No DailyBatchNo found for BatchNo: {batch_no}")

        # Compute "State" but do not add it to df
        state_dict = df_pivot.apply(lambda row: Report.check(row["SetWeight"], row["ActualWeight"], row["Tolerance"]), axis=1)

        # Define displayed columns (excluding "State")
        column_order = ["Category", "SiloNo", "MaterialName", "SetWeight", "ActualWeight", "Difference", "Tolerance"]
        df_pivot = df_pivot[column_order]  # Only include these columns in the table
        df_pivot["SiloNo"] = df_pivot["SiloNo"].astype(int)

        return df_pivot

    except Exception as e:
        print(f"❌ Error in plc_data_process: {e}")
        return {"success": False, "error": str(e)}

def report_data_process(batch_no):
    try:
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()

        query = f"SELECT * FROM plc_data WHERE BatchNo = '{batch_no}'"
        df = pd.read_sql_query(query, engineConRead)

        if df.empty:
            return pd.DataFrame(), pd.DataFrame(), None

        df_string = df[df["Category"] == "Info"].copy()
        df = df[df["Category"] != "Info"].copy()

        df_pivot = df.pivot(index='Category', columns='Name', values='Value')
        original_order = df['Name'].unique()
        df_pivot = df_pivot[original_order].reset_index()

        df_pivot['Category_numeric'] = df_pivot['Category'].str.extract(r'Silo-(\d+)').astype(float)
        df_pivot = df_pivot.sort_values('Category_numeric').drop('Category_numeric', axis=1).reset_index(drop=True)

        df_pivot["Difference"] = df_pivot.apply(lambda r: Report.difference(r["SetWeight"], r["ActualWeight"]), axis=1)

        query_daily = f"SELECT DISTINCT DailyBatchNo FROM plc_data WHERE BatchNo = '{batch_no}'"
        df_daily = pd.read_sql_query(query_daily, engineConRead)
        daily_batch_no = df_daily.iloc[0]['DailyBatchNo'] if not df_daily.empty else None

        # Define displayed columns (excluding "State")
        column_order = ["Category", "SiloNo", "MaterialName", "SetWeight", "ActualWeight", "Difference", "Tolerance"]
        df_pivot = df_pivot[column_order]  # Only include these columns in the table
        df_pivot["SiloNo"] = df_pivot["SiloNo"].astype(int)
        
        return df_pivot, df_string, daily_batch_no
    except Exception as e:
        print(f"❌ Error in plc_data_process: {e}")
        return {"success": False, "error": str(e)}


