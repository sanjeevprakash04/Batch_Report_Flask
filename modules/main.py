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
        

        df_cal_sum = df[df["Category"] == "Summary"].copy()

        # Convert only numeric values
        numeric_vals = pd.to_numeric(df_cal_sum["Value"], errors="coerce")

        # Round numeric ones to 2 decimals and keep date/time as original
        df_cal_sum["Value"] = numeric_vals.round(2).astype(str).where(
            ~numeric_vals.isna(),  # if numeric → keep rounded
            df_cal_sum["Value"]    # if non-numeric (date/time) → keep original
        )

        # Remove Info and Summary rows
        df = df[~df["Category"].isin(["Info", "Summary"])].copy()


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
        
        return df_pivot, df_string, daily_batch_no, df_cal_sum
    except Exception as e:
        print(f"❌ Error in plc_data_process: {e}")
        return {"success": False, "error": str(e)}


def dashboard_calculations(start_timestamp, end_timestamp):
    try:
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()

        # ===============================
        # 1. READ PLC DATA
        # ===============================
        query_plc = f"""
            SELECT *
            FROM plc_data
            WHERE TimeStamp BETWEEN '{start_timestamp}' AND '{end_timestamp}'
        """
        df_plc = pd.read_sql_query(query_plc, engineConRead)

        if df_plc.empty:
            return {"status": "error", "message": "No PLC data found"}

        # ===============================
        # 2. READ BATCH LOGS
        # ===============================
        query_batches = f"""
            SELECT *
            FROM Batches
            WHERE TimeStamp BETWEEN '{start_timestamp}' AND '{end_timestamp}'
        """
        df_batches = pd.read_sql_query(query_batches, engineConRead)

        if df_batches.empty:
            return {"status": "error", "message": "No batch records found"}

        df_batches["TimeStamp"] = pd.to_datetime(df_batches["TimeStamp"], errors="coerce")
        df_batches["Hour"] = df_batches["TimeStamp"].dt.floor("h")  # FIXED: no warning

        # ============================================================
        # LINE CHART: HOURLY BATCH COUNTS 
        # ============================================================
        batch_counts_per_hr = (
            df_batches.groupby("Hour")["BatchNo"]
            .nunique()
            .reset_index(name="BatchCount")
        )

        batch_counts_per_hr["Hour"] = batch_counts_per_hr["Hour"].astype(str)

        # ============================================================
        # SUMMARY
        # ============================================================
        df_prod = df_plc[df_plc["Name"] == "TotalBatchActualWeight"].copy()
        total_production_tons = round(df_prod["Value"].sum() / 1000.0, 2)

        number_of_batches = int((df_plc["Name"] == "Recipe Name").sum())

        tph = int(batch_counts_per_hr["BatchCount"].max()) if not batch_counts_per_hr.empty else 0

        df_acc = df_plc[df_plc["Name"] == "BatchAccuracy"].copy()
        batch_accuracy = round(df_acc["Value"].mean(), 2) if not df_acc.empty else 0.0

        df_cycle = df_plc[df_plc["Name"] == "BatchTimeMinutes"].copy()
        avg_cycle_time = round(df_cycle["Value"].mean(), 2) if not df_cycle.empty else 0.0

        # ============================================================
        # BAR CHART — RAW MATERIAL
        # ============================================================
        df_filtered = df_plc[~df_plc["Category"].isin(["Info", "Summary"])].copy()
        weights_df = df_filtered[df_filtered["Name"].isin(["ActualWeight", "SetWeight"])].copy()

        # FIXED: SettingWithCopyWarning
        weights_df.loc[:, "Value"] = pd.to_numeric(weights_df["Value"], errors="coerce")

        pivot_bar = (
            weights_df.pivot_table(
                index="Category",
                columns="Name",
                values="Value",
                aggfunc="mean",
                fill_value=0
            )
            .reset_index()
        )

        bar_chart_data = pivot_bar.to_dict(orient="records")

        # ============================================================
        # DONUT CHART — RECIPE %
        # ============================================================
        recipe_df = df_plc[df_plc["Name"] == "Recipe Name"].copy()
        recipe_counts = recipe_df["Value"].value_counts().reset_index()
        recipe_counts.columns = ["RecipeName", "Count"]

        recipe_chart_data = recipe_counts.to_dict(orient="records")

        # ============================================================
        # FINAL JSON
        # ============================================================
        return {
            "status": "success",
            "summary": {
                "total_production_tons": total_production_tons,
                "num_batches": number_of_batches,
                "tph": tph,
                "batch_accuracy": batch_accuracy,
                "avg_cycle_time": avg_cycle_time
            },
            "line_chart": batch_counts_per_hr.to_dict(orient="records"),
            "recipe_chart": recipe_chart_data,
            "raw_material_chart": bar_chart_data
        }

    except Exception as e:
        print("❌ Error in dashboard_calculations:", e)
        return {"status": "error", "message": str(e)}


