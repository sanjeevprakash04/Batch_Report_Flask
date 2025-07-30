import psycopg2
from psycopg2.extras import DictCursor
from sqlalchemy import create_engine
import pandas as pd
from config.config import DB_CONFIG

def get_db_connection():
    try:
        conn = psycopg2.connect(
            dbname=DB_CONFIG['dbname'],
            user=DB_CONFIG['user'],
            password=DB_CONFIG['password'],
            host=DB_CONFIG['host'],
            port=DB_CONFIG['port'],
            cursor_factory=DictCursor
        )
        return conn
    except Exception as e:
        print(f"Failed to connect to PostgreSQL: {e}")
        return None

def get_db_connection_engine():
    try:
        db_url = (
            f"postgresql+psycopg2://{DB_CONFIG['user']}:{DB_CONFIG['password']}@"
            f"{DB_CONFIG['host']}:{DB_CONFIG['port']}/{DB_CONFIG['dbname']}"
        )
        engine = create_engine(db_url)
        return engine
    except Exception as e:
        print(f"Failed to create SQLAlchemy engine: {e}")
        return None
    
def dfUser():
    conn = get_db_connection_engine()
    query = """ SELECT id, user_name, role, last_login FROM users WHERE role != 'SuperUser'; """
    df = pd.read_sql_query(query, conn)
    df.columns = ['Id', 'Username', 'Role', 'LastLogin']
    df = df.sort_values(by='Id', ascending=True)
    return df

def insertBatch(df):
    print("Starting batch insertion...")

    # Get PostgreSQL connections
    cursorRead, cursorWrite, engineConRead, engineConWrite, conn = sqlite()  # Rename function if needed

    if df.empty:
        print("Dataframe is empty. No data to insert.")
        return

    # Separate 'Info' category from other data
    df_string = df[df["Category"] == "Info"].copy()
    df = df[df["Category"] != "Info"].copy()
    df_string = df_string.reset_index(drop=True)

    if df_string.empty:
        print("No 'Info' category data found. Skipping batch metadata insertion.")
    else:
        # Pivot Info data
        df_pivot_1 = df_string.pivot(index='BatchNo', columns='Name', values='Value')

        # Add Timestamp column from unique batch entries
        df_pivot_1['TimeStamp'] = df_string.drop_duplicates(subset='BatchNo').set_index('BatchNo')['Timestamp']
        df_pivot_1 = df_pivot_1.reset_index()

        # Define expected column order (adjust as per your DB schema)
        column_order = ["BatchNo", "DailyBatchNo", "TimeStamp", "Plant Name", "Recipe Name", "Start Date Time", "End Date Time"]
        df_pivot_1 = df_pivot_1.reindex(columns=column_order, fill_value=None)

        # Total Batch Weight calculation
        df_weight = df[df["Name"] == "ActualWeight"].groupby("BatchNo")["Value"].sum().reset_index()
        df_weight["Total Batch Weight"] = df_weight["Value"].round(2)
        df_weight.drop(columns=["Value"], inplace=True)

        # Add DailyBatchNo to pivoted data (from main df)
        df_daily_batch = df[['BatchNo', 'DailyBatchNo']].drop_duplicates()
        df_pivot_1 = df_pivot_1.merge(df_daily_batch, on="BatchNo", how="left")

        # Merge weights
        df_pivot_1 = df_pivot_1.merge(df_weight, on="BatchNo", how="left")

        # Insert into PostgreSQL using to_sql
        try:
            df_pivot_1.to_sql("batches", con=engineConWrite, if_exists="append", index=False, method='multi')
            print("✅ Batch metadata inserted successfully into PostgreSQL.")
        except Exception as e:
            print(f"❌ Error during PostgreSQL insertion: {e}")

    print("Batch insertion completed.")

def insertMaterialExtraction(dfPlcdb, engineConRead, cursorWrite, conn):
    try:
        # === Material Index Preparation ===
        dfPlcdb['MaterialIndex'] = dfPlcdb.loc[dfPlcdb['Name'] == 'MaterialName', 'Value']
        dfPlcdb['MaterialIndex'] = dfPlcdb.groupby('Category')['MaterialIndex'].transform(lambda x: x.ffill().bfill())
        dfPlcdb = dfPlcdb.infer_objects(copy=False)  # Fix FutureWarning

        # === Filter & Pivot ===
        df_filtered = dfPlcdb[dfPlcdb['Name'].isin(['ActualWeight', 'SetWeight'])].reset_index(drop=True)
        df_pivot = df_filtered.pivot(index='MaterialIndex', columns='Name', values='Value')
        df_pivot = df_pivot.reset_index().rename(columns={'MaterialIndex': 'MaterialName'})

        # Convert ActualWeight from kg to tons
        df_pivot['ActualWeight'] = df_pivot['ActualWeight'].div(1000).round(2)

        # === Load Existing MaterialData Table ===
        existing_data = pd.read_sql('SELECT "SiloNo", "MaterialName", "TotalExtracted" FROM "MaterialData"', con=engineConRead)

        # === Merge & Calculate ===
        df_merged = pd.merge(df_pivot, existing_data, on='MaterialName', how='inner')
        df_merged['ActualWeight'] = pd.to_numeric(df_merged['ActualWeight'], errors='coerce').fillna(0)
        df_merged['TotalExtracted'] = pd.to_numeric(df_merged['TotalExtracted'], errors='coerce').fillna(0)
        df_merged['TotalWeight'] = df_merged['ActualWeight'] + df_merged['TotalExtracted']

        # === Debug Output ===
        print(df_merged)

        # === Update PostgreSQL Using Parameterized Query ===
        for index, row in df_merged.iterrows():
            update_query = """
            UPDATE "MaterialData"
            SET "TotalExtracted" = %s
            WHERE "MaterialName" = %s;
            """
            cursorWrite.execute(update_query, (row['TotalWeight'], row['MaterialName']))

        conn.commit()
        print("✅ TotalWeight values successfully updated in MaterialData (PostgreSQL).")

    except Exception as e:
        print("❌ Error occurred:", e)







