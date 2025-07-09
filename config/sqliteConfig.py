import sqlite3
import pandas as pd
from sqlalchemy import create_engine
from config.config import DB_PATH

def get_db_connection():
    try:
        conn = sqlite3.connect(DB_PATH)
        conn.row_factory = sqlite3.Row  # for dictionary-like row access
        return conn
    except Exception as e:
        print(f"Failed to connect to SQLite: {e}")
        return None
    
def get_db_connection_engine():
    try:
        engine = create_engine(f"sqlite:///{DB_PATH}")
        return engine
    except Exception as e:
        print(f"Failed to create SQLAlchemy engine: {e}")
        return None

def dfUser():
    conn = get_db_connection()
    query = "SELECT id, username, role, last_login FROM users WHERE role != 'SuperAdmin';"
    df = pd.read_sql_query(query, conn)
    df.columns = ['Id', 'Username', 'Role', 'LastLogin']
    df = df.sort_values(by='Id', ascending=True)
    return df



def sqlite():
    conn = sqlite3.connect('CodeX-Config.db')
    cursorRead = conn.cursor()
    cursorWrite = conn.cursor()
    engine = create_engine('sqlite:///CodeX-Config.db')
    engineConRead = engine.connect()
    engineConWrite = engine.connect()
    return cursorRead, cursorWrite, engineConRead, engineConWrite, conn
