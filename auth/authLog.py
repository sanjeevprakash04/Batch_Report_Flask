# from config.postgreGetCon import get_db_connection
from config.sqliteCon import get_db_connection
from werkzeug.security import check_password_hash
from datetime import datetime

def get_user(username):
    current_time = datetime.now().strftime('%Y-%m-%d %H:%M:%S')

    # Unpack from your sqliteCon method
    conn, cursorRead, cursorWrite = get_db_connection()
    cursor = cursorWrite

    # Fetch the user
    cursor.execute('SELECT * FROM users WHERE user_name = ?', (username,))
    user = cursor.fetchone()

    # Update last_login if user found
    if user:
        cursor.execute('UPDATE users SET last_login = ? WHERE user_name = ?', (current_time, username))
        conn.commit()

    conn.close()
    return user
