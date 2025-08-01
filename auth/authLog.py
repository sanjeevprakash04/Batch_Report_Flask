from config.postgreGetCon import get_db_connection

from werkzeug.security import check_password_hash
from datetime import datetime

def get_user(username):
    current_time = datetime.now()
    current_time_str = current_time.strftime('%Y-%m-%d %H:%M:%S')
    conn = get_db_connection()
    cursor = conn.cursor()
    
    # Fetch the user
    cursor.execute('SELECT * FROM users WHERE username = ?', (username,))
    user = cursor.fetchone()
    
    if user:
        # Update the last_login time
        cursor.execute('UPDATE users SET last_login = ? WHERE username = ?', (current_time_str, username))
        conn.commit()
    
    conn.close()
    return user
