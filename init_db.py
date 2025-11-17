# from werkzeug.security import generate_password_hash
# from config import postgreGetCon, sqliteCon

# # Connect to PostgreSQL
# # conn = postgreGetCon.get_db_connection()
# conn = sqliteCon.get_db_connection()
# cursor = conn.cursor()

# # Read and execute schema
# with open('schema.sql') as f:
#     sql_commands = f.read()

# for command in sql_commands.strip().split(';'):
#     command = command.strip()
#     if command:
#         print("Executing SQL command:\n", command)
#         try:
#             cursor.execute(command)
#         except Exception as e:
#             print("Error executing command:", e)

# # Insert default users
# users = [
#     ("superadmin", "etilorP$1234", "SuperAdmin", "Active"),
# ] 

# for username, password, role, status in users:
#     password_hash = generate_password_hash(password)
#     try:
#         cursor.execute(
#             "INSERT INTO users (username, password_hash, role, status) VALUES (%s, %s, %s, %s)",
#             (username, password_hash, role, status)
#         )
#         print(f"User {username} added.")
#     except Exception as e:
#         print(f"Error adding user {username}: {e}")

# conn.commit()
# conn.close()

from werkzeug.security import generate_password_hash
from config import sqliteCon  # Assuming sqliteCon.py has get_db_connection()

# === Connect to SQLite ===
conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
cursor = cursorWrite

# === Read and execute schema.sql ===
with open('schema.sql', 'r') as f:
    sql_commands = f.read()

# Split SQL commands and execute
for command in sql_commands.strip().split(';'):
    command = command.strip()
    if command:
        print("Executing SQL command:\n", command)
        try:
            cursor.execute(command)
        except Exception as e:
            print("Error executing command:", e)

# === Insert default users ===
users = [
    ("operator", "12345678", "Operator", "Active"),
    ("admin", "12345678", "Admin", "Active"),
]

for username, password, role, status in users:
    password_hash = generate_password_hash(password)
    try:
        cursor.execute(
            "INSERT INTO users (user_name, password_hash, role, status) VALUES (?, ?, ?, ?)",
            (username, password_hash, role, status)
        )
        print(f"User {username} added.")
    except Exception as e:
        print(f"Error adding user {username}: {e}")

# === Commit and close connection ===
conn.commit()
conn.close()
print("Database initialized successfully with default users.")
