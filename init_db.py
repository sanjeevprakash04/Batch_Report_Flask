from werkzeug.security import generate_password_hash
from config import postgreGetCon

# Connect to PostgreSQL
conn = postgreGetCon.get_db_connection()
cursor = conn.cursor()

# Read and execute schema
with open('schema.sql') as f:
    sql_commands = f.read()

for command in sql_commands.strip().split(';'):
    command = command.strip()
    if command:
        print("Executing SQL command:\n", command)
        try:
            cursor.execute(command)
        except Exception as e:
            print("Error executing command:", e)

# Insert default users
users = [
    ("superadmin", "etilorP$1234", "SuperAdmin", "Active"),
]

for username, password, role, status in users:
    password_hash = generate_password_hash(password)
    try:
        cursor.execute(
            "INSERT INTO users (username, password_hash, role, status) VALUES (%s, %s, %s, %s)",
            (username, password_hash, role, status)
        )
        print(f"User {username} added.")
    except Exception as e:
        print(f"Error adding user {username}: {e}")

conn.commit()
conn.close()
