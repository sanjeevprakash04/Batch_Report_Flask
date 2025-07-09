import sqlite3
from werkzeug.security import generate_password_hash

# Connect to DB
conn = sqlite3.connect('batch_report.db')
cursor = conn.cursor()

# Create table
with open('schema.sql') as f:
    cursor.executescript(f.read())

# Add users
users = [
    ("superadmin", "etilorP$1234", "SuperAdmin", "Active"),
]

for username, password, role, status in users:
    password_hash = generate_password_hash(password)
    try:
        cursor.execute("INSERT INTO users (username, password_hash, role, status) VALUES (?, ?, ?, ?)",
                       (username, password_hash, role, status))
        print(f"User {username} added.")
    except sqlite3.IntegrityError:
        print(f"User {username} already exists.")

conn.commit()
conn.close()