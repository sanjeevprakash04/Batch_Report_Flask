from flask import Flask, render_template, request, redirect, url_for, session, jsonify, abort, g, send_file
from werkzeug.security import check_password_hash, generate_password_hash
import pandas as pd
from threading import Thread

#Modules
from auth import authLog, authMac
from config import postgreGetCon
from modules import monitor, main
app = Flask(__name__)
app.secret_key = '4f3d6e9a5f4b1c8d7e6a2b3c9d0e8f1a5b7c2d4e6f9a1b3c8d0e6f2a9b1d3c4'

def is_activated():
    try:
        engine, engineConRead, engineConWrite = postgreGetCon.get_db_connection_engine()
        df = pd.read_sql_query('SELECT * FROM "Info_DB"',engineConRead)
        
        activation_row = df.loc[df['Particulars'] == 'Activation_Key', 'Info']
        
        if activation_row.empty:
            return False
         
        activation_key = activation_row.values[0]
        return bool(activation_key and str(activation_key).strip())
    except Exception as e:
        print("Activation check error:", e)
        return False

@app.route('/')
def index():
    if not is_activated():
        return render_template('activation.html')
    return redirect(url_for('home'))

@app.route('/activate_license', methods=['POST'])
def activate_license():
    data = request.get_json()
    license_key = data.get('licenseKey')

    result = authMac.mac_insert(license_key)
    success = result and "successfully updated" in result.lower()

    return jsonify(success=success, message=result)

@app.route('/home')
def home():
    if 'username' in session:
        return redirect(url_for('dashboard', user=session['username'], role=session.get('role')))
    else:
        return redirect(url_for('dashboard'))

@app.route('/dashboard')
def dashboard():
    return render_template('dashboard.html')

@app.route('/logs')
def logs():
    return render_template('logs.html')

@app.route('/recipe')
def recipe():
    return render_template('recipe.html')

@app.route('/report')
def report():
    return render_template('report.html')

@app.route('/settings')
def settings():
    return render_template('settings.html')

@app.route('/stocks')
def stocks():
    return render_template('stocks.html')

@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/super_admin')
def super_admin():
    user_logged_in = 'username' in session
    df = postgreGetCon.dfUser()
    table_html = df.to_html(classes='table table-striped', index=False, escape=False, table_id='inventory-table')
    return render_template('super_admin.html', table=table_html, user_logged_in=user_logged_in)

@app.route('/update_user_password', methods=['POST'])
def update_user_password():
    data = request.get_json()
    username = data.get('username')
    new_password = data.get('new_password')

    if not username or not new_password:
        return jsonify(success=False, error="Invalid data")

    try:
        conn = postgreGetCon.get_db_connection()
        cur = conn.cursor()
        hashed = generate_password_hash(new_password)

        cur.execute("UPDATE users SET password_hash = ? WHERE username = ?", (hashed, username))
        conn.commit()
        cur.close()
        conn.close()
        return jsonify(success=True)
    except Exception as e:
        return jsonify(success=False, error=str(e))

@app.route('/login', methods=['POST'])
def login():
    username = request.form['username']
    password = request.form['password']

    user = authLog.get_user(username)
    if user and check_password_hash(user[2], password):
        session.permanent = True
        session['username'] = user[1]
        session['role'] = user[3]
        return jsonify(success=True)
    else:
        return jsonify(success=False, error="Invalid Credentials"), 403

@app.route('/change_password', methods=['POST'])
def change_password():
    if 'username' not in session:
        return jsonify(success=False, error="Not logged in"), 403

    data = request.get_json()
    old_password = data.get('oldPassword')
    new_password = data.get('newPassword')

    username = session['username']

    conn = postgreGetCon.get_db_connection()
    if conn is None:
        return jsonify(success=False, error="Database connection error"), 500

    try:
        cur = conn.cursor()
        # Fetch the stored password hash
        cur.execute('SELECT password_hash FROM users WHERE username = ?', (username,))
        row = cur.fetchone()

        if not row:
            return jsonify(success=False, error="User not found"), 404

        stored_password_hash = row[0]

        if check_password_hash(stored_password_hash, old_password):
            # If old password matches, update with new password
            new_password_hash = generate_password_hash(new_password)

            cur.execute('UPDATE users SET password_hash = ? WHERE username = ?', (new_password_hash, username))
            conn.commit()

            return jsonify(success=True)
        else:
            return jsonify(success=False, error="Old password is incorrect."), 400

    except Exception as e:
        print("Error during password change:", e)
        return jsonify(success=False, error=str(e)), 500

    finally:
        cur.close()
        conn.close()


@app.route('/logout')
def logout():
    session.clear()
    return redirect(url_for('home'))

@app.before_request
def start_plc_monitoring():
    if not monitor.plc_running:
        # print("🔥 Starting PLC monitoring...")
        monitor.plc_running = True
        monitor.plc_thread = Thread(target=monitor.trigger_connect)
        monitor.plc_thread.daemon = True
        monitor.plc_thread.start()
        # print(" PLC monitoring started on app start.")

@app.route('/stop_plc', methods=['POST'])
def stop_plc():
    # print(" /stop_plc endpoint called!")
    if monitor.plc_running:
        monitor.plc_running = False
        # print(" PLC monitoring stopped on tab/window close.")
    # print(" Terminating server process.")
    # os._exit(0)
    return '', 204

@app.errorhandler(403)
def forbidden(e):
    return render_template('403.html'), 403

@app.before_request
def load_user():
    g.user = session.get('username')
    g.role = session.get('role')

@app.context_processor
def inject_user():
    return dict(user=session.get('username'), role=session.get('role'))

if __name__ == '__main__':
    app.run(debug=True, host='127.0.0.1', port=5000)