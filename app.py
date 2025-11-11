from flask import Flask, render_template, request, redirect, url_for, session, jsonify, abort, g, send_file, Response
from werkzeug.security import check_password_hash, generate_password_hash
import pandas as pd
from threading import Thread
from datetime import datetime
import io
import json

#Modules
from auth import authLog, authMac
from config import sqliteCon
from modules import monitor, main, Report
app = Flask(__name__)
app.secret_key = '4f3d6e9a5f4b1c8d7e6a2b3c9d0e8f1a5b7c2d4e6f9a1b3c8d0e6f2a9b1d3c4'

# def is_activated():
#     try:
#         engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()
#         df = pd.read_sql_query('SELECT * FROM "Info_DB"',engineConRead)
        
#         activation_row = df.loc[df['Particulars'] == 'Activation_Key', 'Info']
        
#         if activation_row.empty:
#             return False
         
#         activation_key = activation_row.values[0]
#         return bool(activation_key and str(activation_key).strip())
#     except Exception as e:
#         print("Activation check error:", e)
#         return False

@app.route('/')
def index():
    # if not is_activated():
    #     return render_template('activation.html')
    return redirect(url_for('home'))

# @app.route('/activate_license', methods=['POST'])
# def activate_license():
#     data = request.get_json()
#     license_key = data.get('licenseKey')

#     result = authMac.mac_insert(license_key)
#     success = result and "successfully updated" in result.lower()

#     return jsonify(success=success, message=result)

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

# ---- Get all recipes (tree view) ----
@app.route("/api/recipes", methods=["GET"])
def get_recipes():
    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
    cursorRead.execute("SELECT DISTINCT category FROM recipes")
    categories = cursorRead.fetchall()
    
    tree_data = []
    for (category,) in categories:
        cursorRead.execute("SELECT name FROM recipes WHERE category=?", (category,))
        recipes = [r[0] for r in cursorRead.fetchall()]
        print("Category:", category, "Recipes:", recipes)
        tree_data.append({"category": category, "recipes": recipes})
    
    conn.close()
    return jsonify(tree_data)


# ---- Add a new recipe ----
@app.route("/api/recipes", methods=["POST"])
def add_recipe():
    data = request.json
    name = data.get("name")
    category = data.get("category", "Recipes")  # Default parent category
    
    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
    cursorWrite.execute("INSERT INTO recipes (name, category) VALUES (?, ?)", (name, category))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


# ---- Delete a recipe ----
@app.route("/api/recipes/<string:name>", methods=["DELETE"])
def delete_recipe(name):
    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
    cursorWrite.execute("DELETE FROM recipes WHERE name=?", (name,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})


# ---- Load recipe data table ----
@app.route("/api/recipes/<string:category>/data", methods=["GET"])
def get_recipe_data(category):
    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

    # Query same as displaytable() logic
    query = """
        SELECT r."Index", r.SiloNo, m.MaterialName, r.SetWeight, r.FineWeight, r.Tolerance
        FROM recipeData r
        LEFT JOIN MaterialData m ON r.SiloNo = m.SiloNo
        WHERE r.Category = ?
        ORDER BY r.SiloNo
    """
    cursorRead.execute(query, (category,))
    data = cursorRead.fetchall()
    columns = [desc[0] for desc in cursorRead.description]

    conn.close()
    return jsonify([dict(zip(columns, row)) for row in data])
@app.route('/report')
def report():
    return render_template('report.html')

@app.route('/api/report_data', methods=['POST'])
def api_report_data():
    try:
        payload = request.get_json(force=True)
        hours = payload.get('hours')
        from_time = payload.get('from_time')
        to_time = payload.get('to_time')

        print(f"📥 Received Filters → Hours: {hours}, From: {from_time}, To: {to_time}")

        result = main.data_process(hours, from_time, to_time)

        # Ensure everything is JSON serializable
        safe_result = {
            "success": bool(result.get("success", False)),
            "data": result.get("data", []),
            "total_weight": float(result.get("total_weight", 0.0))
        }

        # ✅ Return as clean, compact JSON (no pretty-print)
        response_json = json.dumps(safe_result, separators=(',', ':'))
        return Response(response_json, mimetype='application/json')

    except Exception as e:
        print(f"❌ API Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    

# @app.route("/api/plc_data", methods=["POST"]) 
# def get_plc_data():
#     """Fetch PLC data for a given BatchNo"""
#     try:
#         data = request.get_json()
#         batch_no = data.get("batch_no")
        

#         if not batch_no:
#             return jsonify({"success": False, "error": "BatchNo missing"}), 400

#         df = main.plc_data_process(batch_no)

        
#         # Return as JSON
#         return jsonify({
#             "success": True,
#             "data": df.to_dict(orient="records")
#         })

#     except Exception as e:
#         return jsonify({"success": False, "error": str(e)})


# ===============================================================
# ✅ FETCH PLC DATA FOR POPUP
# ===============================================================
@app.route('/api/plc_data', methods=['POST'])
def api_plc_data():
    try:
        batch_no = request.json.get('batch_no')
        if not batch_no:
            return jsonify({"success": False, "error": "Missing BatchNo"}), 400

        df_pivot, df_string, daily_batch_no = main.report_data_process(batch_no)
        if isinstance(df_pivot, dict) and not df_pivot.get("success", True):
            return jsonify(df_pivot)

        data = df_pivot.to_dict(orient="records")
        return jsonify({"success": True, "data": data, "daily_batch": daily_batch_no})
    except Exception as e:
        print(f"❌ API Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ===============================================================
# ✅ PDF REPORT DOWNLOAD
# ===============================================================
@app.route('/api/plc_data/pdf', methods=['POST'])
def api_plc_data_pdf():
    try:
        batch_no = request.json.get('batch_no')
        if not batch_no:
            return jsonify({"success": False, "error": "BatchNo missing"}), 400

        df_pivot, df_string, daily_batch_no = main.report_data_process(batch_no)
        pdf_bytes = Report.generate_pdf_report(df_pivot, df_string, batch_no)

        return send_file(
            io.BytesIO(pdf_bytes),
            as_attachment=True,
            download_name=f"BatchReport_{batch_no}.pdf",
            mimetype="application/pdf"
        )
    except Exception as e:
        print(f"❌ PDF Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

# ===============================================================
# ✅ EXCEL REPORT DOWNLOAD
# ===============================================================
@app.route('/api/plc_data/excel', methods=['POST'])
def api_plc_data_excel():
    try:
        batch_no = request.json.get('batch_no')
        if not batch_no:
            return jsonify({"success": False, "error": "BatchNo missing"}), 400

        df_pivot, df_string, daily_batch_no = main.report_data_process(batch_no)
        excel_bytes = Report.generate_excel_report(df_pivot, df_string, batch_no)

        return send_file(
            io.BytesIO(excel_bytes),
            as_attachment=True,
            download_name=f"BatchReport_{batch_no}.xlsx",
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )
    except Exception as e:
        print(f"❌ Excel Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    
@app.route('/api/export_data', methods=['POST'])
def api_export_data():
    try:
        payload = request.get_json()
        hours = payload.get('hours')
        from_time = payload.get('from_time')
        to_time = payload.get('to_time')

        print(f"📤 Export requested → Hours: {hours}, From: {from_time}, To: {to_time}")
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()
        
        # 🧠 Use your existing logic or Sqlite.showBatch equivalent
        df = sqliteCon.data_batch(conn, hours, from_time, to_time, engineConRead)

        if df is None or df.empty:
            return jsonify({"success": False, "error": "No data available to export"}), 400

        # ✅ Create Excel in-memory (no file saved on disk)
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='ReportData')
        output.seek(0)  # VERY IMPORTANT — reset file pointer!

        # ✅ Create dynamic file name
        filename = f"Report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        # ✅ Return correct MIME type
        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        print(f"❌ Export Error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500

@app.route('/analytics')
def analytics():
    """
    Loads the Analytics page (UI only)
    """
    return render_template('analytics.html')

@app.route('/settings')
def settings():
    return render_template('settings.html')

@app.route('/stocks')
def stocks():
    return render_template('stocks.html')

# ✅ API Route — returns live data for the Stocks table
@app.route("/api/stocks", methods=["GET"])
def get_stocks_data():
    try:
        # Connect to your SQLite DB
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        # Query your MaterialData table
        query = "SELECT SiloNo, MaterialName, MaterialCode, OperatorName, TotalExtracted FROM MaterialData"
        df = pd.read_sql(query, conn)

        # Assign row index as ID
        df.reset_index(inplace=True)
        df.rename(columns={"index": "Id"}, inplace=True)

        # Convert NaN to empty
        df = df.fillna("")

        # Convert to list of dicts for JSON
        records = df.to_dict(orient="records")

        return jsonify({"success": True, "records": records})
    
    except Exception as e:
        print("❌ Error reading stock data:", e)
        return jsonify({"success": False, "error": str(e)})
    
    finally:
        try:
            conn.close()
        except:
            pass

# Add new stock
@app.route("/api/stocks/add", methods=["POST"])
def add_stock():
    try:
        data = request.get_json()
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        cursorWrite.execute("""
            INSERT INTO MaterialData (SiloNo, MaterialName, MaterialCode, OperatorName)
            VALUES (?, ?, ?, ?)
        """, (data["SiloNo"], data["MaterialName"], data["MaterialCode"], data["OperatorName"]))
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        print("Add Error:", e)
        return jsonify({"success": False, "error": str(e)})

# Update existing stock
@app.route("/api/stocks/update/<string:silono>", methods=["PUT"])
def update_stock(silono):
    try:
        data = request.get_json()
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        cursorWrite.execute("""
            UPDATE MaterialData
            SET MaterialName = ?, MaterialCode = ?, OperatorName = ?
            WHERE SiloNo = ?
        """, (data["MaterialName"], data["MaterialCode"], data["OperatorName"], silono))
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        print("Update Error:", e)
        return jsonify({"success": False, "error": str(e)})

# Delete stock by SiloNo
@app.route("/api/stocks/delete/<string:silono>", methods=["DELETE"])
def delete_stock(silono):
    try:
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        cursorWrite.execute("DELETE FROM MaterialData WHERE SiloNo = ?", (silono,))
        conn.commit()
        return jsonify({"success": True})
    except Exception as e:
        print("Delete Error:", e)
        return jsonify({"success": False, "error": str(e)})

    finally:
        conn.close()


@app.route('/about')
def about():
    return render_template('about.html')

@app.route('/super_admin')
def super_admin():
    user_logged_in = 'username' in session
    df = sqliteCon.dfUser()
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
        conn = sqliteCon.get_db_connection()
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

    conn = sqliteCon.get_db_connection()
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