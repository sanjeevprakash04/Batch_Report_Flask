from flask import Flask, render_template, request, redirect, url_for, session, jsonify, abort, g, send_file, Response
from werkzeug.security import check_password_hash, generate_password_hash
import pandas as pd
from threading import Thread, Timer
from datetime import datetime
import io
import json
import webbrowser
import threading
import plotly
import subprocess

#Modules
from auth import authLog, authMac
from config import sqliteCon
from modules import monitor, main, Report, analytics_module, graphs
app = Flask(__name__)
app.secret_key = '4f3d6e9a5f4b1c8d7e6a2b3c9d0e8f1a5b7c2d4e6f9a1b3c8d0e6f2a9b1d3c4'

def open_browser():
    webbrowser.open("http://127.0.0.1:5000/")

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

@app.route("/api/material/<silo_no>", methods=["GET"])
def get_material_by_silo(silo_no):
    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
    cursorRead.execute("SELECT MaterialName FROM MaterialData WHERE SiloNo = ?", (silo_no,))
    row = cursorRead.fetchone()
    conn.close()
    if row:
        return jsonify({"success": True, "MaterialName": row[0]})
    else:
        return jsonify({"success": False}), 404

@app.route("/api/recipes_data/get_recipes", methods=["GET"])
def get_recipes():
    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
    cursorRead.execute("SELECT id, name FROM recipes ORDER BY id ASC")
    data = [{"id": row[0], "name": row[1]} for row in cursorRead.fetchall()]
    conn.close()
    return jsonify(data)

@app.route("/api/recipes_data/add_recipe", methods=["POST"])
def add_recipe():
    data = request.json
    name = data.get("name")

    if not name or name.strip() == "":
        return jsonify({"success": False, "error": "Recipe name required"}), 400

    name = name.strip()

    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

    # 🔍 Check if recipe already exists
    cursorRead.execute("SELECT COUNT(*) FROM recipes WHERE name = ?", (name,))
    exists = cursorRead.fetchone()[0]

    if exists > 0:
        conn.close()
        return jsonify({"success": False, "error": "Recipe already exists"}), 409

    try:
        # ✅ Insert into recipes table
        cursorWrite.execute(
            "INSERT INTO recipes (name, category) VALUES (?, ?)",
            (name, name)
        )
        conn.commit()

        # ✅ Insert an EMPTY row in recipeData
        cursorWrite.execute("""
            INSERT INTO recipeData (SiloNo, MaterialName, SetWeight, FineWeight, Tolerance, Category)
            VALUES ('', '', '', '', '', ?)
        """, (name,))
        conn.commit()

        return jsonify({"success": True})

    except Exception as e:
        print("❌ Error adding recipe:", e)
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        conn.close()




@app.route("/api/recipes_data/delete_recipe/<string:name>", methods=["DELETE"])
def delete_recipe(name):
    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
    cursorWrite.execute("DELETE FROM recipes WHERE name=?", (name,))
    conn.commit()
    conn.close()
    return jsonify({"success": True})

@app.route("/api/recipes_data/rename_recipe", methods=["PUT"])
def rename_recipe():
    data = request.json
    old = data.get("old_name")
    new = data.get("new_name")

    # Validate
    if not old or not new:
        return jsonify({"success": False, "error": "old_name and new_name required"}), 400

    old = old.strip()
    new = new.strip()

    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

    # 🔍 Check if old recipe exists
    cursorRead.execute("SELECT COUNT(*) FROM recipes WHERE name = ?", (old,))
    old_exists = cursorRead.fetchone()[0]

    if old_exists == 0:
        conn.close()
        return jsonify({"success": False, "error": "Old recipe does not exist"}), 404

    # 🔍 Check if new recipe name already exists
    cursorRead.execute("SELECT COUNT(*) FROM recipes WHERE name = ?", (new,))
    new_exists = cursorRead.fetchone()[0]

    if new_exists > 0:
        conn.close()
        return jsonify({"success": False, "error": "New recipe name already exists"}), 409

    try:
        # Start rename
        cursorWrite.execute("UPDATE recipes SET name=? WHERE name=?", (new, old))
        cursorWrite.execute("UPDATE recipeData SET Category=? WHERE Category=?", (new, old))

        conn.commit()
        return jsonify({"success": True})

    except Exception as e:
        conn.rollback()
        print("❌ Rename error:", e)
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        conn.close()


@app.route("/api/recipes/<string:category>/table", methods=["GET"])
def get_recipe_table(category):
    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
    query = """
        SELECT r."Index", r.SiloNo,
            COALESCE(m.MaterialName, r.MaterialName) AS MaterialName,
            r.SetWeight, r.FineWeight, r.Tolerance
        FROM recipeData r
        LEFT JOIN MaterialData m ON r.SiloNo = m.SiloNo
        WHERE r.Category = ?
        
    """
    # query = """
    #     SELECT "Index", SiloNo, MaterialName, SetWeight, FineWeight, Tolerance
    #     FROM recipeData
    #     WHERE Category = ?
    #     """

    cursorRead.execute(query, (category,))
    data = cursorRead.fetchall()
    cols = [desc[0] for desc in cursorRead.description]
    conn.close()
    return jsonify([dict(zip(cols, row)) for row in data])

@app.route("/api/recipes_data/add_row", methods=["POST"])
def add_row():
    data = request.json
    silo = data.get("SiloNo")
    category = data.get("Category")

    # Validate inputs
    if not silo or not category:
        return jsonify({"success": False, "error": "SiloNo and Category required"}), 400

    silo = str(silo).strip()
    category = category.strip()

    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

    # --------------------------------------------------------------
    # 1️⃣ Check if Silo exists in MaterialData
    # --------------------------------------------------------------
    cursorRead.execute("SELECT MaterialName FROM MaterialData WHERE SiloNo=?", (silo,))
    mrow = cursorRead.fetchone()

    if not mrow:
        conn.close()
        return jsonify({"success": False, "error": "silo_not_found"}), 404

    material_name = mrow[0]

    # --------------------------------------------------------------
    # 2️⃣ Check if SAME SiloNo already exists in recipeData under SAME Category
    # --------------------------------------------------------------
    cursorRead.execute("""
        SELECT COUNT(*) 
        FROM recipeData 
        WHERE SiloNo = ? AND Category = ?
    """, (silo, category))

    exists = cursorRead.fetchone()[0]

    if exists > 0:
        conn.close()
        return jsonify({"success": False, "error": "silo_already_exists"}), 409

    # --------------------------------------------------------------
    # 3️⃣ Insert new recipe row
    # --------------------------------------------------------------
    try:
        cursorWrite.execute("""
            INSERT INTO recipeData (SiloNo, MaterialName, SetWeight, FineWeight, Tolerance, Category)
            VALUES (?, ?, ?, ?, ?, ?)
        """, (
            silo,
            material_name,
            data.get("SetWeight"),
            data.get("FineWeight"),
            data.get("Tolerance"),
            category
        ))

        conn.commit()
        return jsonify({"success": True})

    except Exception as e:
        conn.rollback()
        print("❌ Add Row Error:", e)
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        conn.close()

@app.route('/api/recipes/export', methods=['POST'])
def export_recipe_data():
    try:
        payload = request.get_json()
        category = payload.get("category")

        if not category:
            return jsonify({"success": False, "error": "No category provided"}), 400

        print(f"📤 Export Recipe → {category}")

        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

        query = """
            SELECT 
                r.SiloNo,
                COALESCE(m.MaterialName, r.MaterialName) AS MaterialName,
                r.SetWeight,
                r.FineWeight,
                r.Tolerance
            FROM recipeData r
            LEFT JOIN MaterialData m ON r.SiloNo = m.SiloNo
            WHERE r.Category = ?
        """

        df = pd.read_sql_query(query, conn, params=(category,))

        if df.empty:
            return jsonify({"success": False, "error": "No data found for this recipe"}), 400

        # Create Excel in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name=category[:31])

        output.seek(0)

        filename = f"{category}_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        print("❌ Export Recipe Error:", e)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/recipes/import", methods=["POST"])
def import_recipe_excel():
    try:
        if "file" not in request.files:
            return jsonify({"success": False, "error": "No file uploaded"}), 400

        file = request.files["file"]
        filename = file.filename

        if not filename.endswith(".xlsx"):
            return jsonify({"success": False, "error": "Only .xlsx allowed"}), 400

        # Category = file name without extension
        category = filename.rsplit(".", 1)[0].strip()

        # Load Excel into pandas
        df = pd.read_excel(file)

        required_cols = ["SiloNo", "MaterialName", "SetWeight", "FineWeight", "Tolerance"]

        for col in required_cols:
            if col not in df.columns:
                return jsonify({"success": False, "error": f"Missing column: {col}"}), 400

        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

        # 1️⃣ Check if recipe already exists
        cursorRead.execute("SELECT COUNT(*) FROM recipes WHERE name = ?", (category,))
        if cursorRead.fetchone()[0] > 0:
            return jsonify({"success": False, "error": "Recipe already exists"}), 409

        # 2️⃣ Insert recipe name
        cursorWrite.execute(
            "INSERT INTO recipes (name, category) VALUES (?, ?)",
            (category, category)
        )
        conn.commit()

        # 3️⃣ Insert all rows into recipeData
        for _, row in df.iterrows():
            silo = str(row["SiloNo"]).strip()

            # Validate silo exists in MaterialData
            cursorRead.execute("SELECT MaterialName FROM MaterialData WHERE SiloNo=?", (silo,))
            mr = cursorRead.fetchone()

            if not mr:
                conn.rollback()
                return jsonify({"success": False, "error": f"Silo not found: {silo}"}), 400

            cursorWrite.execute("""
                INSERT INTO recipeData (SiloNo, MaterialName, SetWeight, FineWeight, Tolerance, Category)
                VALUES (?, ?, ?, ?, ?, ?)
            """, (
                silo,
                mr[0],                              # MaterialName from MaterialData
                row["SetWeight"],
                row["FineWeight"],
                row["Tolerance"],
                category
            ))

        conn.commit()
        return jsonify({"success": True})

    except Exception as e:
        print("❌ Import Error:", e)
        return jsonify({"success": False, "error": str(e)}), 500

@app.route("/api/recipes_data/update_row/<int:index>", methods=["PUT"])
def update_row(index):
    data = request.json

    silo = data.get("SiloNo")
    category = data.get("Category")
    set_weight = data.get("SetWeight")
    fine_weight = data.get("FineWeight")
    tolerance = data.get("Tolerance")

    # ------------------------------
    # Validate required fields
    # ------------------------------
    if not silo or not category:
        return jsonify({"success": False, "error": "SiloNo and Category required"}), 400

    silo = str(silo).strip()
    category = category.strip()

    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

    # --------------------------------------------------------------
    # 1️⃣ Check if Silo exists in MaterialData
    # --------------------------------------------------------------
    cursorRead.execute("SELECT MaterialName FROM MaterialData WHERE SiloNo = ?", (silo,))
    mrow = cursorRead.fetchone()

    if not mrow:
        conn.close()
        return jsonify({"success": False, "error": "silo_not_found"}), 404

    material_name = mrow[0]

    # --------------------------------------------------------------
    # 2️⃣ Check that Index exists in recipeData
    # --------------------------------------------------------------
    cursorRead.execute('SELECT COUNT(*) FROM recipeData WHERE "Index"=?', (index,))
    if cursorRead.fetchone()[0] == 0:
        conn.close()
        return jsonify({"success": False, "error": "row_not_found"}), 404

    # --------------------------------------------------------------
    # 3️⃣ Duplicate Silo check (same recipe & category)
    # --------------------------------------------------------------
    cursorRead.execute("""
        SELECT COUNT(*) FROM recipeData
        WHERE SiloNo = ?
          AND Category = ?
          AND "Index" != ?
    """, (silo, category, index))

    if cursorRead.fetchone()[0] > 0:
        conn.close()
        return jsonify({"success": False, "error": "silo_already_exists"}), 409

    # --------------------------------------------------------------
    # 4️⃣ Perform the UPDATE
    # --------------------------------------------------------------
    try:
        cursorWrite.execute("""
            UPDATE recipeData
            SET SiloNo = ?, 
                Category = ?, 
                MaterialName = ?, 
                SetWeight = ?, 
                FineWeight = ?, 
                Tolerance = ?
            WHERE "Index" = ?
        """, (
            silo,
            category,
            material_name,
            set_weight,
            fine_weight,
            tolerance,
            index
        ))

        conn.commit()
        return jsonify({"success": True})

    except Exception as e:
        conn.rollback()
        print("❌ Update row error:", e)
        return jsonify({"success": False, "error": str(e)}), 500

    finally:
        conn.close()


@app.route("/api/recipes_data/delete_row/<int:index>", methods=["DELETE"])
def delete_row(index):
    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

    # STEP 1: Delete selected row
    cursorWrite.execute('DELETE FROM recipeData WHERE "Index"=?', (index,))
    conn.commit()

    # STEP 2: Read all remaining rows ordered by old Index
    cursorRead.execute('SELECT rowid FROM recipeData ORDER BY "Index" ASC')
    rows = cursorRead.fetchall()

    # STEP 3: Reset index from 1...N
    new_index = 1
    for row in rows:
        cursorWrite.execute(
            'UPDATE recipeData SET "Index"=? WHERE rowid=?',
            (new_index, row[0])
        )
        new_index += 1

    conn.commit()
    conn.close()

    return jsonify({"success": True})




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
    return redirect(url_for('analytics_tab', tab='data'))

@app.route("/api/analytics_data", methods=["POST"])
def analytics_data():
    """
    Expects JSON: { "hours": "1 Hr" | "4 Hr" | ... | "Custom",
                   "from_time": "2025-11-01T10:00" (ISOLocal) optional,
                   "to_time": "2025-11-01T12:00" (ISOLocal) optional }
    Returns:
      {
        "success": True,
        "data": [ {Category:..., SetWeight:..., ActualWeight:..., Error_%:..., Error_Kg:...}, ... ],
        "total_weight": <float>  # total Error_Kg sum / 1000 (rounded)
      }
    """
    try:
        payload = request.get_json() or {}
        hours = payload.get("hours", "1 Hr")
        from_time = payload.get("from_time")
        to_time = payload.get("to_time")
        print(f"📥 Analytics Filters → Hours: {hours}, From: {from_time}, To: {to_time}")
        # Basic validation for Custom range
        if hours == "Custom":
            if not from_time or not to_time:
                return jsonify({"success": False, "error": "Custom range requires from_time and to_time"}), 400
            # optional: validate ISO format
            try:
                # This only validates format; your show_data handles DB queries
                datetime.fromisoformat(from_time)
                datetime.fromisoformat(to_time)
            except Exception:
                return jsonify({"success": False, "error": "Invalid from_time/to_time format, use ISO format"}), 400

        # Get DB connections (uses your existing helper)
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()

        # Use your existing show_data to fetch raw rows
        df = sqliteCon.show_data(conn, hours, from_time, to_time, engineConRead)

        if df is None or df.empty:
            return jsonify({"success": True, "data": [], "total_weight": 0.0})

        # Use your existing processing function
        df_diff = sqliteCon.process_batch_data(df)
        if df_diff is None or df_diff.empty:
            return jsonify({"success": True, "data": [], "total_weight": 0.0})

        # Keep only the columns your frontend expects (if present)
        column_order = ["Category", "SetWeight", "ActualWeight", "Error_%", "Error_Kg"]
        existing_columns = [c for c in column_order if c in df_diff.columns]
        df_diff = df_diff[existing_columns]

        # Calculate total (sum Error_Kg -> convert to tons by dividing 1000)
        total_error_kg = df_diff["Error_Kg"].sum() if "Error_Kg" in df_diff.columns else 0
        total_tons = round(total_error_kg / 1000.0, 2)

        # Convert to JSON-serializable structure
        data = df_diff.fillna("").to_dict(orient="records")

        return jsonify({"success": True, "data": data, "total_weight": total_tons})

    except Exception as e:
        # keep message minimal for production; full error helps during development
        return jsonify({"success": False, "error": str(e)}), 500
    
@app.route("/api/export_data_analytics", methods=["POST"])
def export_data_analytics():
    """
    Exports processed analytics data as an Excel file.
    Expects JSON body: { "hours": "1 Hr", "from_time": "...", "to_time": "..." }
    """
    try:
        payload = request.get_json() or {}
        hours = payload.get("hours", "1 Hr")
        from_time = payload.get("from_time")
        to_time = payload.get("to_time")

        # Validation for custom range
        if hours == "Custom" and (not from_time or not to_time):
            return jsonify({"success": False, "error": "Missing from/to times"}), 400

        # DB Connection
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()

        # Fetch raw data
        df = sqliteCon.show_data(conn, hours, from_time, to_time, engineConRead)
        if df is None or df.empty:
            return jsonify({"success": False, "error": "No data available for export"}), 404

        # Process
        df_diff = sqliteCon.process_batch_data(df)
        if df_diff is None or df_diff.empty:
            return jsonify({"success": False, "error": "No processed data to export"}), 404

        # Reorder columns
        column_order = ["Category", "SetWeight", "ActualWeight", "Error_%", "Error_Kg"]
        existing_columns = [col for col in column_order if col in df_diff.columns]
        df_diff = df_diff[existing_columns]

        # Convert to Excel in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine="xlsxwriter") as writer:
            df_diff.to_excel(writer, index=False, sheet_name="Analytics_Report")
            worksheet = writer.sheets["Analytics_Report"]
            # Optional: auto adjust column widths
            for i, col in enumerate(df_diff.columns):
                max_len = max(df_diff[col].astype(str).map(len).max(), len(col))
                worksheet.set_column(i, i, max_len + 3)

        output.seek(0)

        # Generate filename
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"Analytics_Report_{timestamp}.xlsx"

        # Send as file download
        return send_file(
            output,
            mimetype="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            as_attachment=True,
            download_name=filename
        )

    except Exception as e:
        print(f"❌ Export error: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    
@app.route("/api/plc_data_analytics", methods=["POST"])
def plc_data_analytics():
    try:
        data = request.get_json() or {}

        # ✅ Extract parameters safely
        category = data.get("category")  # Category or Silo
        hours = data.get("hours", "1 Hr")
        from_time = data.get("from_time")
        to_time = data.get("to_time")

        # ✅ Validation
        if not category:
            return jsonify({"success": False, "error": "Missing 'category' field"}), 400

        # ✅ Get DB connections
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()

        # ✅ Fetch main PLC data
        df = sqliteCon.show_data(conn, hours, from_time, to_time, engineConRead)
        if df is None or df.empty:
            return jsonify({"success": False, "error": "No PLC data found for the selected range"}), 404

        # ✅ Filter and transform data
        df = df[df["Category"] != "Info"]
        df_pivot = sqliteCon.get_silo_pivot(df, category)
        if df_pivot is None or df_pivot.empty:
            return jsonify({"success": True, "data": []})

        # ✅ Fixed column order
        col_order = [
            "Category", "SetWeight", "ActualWeight", "FineWeight",
            "Error_Kg", "Error_%", "DiffPerc", "DiffKg", "TimeStamp"
        ]
        existing_cols = [c for c in col_order if c in df_pivot.columns]
        df_pivot = df_pivot[existing_cols]

        # ✅ Send cleaned response
        return jsonify({
            "success": True,
            "data": df_pivot.fillna("").to_dict(orient="records")
        })

    except Exception as e:
        print(f"❌ Error in /api/plc_data_analytics: {e}")
        return jsonify({"success": False, "error": str(e)}), 500
    
@app.route("/api/analytics/dash", methods=["GET"])
def analytics_dashboard():
    try:
        print("🚀 Starting Dashboard...")

        # Start Dash in a non-daemon thread
        thread = threading.Thread(target=analytics_module.run_dashboard)
        thread.daemon = False
        thread.start()

        # Detect server/public IP automatically
        server_host = request.host.split(":")[0]

        # Dash always runs on port 8050
        dash_url = f"http://{server_host}:8050"

        return jsonify({"success": True, "url": dash_url})

    except Exception as e:
        print("❌ Dashboard failed:", e)
        return jsonify({"success": False, "error": str(e)})



    
@app.route('/api/analytics/graph/data', methods=['GET', 'POST'])
def get_analytics_graph_data():
    try:
        print("✅ Flask route reached /api/analytics/graph/data")
        # Safe JSON extraction
        data = request.get_json(force=True, silent=True) or {}
        print("📥 Incoming JSON:", data)

        hours = data.get("hours", "1 Hr")
        from_time = data.get("from_time")
        to_time = data.get("to_time")

        print(f"📥 Received parameters → Hours: {hours}, From: {from_time}, To: {to_time}")

        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        engine, engineConRead, engineConWrite = sqliteCon.get_db_connection_engine()

        df = sqliteCon.show_data(conn, hours, from_time, to_time, engineConRead)
        df_diff = sqliteCon.process_batch_data(df)

        if df_diff is None or df_diff.empty:
            return jsonify({"error": "No data found"})

        total_error_kg = round(df_diff["Error_Kg"].sum(), 2)
        total_error_per = round(df_diff["Error_%"].mean(), 2)

        return jsonify({
            "data": df_diff.to_dict(orient="records"),
            "total_error_kg": total_error_kg,
            "total_error_per": total_error_per
        })
    except Exception as e:
        print("❌ Graph API error:", e)
        return jsonify({"error": str(e)})


@app.route('/analytics/<tab>')
def analytics_tab(tab):
    user_logged_in = 'username' in session
    if tab == 'graph':
        return render_template('analyticsgraph.html', tab='graph', user_logged_in=user_logged_in)
    return render_template('analyticsdata.html', tab='data', user_logged_in=user_logged_in)

@app.route('/settings')
def settings():
    user_logged_in = 'username' in session


    return render_template(
        "settings.html",
        user_logged_in=user_logged_in
    )


def is_admin():
    return session.get("role") in ["admin", "superadmin"]

@app.route('/stocks')
def stocks():
    return render_template('stocks.html')

# ✅ API Route — returns live data for the Stocks table
@app.route("/api/stocks", methods=["GET"])
def get_stocks_data():
    try:
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

        query = """
            SELECT SiloNo, MaterialName, MaterialCode, OperatorName, TotalExtracted
            FROM MaterialData
        """
        df = pd.read_sql(query, conn)

        # REMOVE rows where SiloNo is NULL, empty string, or '-'
        df = df[df["SiloNo"].notna()]              # drop NULL
        df = df[df["SiloNo"].astype(str).str.strip() != ""]   # drop empty string
        df = df[df["SiloNo"].astype(str).str.strip() != "-"]  # drop '-'

        # Convert SiloNo to integer safely
        df["SiloNo"] = pd.to_numeric(df["SiloNo"], errors="coerce")
        df = df.dropna(subset=["SiloNo"])  # drop rows that still could not convert
        df["SiloNo"] = df["SiloNo"].astype(int)

        # Fill remaining NaN values
        df = df.fillna("")

        # Add unique id
        df.reset_index(drop=True, inplace=True)
        df.insert(0, "Id", df.index + 1)

        # Sort descending
        df_sorted = df.sort_values(by="SiloNo", ascending=True)

        # Convert to records
        return jsonify({
            "success": True,
            "records": df_sorted.to_dict(orient="records")
        })

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
        silono = data["SiloNo"]

        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

        # ❗ Check if SiloNo already exists
        cursorRead.execute("SELECT 1 FROM MaterialData WHERE SiloNo = ?", (silono,))
        exists = cursorRead.fetchone()

        if exists:
            return jsonify({
                "success": False,
                "error": f"SiloNo {silono} already exists. Please use another."
            })

        # Insert new row
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
        new_silono = data.get("SiloNo", silono)

        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

        # ❗ If SiloNo is being changed, ensure it is unique
        if new_silono:
            cursorRead.execute("SELECT 1 FROM MaterialData WHERE SiloNo = ?", (new_silono,))
            exists = cursorRead.fetchone()

            if exists:
                return jsonify({
                    "success": False,
                    "error": f"SiloNo {new_silono} already exists. Please use another."
                })

        # Update record
        cursorWrite.execute("""
            UPDATE MaterialData
            SET SiloNo = ?, MaterialName = ?, MaterialCode = ?, OperatorName = ?
            WHERE SiloNo = ?
        """, (new_silono, data["MaterialName"], data["MaterialCode"], data["OperatorName"], silono))

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

@app.route('/api/stocks/export', methods=['POST'])
def export_material_data():
    try:
        print("📤 MaterialData Excel Export Requested")

        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()

        # Read table into pandas
        query = """
            SELECT SiloNo, MaterialName, MaterialCode, OperatorName, TotalExtracted
            FROM MaterialData
        """
        df = pd.read_sql_query(query, conn)

        if df is None or df.empty:
            return jsonify({"success": False, "error": "No data available to export"}), 400

        # Prepare Excel in memory
        output = io.BytesIO()
        with pd.ExcelWriter(output, engine='xlsxwriter') as writer:
            df.to_excel(writer, index=False, sheet_name='MaterialData')

        output.seek(0)

        filename = f"MaterialData_{datetime.now().strftime('%Y%m%d_%H%M%S')}.xlsx"

        return send_file(
            output,
            as_attachment=True,
            download_name=filename,
            mimetype='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
        )

    except Exception as e:
        print("❌ Export Error:", e)
        return jsonify({"success": False, "error": str(e)}), 500


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
        conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
        
        hashed = generate_password_hash(new_password)

        cursorWrite.execute("UPDATE users SET password_hash = ? WHERE user_name = ?", (hashed, username))
        conn.commit()
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

    conn, cursorRead, cursorWrite = sqliteCon.get_db_connection()
    if conn is None:
        return jsonify(success=False, error="Database connection error"), 500

    try:

        # Fetch the stored password hash
        cursorWrite.execute('SELECT password_hash FROM users WHERE user_name = ?', (username,))
        row = cursorWrite.fetchone()

        if not row:
            return jsonify(success=False, error="User not found"), 404

        stored_password_hash = row[0]

        if check_password_hash(stored_password_hash, old_password):
            # If old password matches, update with new password
            new_password_hash = generate_password_hash(new_password)

            cursorWrite.execute('UPDATE users SET password_hash = ? WHERE user_name = ?', (new_password_hash, username))
            conn.commit()

            return jsonify(success=True)
        else:
            return jsonify(success=False, error="Old password is incorrect."), 400

    except Exception as e:
        print("Error during password change:", e)
        return jsonify(success=False, error=str(e)), 500

    finally:
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
    # Timer(1, open_browser).start()
    app.run()