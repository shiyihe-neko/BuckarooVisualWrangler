# Buckaroo Project - Final Stable Version (Memory Optimized)
import numpy as np
import pandas as pd
from flask import request, render_template, send_from_directory, Response
import os
import time
import gc
from app import app
from app import connection, engine
from app.service_helpers import clean_table_name, get_whole_table_query, run_detectors, create_error_dict, \
    init_session_data_state, fetch_detected_and_undetected_current_dataset_from_db, calculate_attribute_rankings
from app import data_state_manager
from app.set_id_column import set_id_column
import json
from sqlalchemy import inspect, text

# --- CORE OPTIMIZATION: Safe Chunked Write ---
def safe_write_to_db_with_sleep(df, table_name, engine, chunk_size=2000):
    """
    Strategy:
    Larger Chunk (2000) = Fewer network requests = Fewer SSL handshake errors
    Sleep (1.0s) = Give the database I/O buffer time to clear
    """
    total_rows = len(df)
    print(f"[START] Safe write for {table_name}: {total_rows} rows...")
    
    try:
        # Write first chunk (Replace mode to create table)
        first_chunk = df.iloc[0:chunk_size]
        use_index = "rankings" not in table_name
        first_chunk.to_sql(table_name, engine, if_exists='replace', index=use_index)
        print(f"   [WRITE] Initialized table with first {len(first_chunk)} rows...")
        time.sleep(1) # Sleep to prevent connection reset
        
        # Write remaining chunks (Append mode)
        for i in range(chunk_size, total_rows, chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            chunk.to_sql(table_name, engine, if_exists='append', index=use_index)
            print(f"   [WRITE] Chunk starting at row {i}...")
            time.sleep(1) 
            
    except Exception as e:
        print(f"[ERROR] Write failed for {table_name}: {e}")
        raise e 

    print(f"[SUCCESS] Finished writing {table_name}!")

# --- Auto-Load and Self-Healing Logic (With Memory Cleanup) ---
def initialize_dataset_if_needed(cleaned_table_name, original_filename):
    inspector = inspect(engine)
    has_main = inspector.has_table(cleaned_table_name)
    has_error = inspector.has_table("errors" + cleaned_table_name)

    # If any table is missing, force a clean reload
    if not has_main or not has_error:
        print(f"[WARN] Data mismatch for {cleaned_table_name}. Starting clean reload...")
        
        # 1. Force cleanup old tables
        try:
            with engine.connect() as conn:
                trans = conn.begin()
                conn.execute(text(f'DROP TABLE IF EXISTS "{cleaned_table_name}" CASCADE;'))
                conn.execute(text(f'DROP TABLE IF EXISTS "errors{cleaned_table_name}" CASCADE;'))
                conn.execute(text(f'DROP TABLE IF EXISTS "rankings{cleaned_table_name}" CASCADE;'))
                trans.commit()
            print("   [CLEANUP] Old tables dropped.")
        except Exception as e:
            print(f"   [CLEANUP ERROR] {e}")

        # 2. Locate CSV file
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        paths = [
            os.path.join(base_dir, 'provided_datasets', original_filename),
            os.path.join(base_dir, 'app', 'static', 'provided_datasets', original_filename)
        ]
        csv_path = next((p for p in paths if os.path.exists(p)), None)
             
        if csv_path:
            try:
                print(f"[READ] Reading CSV: {original_filename}")
                df = pd.read_csv(csv_path)
                
                print("[PROCESS] Running detectors...")
                df_with_id = set_id_column(df)
                detected_data = run_detectors(df)
                
                # --- MEMORY CLEANUP 1: Remove raw df ---
                del df
                gc.collect()
                print("[GC] Raw dataframe cleaned.")
                
                # 3. Execute Safe Write
                safe_write_to_db_with_sleep(df_with_id, cleaned_table_name, engine)
                
                # --- MEMORY CLEANUP 2: Remove id dataframe ---
                del df_with_id
                gc.collect()
                print("[GC] ID dataframe cleaned.")
                
                safe_write_to_db_with_sleep(detected_data, "errors" + cleaned_table_name, engine)
                
                rankings = calculate_attribute_rankings(detected_data)
                rankings.to_sql("rankings" + cleaned_table_name, engine, if_exists='replace', index=False)
                
                # --- MEMORY CLEANUP 3: Final cleanup ---
                del detected_data
                del rankings
                gc.collect()
                print("[GC] Final cleanup done.")
                
                print(f"[DONE] Successfully loaded {cleaned_table_name}")
            except Exception as e:
                print(f"[FAIL] Failed to auto-load dataset: {e}")
                # Cleanup partial data
                with engine.connect() as conn:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{cleaned_table_name}" CASCADE;'))
        else:
            print(f"[ERROR] CSV file not found: {original_filename}")

# --- API Routes ---

@app.post("/api/upload")
def upload_csv():
    try:
        csv_file = request.files['file']
        dataframe = pd.read_csv(csv_file)
        table_with_id_added = set_id_column(dataframe)
        
        start_time = time.time()
        detected_data = run_detectors(dataframe)
        time_to_detect = time.time() - start_time
        
        cleaned_table_name = clean_table_name(csv_file.filename)
        if not os.path.exists("report"): os.makedirs("report")
        json.dump({'db': cleaned_table_name, "clean_time": time_to_detect, "dataframe_shape": list(detected_data.shape)}, open(f"report/{cleaned_table_name}.json", "w"))

        # --- MEMORY CLEANUP: Upload flow ---
        del dataframe
        gc.collect()

        # Use safe write for uploads too
        safe_write_to_db_with_sleep(table_with_id_added, cleaned_table_name, engine)
        del table_with_id_added
        gc.collect()

        safe_write_to_db_with_sleep(detected_data, "errors"+cleaned_table_name, engine)
        
        rankings = calculate_attribute_rankings(detected_data)
        rankings.to_sql("rankings"+cleaned_table_name, engine, if_exists='replace', index=False)
        
        del detected_data
        del rankings
        gc.collect()

        return{"success": True, "rows for undetected data": "inserted", "clean_table_name": cleaned_table_name}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/get-sample")
def get_sample():
    filename = request.args.get("filename")
    data_size = request.args.get("datasize")
    cleaned_table_name = clean_table_name(filename)
    if not filename: return {"success": False, "error": "Filename required"}
    
    try:
        initialize_dataset_if_needed(cleaned_table_name, filename)
    except Exception as e:
        print(f"Init Error: {e}")

    QUERY = get_whole_table_query(cleaned_table_name,False) + " LIMIT "+ data_size
    try:
        fetch_detected_and_undetected_current_dataset_from_db(cleaned_table_name,engine)
        return pd.read_sql_query(QUERY, engine).replace(np.nan, None).to_dict(orient="records")
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.get("/api/get-errors")
def get_errors():
    filename = request.args.get("filename")
    data_size = request.args.get("datasize")
    cleaned_table_name = clean_table_name(filename)
    if not filename: return {"success": False, "error": "Filename required"}
    
    try:
        initialize_dataset_if_needed(cleaned_table_name, filename)
    except: pass

    query = get_whole_table_query(cleaned_table_name,True)
    try:
        full_error_df = pd.read_sql_query(query, engine)
        return create_error_dict(full_error_df, int(data_size))
    except Exception as e:
        return {"success": False, "error": str(e)}

# --- ADMIN ROUTES ---

@app.route('/admin')
def admin_panel():
    return render_template('admin.html')

@app.route('/api/admin/reset_dataset')
def reset_dataset():
    filename = request.args.get('filename')
    if not filename: return "Filename required", 400
    cleaned_name = clean_table_name(filename)
    
    try:
        with engine.connect() as conn:
            trans = conn.begin()
            for table in [cleaned_name, "errors"+cleaned_name, "rankings"+cleaned_name]:
                conn.execute(text(f'DROP TABLE IF EXISTS "{table}" CASCADE;'))
            trans.commit()
        
        # Trigger explicit GC after a reset operation
        gc.collect()
        
        return {"success": True, "message": f"Dataset {cleaned_name} reset."}
    except Exception as e:
        return {"success": False, "error": str(e)}

@app.route('/api/admin/download_table')
def download_table():
    table_name = request.args.get('table')
    if not table_name: return "Table name required", 400
    clean_name = clean_table_name(table_name)
    try:
        df = pd.read_sql_table(clean_name, engine)
        csv = df.to_csv(index=False)
        
        # Cleanup
        del df
        gc.collect()
        
        return Response(csv, mimetype="text/csv", headers={"Content-disposition": f"attachment; filename={clean_name}_cleaned.csv"})
    except Exception as e:
        return str(e), 500

@app.route('/api/admin/download_script')
def download_script_file():
    """Admin: Download a helper Python script to load the CSV"""
    filename = request.args.get('filename')
    if not filename: return "Filename required", 400
    
    clean_name = clean_table_name(filename)
    csv_filename = f"{clean_name}_cleaned.csv"
    
    script_content = f'''"""
Buckaroo Data Cleaning Export
Dataset: {filename}
Date: {time.strftime("%Y-%m-%d %H:%M:%S")}

Instructions:
1. Ensure the file "{csv_filename}" is in the same folder as this script.
2. Run this script to load the cleaned data.
"""

import pandas as pd
import os

def load_cleaned_data():
    csv_file = "{csv_filename}"
    
    if not os.path.exists(csv_file):
        print(f"Error: Could not find {{csv_file}}")
        print("Please download the CSV from the Admin Console and place it here.")
        return None
        
    print(f"Loading {{csv_file}}...")
    try:
        # Load the dataset
        df = pd.read_csv(csv_file)
        
        # Display info
        print("-" * 30)
        print(f"Successfully loaded {{len(df)}} rows.")
        print("-" * 30)
        print("Data Columns:")
        print(df.columns.tolist())
        print("-" * 30)
        
        return df
    except Exception as e:
        print(f"Error loading data: {{e}}")
        return None

if __name__ == "__main__":
    # Load the dataframe
    df = load_cleaned_data()
    
    if df is not None:
        # You can start your analysis here
        print("Dataframe is ready for analysis!")
        print(df.head())
'''
    
    return Response(
        script_content,
        mimetype="text/x-python",
        headers={"Content-disposition": f"attachment; filename=load_{clean_name}.py"}
    )

# --- Standard Routes ---
@app.get("/")
def home(): return render_template('index.html')

@app.get('/data_cleaning_vis_tool')
def data_cleaning_vis_tool(): return render_template('data_cleaning_vis_tool.html')

@app.route('/tool')
def tool(): return render_template('data_cleaning_vis_tool.html')

# --- Static Files ---
BASE_DIR = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
@app.route('/detectors/<path:filename>')
def serve_detectors(filename): return send_from_directory(os.path.join(BASE_DIR, 'detectors'), filename)
@app.route('/wranglers/<path:filename>')
def serve_wranglers(filename): return send_from_directory(os.path.join(BASE_DIR, 'wranglers'), filename)
@app.route('/provided_datasets/<path:filename>')
def serve_datasets(filename): return send_from_directory(os.path.join(BASE_DIR, 'provided_datasets'), filename)
@app.route('/<filename>.json')
def serve_root_json(filename): return send_from_directory(BASE_DIR, f"{filename}.json")