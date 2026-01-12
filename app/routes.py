# Buckaroo Project - Final Stable Version
import numpy as np
import pandas as pd
from flask import request, render_template, send_from_directory, Response
import os
import time
from app import app
from app import connection, engine
from app.service_helpers import clean_table_name, get_whole_table_query, run_detectors, create_error_dict, \
    init_session_data_state, fetch_detected_and_undetected_current_dataset_from_db, calculate_attribute_rankings
from app import data_state_manager
from app.set_id_column import set_id_column
import json
from sqlalchemy import inspect, text

# --- 核心优化：大块慢速写入 (减少 SSL 握手次数) ---
def safe_write_to_db_with_sleep(df, table_name, engine, chunk_size=2000):
    """
    策略调整：
    增大 Chunk (2000) = 减少网络请求次数 = 减少 SSL 报错概率
    增加 Sleep (1.0s) = 给数据库充足的 I/O 缓冲时间
    """
    total_rows = len(df)
    print(f"🚀 Starting SAFE WRITE for {table_name}: {total_rows} rows...")
    
    # 获取迭代器，不一次性加载到内存
    # 第一次写入 (Replace)
    try:
        first_chunk = df.iloc[0:chunk_size]
        use_index = "rankings" not in table_name
        first_chunk.to_sql(table_name, engine, if_exists='replace', index=use_index)
        print(f"   Written first {chunk_size} rows...")
        time.sleep(1) # 休息 1 秒
        
        # 后续写入 (Append)
        for i in range(chunk_size, total_rows, chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            chunk.to_sql(table_name, engine, if_exists='append', index=use_index)
            print(f"   Written chunk starting at {i}...")
            time.sleep(1) # 每次写完休息 1 秒
            
    except Exception as e:
        print(f"❌ Write failed for {table_name}: {e}")
        raise e # 抛出异常以便外层捕获

    print(f"🎉 Finished writing {table_name}!")

# --- 自动加载与修复逻辑 ---
def initialize_dataset_if_needed(cleaned_table_name, original_filename):
    inspector = inspect(engine)
    has_main = inspector.has_table(cleaned_table_name)
    has_error = inspector.has_table("errors" + cleaned_table_name)

    # 只要有任何一张表缺失，或者处于不一致状态，就重新加载
    if not has_main or not has_error:
        print(f"⚠️ Data mismatch for {cleaned_table_name}. Starting clean reload...")
        
        # 1. 先强制清理环境 (解决 relation does not exist 问题)
        try:
            with engine.connect() as conn:
                trans = conn.begin()
                conn.execute(text(f'DROP TABLE IF EXISTS "{cleaned_table_name}" CASCADE;'))
                conn.execute(text(f'DROP TABLE IF EXISTS "errors{cleaned_table_name}" CASCADE;'))
                conn.execute(text(f'DROP TABLE IF EXISTS "rankings{cleaned_table_name}" CASCADE;'))
                trans.commit()
            print("   Cleaned up old tables.")
        except Exception as e:
            print(f"   Cleanup warning: {e}")

        # 2. 寻找文件
        base_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), '..'))
        paths = [
            os.path.join(base_dir, 'provided_datasets', original_filename),
            os.path.join(base_dir, 'app', 'static', 'provided_datasets', original_filename)
        ]
        csv_path = next((p for p in paths if os.path.exists(p)), None)
             
        if csv_path:
            try:
                print(f"📖 Reading CSV: {original_filename}")
                df = pd.read_csv(csv_path)
                
                print("🔍 Running detectors...")
                df_with_id = set_id_column(df)
                detected_data = run_detectors(df)
                
                # 3. 执行安全写入
                safe_write_to_db_with_sleep(df_with_id, cleaned_table_name, engine)
                safe_write_to_db_with_sleep(detected_data, "errors" + cleaned_table_name, engine)
                
                rankings = calculate_attribute_rankings(detected_data)
                rankings.to_sql("rankings" + cleaned_table_name, engine, if_exists='replace', index=False)
                
                print(f"✅ Successfully loaded {cleaned_table_name}")
            except Exception as e:
                print(f"❌ Failed to auto-load dataset: {e}")
                # 再次清理，防止留下半成品
                with engine.connect() as conn:
                    conn.execute(text(f'DROP TABLE IF EXISTS "{cleaned_table_name}" CASCADE;'))
        else:
            print(f"❌ CSV file not found: {original_filename}")

# --- 路由定义 ---

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

        safe_write_to_db_with_sleep(table_with_id_added, cleaned_table_name, engine)
        safe_write_to_db_with_sleep(detected_data, "errors"+cleaned_table_name, engine)
        
        rankings = calculate_attribute_rankings(detected_data)
        rankings.to_sql("rankings"+cleaned_table_name, engine, if_exists='replace', index=False)

        return{"success": True, "rows for undetected data": len(table_with_id_added), "clean_table_name": cleaned_table_name}
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
        return Response(csv, mimetype="text/csv", headers={"Content-disposition": f"attachment; filename={clean_name}_cleaned.csv"})
    except Exception as e:
        return str(e), 500

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