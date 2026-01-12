# Buckaroo Project - Final Stable Version
# This file handles all endpoints surrounding wranglers

from flask import request
from app import app
from app import engine
from postgres_wrangling import query
import traceback
import pandas as pd
from pprint import pprint
from app.service_helpers import run_detectors
import time
import gc
from sqlalchemy import text

# --- GLOBAL ACTION HISTORY LOG ---
# This stores the python code strings for user actions
ACTION_HISTORY = [
    "# Buckaroo Auto-Generated Action Script",
    "import pandas as pd",
    "import numpy as np",
    "",
    "# 1. Load your dataset",
    "# df = pd.read_csv('your_dataset.csv')",
    "# df['ID'] = range(1, len(df) + 1) # Ensure ID column exists",
    "",
    "# --- User Actions Start Below ---"
]

def record_action(comment, code=None):
    """Helper to append actions to the log"""
    timestamp = time.strftime("%H:%M:%S")
    ACTION_HISTORY.append(f"\n# [{timestamp}] {comment}")
    if code:
        ACTION_HISTORY.append(code)

# --- CORE OPTIMIZATION: Safe Chunked Write (Copied from routes for local use) ---
def safe_write_to_db_with_sleep(df, table_name, engine, chunk_size=2000):
    total_rows = len(df)
    print(f"[WRANGLER] Safe write for {table_name}: {total_rows} rows...")
    
    try:
        # First chunk (Replace)
        first_chunk = df.iloc[0:chunk_size]
        first_chunk.to_sql(table_name, engine, if_exists='replace', index=False)
        time.sleep(1) 
        
        # Remaining chunks (Append)
        for i in range(chunk_size, total_rows, chunk_size):
            chunk = df.iloc[i : i + chunk_size]
            chunk.to_sql(table_name, engine, if_exists='append', index=False)
            time.sleep(1) 
            
    except Exception as e:
        print(f"[WRANGLER ERROR] Write failed: {e}")
        raise e 

# ─────────────────────────────────────────────────────────────────────────────
# Helper: Re-run error detection after modification
# ─────────────────────────────────────────────────────────────────────────────

def update_errors_table(table_name: str) -> None:
    """
    After modifying a table in-place, re-run error detection
    and update the errors table using SAFE WRITE + GC.
    """
    try:
        # 1. Read updated table
        print(f"[WRANGLER] Re-reading table {table_name}...")
        df = pd.read_sql_query(f'SELECT * FROM "{table_name}"', engine)
        
        # 2. Run detectors
        print("[WRANGLER] Re-running detectors...")
        detected_errors_df = run_detectors(df)
        
        # GC Cleanup 1
        del df
        gc.collect()

        # 3. Safe Write Errors
        errors_table_name = f"errors{table_name}"
        safe_write_to_db_with_sleep(detected_errors_df, errors_table_name, engine)
        
        # GC Cleanup 2
        del detected_errors_df
        gc.collect()
        
        print(f"✓ Updated errors table: {errors_table_name}")
    except Exception as e:
        print(f"Warning: Could not update errors table for {table_name}: {e}")
        traceback.print_exc()

# ─────────────────────────────────────────────────────────────────────────────
# Wrangling Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@app.post("/api/wrangle/remove")
def wrangle_remove():
    """
    Remove rows in-place. Handles both bin-based (histogram) and ID-based (scatterplot) selections.
    """
    try:
        body = request.get_json(force=True)
        currentSelection = body["currentSelection"]
        cols = body["cols"]
        table = body["table"]

        print(f"[WRANGLER] Remove request for {table}")

        # Detect selection type
        first_item = currentSelection["data"][0]
        action_code = ""
        action_comment = ""

        if "bin" in first_item and "xBin" not in first_item:
            # 1D histogram
            remaining_rows = query.remove_flagged_rows_in_1d_bin(currentSelection, cols[0], table)
            action_comment = f"Removed rows based on Histogram selection in column '{cols[0]}'"
            action_code = f"# Logic: Remove rows where {cols[0]} is in selected bin range"

        elif "xBin" in first_item and "yBin" in first_item:
            # 2D heatmap
            remaining_rows = query.remove_flagged_rows_in_bin(currentSelection, cols, table)
            action_comment = f"Removed rows based on Heatmap selection in columns {cols}"
            action_code = f"# Logic: Remove rows where {cols} fall in selected 2D bin"

        else:
            # ID-based (scatterplot)
            ids = [point["ID"] for point in currentSelection["data"]]
            remaining_rows = query.remove_rows_by_ids(table=table, ids=ids)
            
            # Record exact Python code for ID removal
            action_comment = f"Removed {len(ids)} specific rows selected from Scatterplot"
            action_code = f"ids_to_remove = {ids}\ndf = df[~df['ID'].isin(ids_to_remove)]"

        # Log the action
        record_action(action_comment, action_code)

        # Re-run error detection (Using Safe Write)
        update_errors_table(table)

        return {
            "success": True,
            "remaining_rows": remaining_rows
        }
    except Exception as e:
        print("ERROR OCCURRED")
        print(traceback.format_exc())
        return {"success": False, "error": str(e)}, 400


@app.post("/api/wrangle/impute")
def wrangle_impute():
    """
    Impute missing values in-place.
    """
    try:
        body = request.get_json(force=True)
        currentSelection = body["currentSelection"]
        cols = body["cols"]
        table = body["table"]
        col = body.get("col") 

        print(f"[WRANGLER] Impute request for {table}")

        # Detect selection type
        first_item = currentSelection["data"][0]
        action_code = ""
        action_comment = ""

        if "bin" in first_item and "xBin" not in first_item:
            # 1D histogram
            rows_examined, cells_imputed = query.impute_1d_bin_in_place(currentSelection, cols[0], table)
            action_comment = f"Imputed Mean/Mode for column '{cols[0]}' (Histogram selection)"
            action_code = f"# Logic: Fill NA in '{cols[0]}' with mean/mode for selected bin"

        elif "xBin" in first_item and "yBin" in first_item:
            # 2D heatmap
            rows_examined, cells_imputed = query.impute_bin_in_place(currentSelection, cols, table)
            action_comment = f"Imputed Mean/Mode for columns {cols} (Heatmap selection)"
            action_code = f"# Logic: Fill NA in {cols} for selected 2D bin"

        else:
            # ID-based
            if not col:
                return {"success": False, "error": "Column required"}, 400
            ids = [point["ID"] for point in currentSelection["data"]]
            rows_examined, cells_imputed = query.impute_by_ids(table=table, col=col, ids=ids)
            
            # Record exact Python code
            action_comment = f"Imputed column '{col}' for {len(ids)} selected IDs"
            action_code = f"# Note: Imputation uses database-calculated mean/mode\nids_to_impute = {ids}\n# df.loc[df['ID'].isin(ids_to_impute), '{col}'] = df['{col}'].mean() # Example logic"

        # Log the action
        record_action(action_comment, action_code)

        # Re-run error detection (Using Safe Write)
        update_errors_table(table)

        return {
            "success": True,
            "rows_examined": rows_examined,
            "cells_imputed": cells_imputed
        }
    except Exception as e:
        print("ERROR OCCURRED")
        print(traceback.format_exc())
        return {"success": False, "error": str(e)}, 400