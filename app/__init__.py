# Buckaroo Project - June 1, 2025
# This file allows the app to use packages for maintainability

# make it able to read the variables from the .env file
import os
import psycopg2
from dotenv import load_dotenv
from flask import Flask
from sqlalchemy import create_engine
import json

from data_management.data_state import DataState
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT

# Function to create the database if it does not exist
def create_database_if_not_exists(conn, db_name):
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()
    cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (db_name,))
    exists = cur.fetchone()
    if not exists:
        cur.execute(f"CREATE DATABASE {db_name}")
    cur.close()

# Function to load database connection information from a JSON file or prompt the user
def load_database_info():
    basepath = os.path.dirname(os.path.abspath(__file__))
    json_path = os.path.join(basepath, "database.json")

    if os.path.exists(json_path):
        with open(json_path, "r") as f:
            db_info = json.loads(f.read())
            return db_info["host"], db_info["port"], db_info["user"], db_info["password"], db_info["db_name"]
    else:
        # Local fallback only
        print("database.json file not found, using default connection parameters.")
        host = input("Enter host (default: localhost): ") or "localhost"
        port = input("Enter port (default: 5432): ") or 5432
        user = input("Enter user (default: postgres): ") or "postgres"
        password = input("Enter password: ")
        db_name = input("Enter db_name (default: buckaroo_db): ") or "buckaroo_db"
        
        with open(json_path, "w") as f:
            json.dump({
                "host": host, "port": port, "user": user, 
                "password": password, "db_name": db_name
            }, f, indent=4)    

    return host, port, user, password, db_name


load_dotenv()
app = Flask(__name__)

# --- DATABASE SETUP (Fixed) ---

render_db_url = os.environ.get('DATABASE_URL')
connection = None # Initialize variable

if render_db_url:
    # === Production Environment (Render) ===
    print("Using Render Database URL...")
    
    # Fix for SQLAlchemy compatibility (postgres:// -> postgresql://)
    if render_db_url.startswith("postgres://"):
        sqlalchemy_url = render_db_url.replace("postgres://", "postgresql://", 1)
    else:
        sqlalchemy_url = render_db_url
        
    # 1. Create SQLAlchemy Engine
    engine = create_engine(sqlalchemy_url)
    
    # 2. Create Raw Connection (CRITICAL: routes.py needs this)
    # We use the original URL for psycopg2
    connection = psycopg2.connect(render_db_url)

else:
    # === Local Environment ===
    print("Using Local Database Configuration...")
    host, port, user, password, db_name = load_database_info()

    # 1. Connect to server to check/create DB
    # Note: We connect to the server (default DB) first
    try:
        temp_conn = psycopg2.connect(host=host, port=port, user=user, password=password)
        create_database_if_not_exists(temp_conn, db_name)
        temp_conn.close()
    except Exception as e:
        print(f"Warning: Could not check database existence: {e}")

    # 2. Create Raw Connection (The global one used by routes)
    # Connect specifically to the database now
    connection = psycopg2.connect(host=host, port=port, user=user, password=password, dbname=db_name)

    # 3. Create SQLAlchemy Engine
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")


# Initialize Data State
data_state_manager = DataState()

# Initialize DB Functions
from app.db_functions import initialize_database_functions
try:
    initialize_database_functions(engine)
except Exception as e:
    print(f"Warning: DB functions init failed: {e}")

# Import Routes (Must be at the end to avoid circular import issues, 
# but AFTER 'connection' and 'engine' are defined)
from app import routes
from app import wrangler_routes_sql as wrangler_routes
from app import plot_routes