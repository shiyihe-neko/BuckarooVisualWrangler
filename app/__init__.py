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
# from data_management.data_integration import *


# Function to create the database if it does not exist
# This function checks if the database exists and creates it if it does not
def create_database_if_not_exists(conn, db_name):
    conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
    cur = conn.cursor()

    # Check if the database exists
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
            host = db_info["host"]
            port = db_info["port"]
            user = db_info["user"]
            password = db_info["password"]
            db_name = db_info["db_name"]
            return host, port, user, password, db_name
    else:
        print("database.json file not found, using default connection parameters.")
        print("Enter the host of the database (default: localhost): ")
        host = input() or "localhost"
        print("Enter the port of the database (default: 5432): ")
        port = input() or 5432
        print("Enter the user of the database (default: postgres): ")
        user = input() or "postgres"
        print("Enter the password of the database: ")
        password = input()
        print("Enter the name of the database (default: buckaroo_db): ")
        db_name = input() or "buckaroo_db"
        with open(json_path, "w") as f:
            db_info = {
                "host": host,
                "port": port,
                "user": user,
                "password": password,
                "db_name": db_name
            }
            f.write(json.dumps(db_info, indent=4))    

    return host, port, user, password, db_name


# load the .env file and read the different variables in there and them in the environment variables for this process
load_dotenv()

app = Flask(__name__)

# Check for Render's DATABASE_URL environment variable
render_db_url = os.environ.get('DATABASE_URL')

if render_db_url:
    # --- Production Environment (Render) ---
    print("Using Render Database URL...")
    
    # Fix for SQLAlchemy compatibility (Render provides postgres:// but SQLAlchemy requires postgresql://)
    if render_db_url.startswith("postgres://"):
        render_db_url = render_db_url.replace("postgres://", "postgresql://", 1)
        
    # Create engine directly using the provided URL
    engine = create_engine(render_db_url)

else:
    # --- Local Environment ---
    print("Using Local Database Configuration...")
    host, port, user, password, db_name = load_database_info()

    # Connect to postgres default DB to check/create the target database
    try:
        connection = psycopg2.connect(host=host, port=port, user=user, password=password)
        create_database_if_not_exists(connection, db_name)
        connection.close() 
    except Exception as e:
        print(f"Warning: Could not check/create database: {e}")

    # engine to use pandas with the db
    engine = create_engine(f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db_name}")


# manages the different data instances of the data during the users session
data_state_manager = DataState()

# Initialize PostgreSQL stored procedures for histogram generation with errors
from app.db_functions import initialize_database_functions
try:
    initialize_database_functions(engine)
except Exception as e:
    print(f"Warning: Could not initialize DB functions (Tables might not exist yet): {e}")

from app import routes
from app import wrangler_routes_sql as wrangler_routes
from app import plot_routes