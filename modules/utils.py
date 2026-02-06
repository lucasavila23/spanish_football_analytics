import pandas as pd
import os
import sys
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

def get_db_connection():
    """
    Establishes a connection to the database securely.
    """
    user = os.getenv("DB_USER")
    password = os.getenv("DB_PASSWORD")
    host = os.getenv("DB_HOST", "localhost")
    dbname = os.getenv("DB_NAME", "spanish_football")

    if not password:
        print(" CRITICAL ERROR: DB_PASSWORD not found in .env file.")
        sys.exit(1)

    # Return the engine
    uri = f"postgresql+psycopg2://{user}:{password}@{host}/{dbname}"
    return create_engine(uri)

def run_test_query(title, query):
    """
    Generic function to run a SQL query and return a DataFrame.
    """
    print(f"\n TEST: {title}")
    print("-" * 50)
    
    engine = get_db_connection()
    try:
        with engine.connect() as conn:
            df = pd.read_sql(text(query), conn)
            return df
    except Exception as e:
        print(f" SQL ERROR: {e}")
        return pd.DataFrame() # Return empty DF on error