import psycopg2
import os

# Database Configuration
DB_NAME = "spanish_football"
DB_USER = "runner"
DB_PASSWORD = "football_password"
DB_HOST = "localhost"

def reset_database():
    print("--- STARTING DATABASE RESET ---")
    
    try:
        # 1. Connect to PostgreSQL
        conn = psycopg2.connect(
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASSWORD,
            host=DB_HOST
        )
        conn.autocommit = True
        cur = conn.cursor()
        print(f"[OK] Connected to database '{DB_NAME}'")

        # 2. Read schema.sql file
        if not os.path.exists("schema.sql"):
            print("[ERROR] schema.sql file not found!")
            return

        with open("schema.sql", "r") as f:
            schema_sql = f.read()

        # 3. Execute the SQL
        print("--- APPLYING SCHEMA ---")
        cur.execute(schema_sql)
        print("[OK] Tables dropped and recreated successfully.")
        
        # 4. Verify tables exist
        cur.execute("""
            SELECT table_name 
            FROM information_schema.tables 
            WHERE table_schema = 'public';
        """)
        tables = cur.fetchall()
        print(f"--- CURRENT TABLES ---")
        for t in tables:
            print(f"- {t[0]}")

        # Cleanup
        cur.close()
        conn.close()
        print("--- DATABASE RESET COMPLETE ---")

    except Exception as e:
        print(f"\n[CRITICAL ERROR] Could not reset database: {e}")

if __name__ == "__main__":
    reset_database()