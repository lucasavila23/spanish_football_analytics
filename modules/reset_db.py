import psycopg2
import os
from dotenv import load_dotenv

# Load credentials
load_dotenv()

DB_CONFIG = {
    "dbname": os.getenv("DB_NAME", "spanish_football"),
    "user": os.getenv("DB_USER", "runner"),
    "password": os.getenv("DB_PASSWORD"),
    "host": os.getenv("DB_HOST", "localhost")
}

def reset_database():
    print("--- RESETTING DATABASE FOR MULTI-SEASON SCALING ---")
    
    conn = psycopg2.connect(**DB_CONFIG)
    cur = conn.cursor()

    # 1. Drop existing tables (Clean Slate)
    print("1. Dropping old tables...")
    cur.execute("DROP TABLE IF EXISTS lineups CASCADE;")
    cur.execute("DROP TABLE IF EXISTS player_stats CASCADE;")
    cur.execute("DROP TABLE IF EXISTS matches CASCADE;")

    # 2. Create Matches Table (Now with SEASON column)
    print("2. Creating Table: matches (with Season column)...")
    cur.execute("""
        CREATE TABLE matches (
            id SERIAL PRIMARY KEY,
            season VARCHAR(10) NOT NULL,  -- <--- NEW COLUMN
            date DATE NOT NULL,
            home_team TEXT NOT NULL,
            away_team TEXT NOT NULL,
            home_score INT,
            away_score INT,
            home_xg NUMERIC,
            away_xg NUMERIC,
            UNIQUE(season, date, home_team, away_team)
        );
    """)

    # 3. Create Player Stats Table
    print("3. Creating Table: player_stats...")
    cur.execute("""
        CREATE TABLE player_stats (
            id SERIAL PRIMARY KEY,
            match_id INT REFERENCES matches(id) ON DELETE CASCADE,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            minutes INT,
            goals INT,
            assists INT,
            shots INT,
            xg NUMERIC,
            xa NUMERIC,
            xg_chain NUMERIC,
            xg_buildup NUMERIC,
            key_passes INT,
            yellow_card INT,
            red_card INT
        );
    """)

    # 4. Create Lineups Table
    print("4. Creating Table: lineups...")
    cur.execute("""
        CREATE TABLE lineups (
            id SERIAL PRIMARY KEY,
            match_id INT REFERENCES matches(id) ON DELETE CASCADE,
            team TEXT NOT NULL,
            player_name TEXT NOT NULL,
            position TEXT,
            is_starter BOOLEAN,
            shots_on_target INT,
            fouls_committed INT,
            fouls_suffered INT,
            offsides INT,
            saves INT,
            goals_conceded INT
        );
    """)

    conn.commit()
    cur.close()
    conn.close()
    print("--- DATABASE RESET COMPLETE ---")

if __name__ == "__main__":
    reset_database()